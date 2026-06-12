package queue

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"blitiri.com.ar/go/chasquid/internal/aliases"
	"blitiri.com.ar/go/chasquid/internal/set"
	"blitiri.com.ar/go/chasquid/internal/testlib"
	"blitiri.com.ar/go/chasquid/internal/trace"
)

func allUsersExist(tr *trace.Trace, user, domain string) (bool, error) {
	return true, nil
}

func TestBasic(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	localC := testlib.NewTestCourier()
	remoteC := testlib.NewTestCourier()
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		localC, remoteC)
	tr := trace.New("test", "TestBasic")
	defer tr.Finish()

	localC.Expect(2)
	remoteC.Expect(1)
	id, err := q.Put(tr, "from", []string{"am@loco", "x@remote", "nodomain"}, []byte("data"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	if len(id) < 6 {
		t.Errorf("short ID: %v", id)
	}

	localC.Wait()
	remoteC.Wait()

	// Make sure the delivered items leave the queue.
	testlib.WaitFor(func() bool { return q.Len() == 0 }, 2*time.Second)
	if q.Len() != 0 {
		t.Fatalf("%d items not removed from the queue after delivery", q.Len())
	}

	cases := []struct {
		courier    *testlib.TestCourier
		expectedTo string
	}{
		{localC, "nodomain"},
		{localC, "am@loco"},
		{remoteC, "x@remote"},
	}
	for _, c := range cases {
		req := c.courier.ReqFor[c.expectedTo]
		if req == nil {
			t.Errorf("missing request for %q", c.expectedTo)
			continue
		}

		if req.From != "from" || req.To != c.expectedTo ||
			!bytes.Equal(req.Data, []byte("data")) {
			t.Errorf("wrong request for %q: %v", c.expectedTo, req)
		}
	}
}

func TestDSNOnTimeout(t *testing.T) {
	localC := testlib.NewTestCourier()
	remoteC := testlib.NewTestCourier()
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		localC, remoteC)

	// Insert an expired item in the queue.
	item := &Item{
		Message: Message{
			ID:   <-newID,
			From: "from@loco",
			Rcpt: []*Recipient{
				mkR("to@to", Recipient_EMAIL, Recipient_PENDING, "err", "to@to")},
			Data: []byte("data"),
		},
		CreatedAt: time.Now().Add(-24 * time.Hour),
	}
	q.q[item.ID] = item
	err := item.WriteTo(q.path)
	if err != nil {
		t.Errorf("failed to write item: %v", err)
	}

	// Exercise DumpString while at it.
	q.DumpString()

	// Launch the sending loop, expect 1 local delivery (the DSN).
	localC.Expect(1)
	go item.SendLoop(q)
	localC.Wait()

	req := localC.ReqFor["from@loco"]
	if req == nil {
		t.Fatal("missing DSN")
	}

	if req.From != "<>" || req.To != "from@loco" ||
		!strings.Contains(string(req.Data), "X-Failed-Recipients: to@to,") {
		t.Errorf("wrong DSN: %q", string(req.Data))
	}
}

func TestAliases(t *testing.T) {
	localC := testlib.NewTestCourier()
	remoteC := testlib.NewTestCourier()
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		localC, remoteC)
	tr := trace.New("test", "TestAliases")
	defer tr.Finish()

	q.aliases.AddDomain("loco")
	q.aliases.AddAliasForTesting("ab@loco", "pq@loco", nil, aliases.EMAIL)
	q.aliases.AddAliasForTesting("ab@loco", "rs@loco", nil, aliases.EMAIL)
	q.aliases.AddAliasForTesting("cd@loco", "ata@hualpa", nil, aliases.EMAIL)
	q.aliases.AddAliasForTesting(
		"fwd@loco", "fwd@loco", []string{"server"}, aliases.FORWARD)
	q.aliases.AddAliasForTesting(
		"remote@loco", "remote@rana", []string{"server"}, aliases.FORWARD)
	// Note the pipe aliases are tested below, as they don't use the couriers
	// and it can be quite inconvenient to test them in this way.

	localC.Expect(2)
	remoteC.Expect(3)

	// One email from a local domain: from@loco -> ab@loco, cd@loco, fwd@loco.
	_, err := q.Put(tr, "from@loco",
		[]string{"ab@loco", "cd@loco", "fwd@loco"},
		[]byte("data"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// And another from a remote domain: from@rana -> remote@loco
	_, err = q.Put(tr, "from@rana",
		[]string{"remote@loco"},
		[]byte("data"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	localC.Wait()
	remoteC.Wait()

	cases := []struct {
		courier      *testlib.TestCourier
		expectedFrom string
		expectedTo   string
	}{
		// From the local domain: from@loco
		{localC, "from@loco", "pq@loco"},
		{localC, "from@loco", "rs@loco"},
		{remoteC, "from@loco", "ata@hualpa"},
		{remoteC, "from@loco", "fwd@loco"},

		// From the remote domain: from@rana.
		// Note the SRS in the remoteC.
		{remoteC, "remote+fwd_from=from=rana@loco", "remote@rana"},
	}
	for _, c := range cases {
		req := c.courier.ReqFor[c.expectedTo]
		if req == nil {
			t.Errorf("missing request for %q", c.expectedTo)
			continue
		}

		if req.From != c.expectedFrom || req.To != c.expectedTo ||
			!bytes.Equal(req.Data, []byte("data")) {
			t.Errorf("wrong request for %q: %v", c.expectedTo, *req)
		}
	}
}

func TestFullQueue(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	q, _ := New(dir, set.NewString(),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, testlib.DumbCourier)
	tr := trace.New("test", "TestFullQueue")
	defer tr.Finish()

	// Force-insert as many items in the queue as it supports.
	oneID := ""
	for i := 0; i < q.MaxItems; i++ {
		item := &Item{
			Message: Message{
				ID:   <-newID,
				From: fmt.Sprintf("from-%d", i),
				Rcpt: []*Recipient{
					mkR("to", Recipient_EMAIL, Recipient_PENDING, "", "")},
				Data: []byte("data"),
			},
			CreatedAt: time.Now(),
		}
		q.q[item.ID] = item
		oneID = item.ID
	}

	// This one should fail due to the queue being too big.
	id, err := q.Put(tr, "from", []string{"to"}, []byte("data-qf"))
	if err != errQueueFull {
		t.Errorf("Not failed as expected: %v - %v", id, err)
	}

	// Remove one, and try again: it should succeed.
	// Write it first so we don't get complaints about the file not existing
	// (as we did not all the items properly).
	q.q[oneID].WriteTo(q.path)
	q.Remove(oneID)

	id, err = q.Put(tr, "from", []string{"to"}, []byte("data"))
	if err != nil {
		t.Errorf("Put: %v", err)
	}
	q.Remove(id)
}

func TestPipes(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, testlib.DumbCourier)

	item := &Item{
		Message: Message{
			ID:   <-newID,
			From: "from",
			Rcpt: []*Recipient{
				mkR("true", Recipient_PIPE, Recipient_PENDING, "", "")},
			Data: []byte("data"),
		},
		CreatedAt: time.Now(),
	}

	if err, _ := item.deliver(q, item.Rcpt[0]); err != nil {
		t.Errorf("pipe delivery failed: %v", err)
	}
}

func TestBadPath(t *testing.T) {
	// A new queue will attempt to os.MkdirAll the path.
	// We expect this path to fail.
	_, err := New("/proc/doesnotexist", set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, testlib.DumbCourier)
	if err == nil {
		t.Errorf("could create queue, expected permission denied")
	}
}

func TestNextDelay(t *testing.T) {
	cases := []struct{ since, min time.Duration }{
		{10 * time.Second, 1 * time.Minute},
		{3 * time.Minute, 5 * time.Minute},
		{7 * time.Minute, 10 * time.Minute},
		{15 * time.Minute, 20 * time.Minute},
		{30 * time.Minute, 20 * time.Minute},
	}
	for _, c := range cases {
		// Repeat each case a few times to exercise the perturbation a bit.
		for i := 0; i < 10; i++ {
			delay := nextDelay(time.Now().Add(-c.since))

			max := c.min + 1*time.Minute
			if delay < c.min || delay > max {
				t.Errorf("since:%v  expected [%v, %v], got %v",
					c.since, c.min, max, delay)
			}
		}
	}
}

func TestSerialization(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)

	// Save an item in the queue directory.
	item := &Item{
		Message: Message{
			ID:   <-newID,
			From: "from@loco",
			Rcpt: []*Recipient{
				mkR("to@to", Recipient_EMAIL, Recipient_PENDING, "err", "to@to")},
			Data: []byte("data"),
		},
		CreatedAt: time.Now().Add(-1 * time.Hour),
	}
	err := item.WriteTo(dir)
	if err != nil {
		t.Errorf("failed to write item: %v", err)
	}

	// Create the queue; should load the
	remoteC := testlib.NewTestCourier()
	remoteC.Expect(1)
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, remoteC)
	q.Load()

	// Launch the sending loop, expect 1 remote delivery for the item we saved.
	remoteC.Wait()

	req := remoteC.ReqFor["to@to"]
	if req == nil {
		t.Fatal("email not delivered")
	}

	if req.From != "from@loco" || req.To != "to@to" {
		t.Errorf("wrong email: %v", req)
	}
}

func mkR(a string, t Recipient_Type, s Recipient_Status, m, o string) *Recipient {
	return &Recipient{
		Address:            a,
		Type:               t,
		Status:             s,
		LastFailureMessage: m,
		OriginalAddress:    o,
	}
}

func TestViewEmpty(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, testlib.DumbCourier)

	view := q.View("")
	if view.Length != 0 || len(view.Items) != 0 {
		t.Errorf("empty queue: expected no items, got length=%d items=%v",
			view.Length, view.Items)
	}
	if !view.OldestCreatedAt.IsZero() {
		t.Errorf("empty queue: expected zero oldest time, got %v",
			view.OldestCreatedAt)
	}
	if view.PendingRcpts != 0 || view.SentRcpts != 0 || view.FailedRcpts != 0 {
		t.Errorf("empty queue: expected zero counts, got p=%d s=%d f=%d",
			view.PendingRcpts, view.SentRcpts, view.FailedRcpts)
	}
	// The maps must be non-nil and empty so automation can rely on them.
	if view.FailureReasons == nil || len(view.FailureReasons) != 0 {
		t.Errorf("empty queue: expected empty FailureReasons, got %v",
			view.FailureReasons)
	}
	if view.ItemsByDomain == nil || len(view.ItemsByDomain) != 0 {
		t.Errorf("empty queue: expected empty ItemsByDomain, got %v",
			view.ItemsByDomain)
	}
}

// fillQueueForView force-inserts a few backlogged items, in a non-chronological
// order, to exercise the structured view's aggregation, sorting and filtering.
func fillQueueForView(q *Queue, now time.Time) {
	add := func(id string, age time.Duration, rcpts ...*Recipient) {
		q.q[id] = &Item{
			Message: Message{
				ID:   id,
				From: "sender@loco",
				To:   []string{rcpts[0].OriginalAddress},
				Rcpt: rcpts,
				Data: []byte("data"),
			},
			CreatedAt: now.Add(-age),
		}
	}

	add("item-c", 1*time.Hour,
		mkR("a@example.com", Recipient_EMAIL, Recipient_PENDING,
			"deferred: timeout", "a@example.com"))
	add("item-a", 3*time.Hour,
		mkR("b@example.com", Recipient_EMAIL, Recipient_FAILED,
			"550 no such user", "b@example.com"),
		mkR("c@other.net", Recipient_EMAIL, Recipient_PENDING,
			"deferred: timeout", "c@other.net"))
	add("item-b", 2*time.Hour,
		mkR("d@other.net", Recipient_EMAIL, Recipient_SENT,
			"", "d@other.net"))
}

func TestViewBacklog(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, testlib.DumbCourier)

	now := time.Now()
	fillQueueForView(q, now)

	view := q.View("")

	if view.Length != 3 {
		t.Errorf("expected 3 items, got %d", view.Length)
	}

	// Sorting: oldest first => item-a (3h), item-b (2h), item-c (1h).
	var gotOrder []string
	for _, it := range view.Items {
		gotOrder = append(gotOrder, it.ID)
	}
	if want := []string{"item-a", "item-b", "item-c"}; !reflect.DeepEqual(gotOrder, want) {
		t.Errorf("wrong item order: got %v, want %v", gotOrder, want)
	}

	// Oldest reported time is item-a's creation time.
	if !view.OldestCreatedAt.Equal(now.Add(-3 * time.Hour)) {
		t.Errorf("wrong oldest time: got %v, want %v",
			view.OldestCreatedAt, now.Add(-3*time.Hour))
	}

	// Recipient status counts: pending 2 (a, c), failed 1 (b), sent 1 (d).
	if view.PendingRcpts != 2 || view.FailedRcpts != 1 || view.SentRcpts != 1 {
		t.Errorf("wrong status counts: pending=%d failed=%d sent=%d",
			view.PendingRcpts, view.FailedRcpts, view.SentRcpts)
	}

	// Failure reasons aggregated across recipients.
	if n := view.FailureReasons["deferred: timeout"]; n != 2 {
		t.Errorf("expected 2 'deferred: timeout', got %d", n)
	}
	if n := view.FailureReasons["550 no such user"]; n != 1 {
		t.Errorf("expected 1 '550 no such user', got %d", n)
	}
	if _, ok := view.FailureReasons[""]; ok {
		t.Error("empty failure message should not be counted")
	}

	// Items by domain: example.com -> 2 (item-c, item-a),
	// other.net -> 2 (item-a, item-b).
	if view.ItemsByDomain["example.com"] != 2 {
		t.Errorf("expected 2 items for example.com, got %d",
			view.ItemsByDomain["example.com"])
	}
	if view.ItemsByDomain["other.net"] != 2 {
		t.Errorf("expected 2 items for other.net, got %d",
			view.ItemsByDomain["other.net"])
	}

	// The recipient details are carried through, with enums as readable strings.
	itemA := view.Items[0]
	if len(itemA.Rcpt) != 2 {
		t.Fatalf("item-a: expected 2 recipients, got %d", len(itemA.Rcpt))
	}
	r0 := itemA.Rcpt[0]
	if r0.Address != "b@example.com" || r0.Status != "FAILED" ||
		r0.Type != "EMAIL" || r0.LastFailureMessage != "550 no such user" {
		t.Errorf("item-a recipient 0 wrong: %+v", r0)
	}
}

func TestViewDomainFilter(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, testlib.DumbCourier)

	fillQueueForView(q, time.Now())

	// Filter to example.com: only item-a and item-c have a recipient there.
	view := q.View("example.com")
	if view.Length != 2 {
		t.Errorf("filtered: expected 2 items, got %d", view.Length)
	}
	var got []string
	for _, it := range view.Items {
		got = append(got, it.ID)
	}
	if want := []string{"item-a", "item-c"}; !reflect.DeepEqual(got, want) {
		t.Errorf("filtered order: got %v, want %v", got, want)
	}

	// Aggregates are computed over the filtered subset. item-a contributes
	// b@example.com (FAILED) and c@other.net (PENDING); item-c contributes
	// a@example.com (PENDING). So pending=2, failed=1, sent=0.
	if view.PendingRcpts != 2 || view.FailedRcpts != 1 || view.SentRcpts != 0 {
		t.Errorf("filtered status counts: pending=%d failed=%d sent=%d",
			view.PendingRcpts, view.FailedRcpts, view.SentRcpts)
	}

	// Filtering by a domain not present yields an empty (but well-formed) view.
	empty := q.View("nope.invalid")
	if empty.Length != 0 || len(empty.Items) != 0 {
		t.Errorf("filter by unknown domain: expected empty, got %+v", empty)
	}
	if !empty.OldestCreatedAt.IsZero() {
		t.Errorf("filter by unknown domain: expected zero oldest, got %v",
			empty.OldestCreatedAt)
	}
}

func TestViewConsistentWithDumpString(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, testlib.DumbCourier)

	fillQueueForView(q, time.Now())

	dump := q.DumpString()
	view := q.View("")

	// The text dump reports the same length as the structured view.
	if !strings.Contains(dump, fmt.Sprintf("length: %d", view.Length)) {
		t.Errorf("DumpString length does not match view length %d:\n%s",
			view.Length, dump)
	}

	// Every item ID, recipient address and failure message present in the
	// structured view also appears in the text dump (same underlying state).
	for _, it := range view.Items {
		if !strings.Contains(dump, it.ID) {
			t.Errorf("item %q missing from DumpString:\n%s", it.ID, dump)
		}
		for _, r := range it.Rcpt {
			if !strings.Contains(dump, r.Address) {
				t.Errorf("recipient %q missing from DumpString:\n%s",
					r.Address, dump)
			}
			if r.LastFailureMessage != "" &&
				!strings.Contains(dump, r.LastFailureMessage) {
				t.Errorf("failure %q missing from DumpString:\n%s",
					r.LastFailureMessage, dump)
			}
		}
	}
}

func TestViewAfterReload(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)

	// Write an item to disk, as if it had been queued before a restart.
	orig := &Item{
		Message: Message{
			ID:   <-newID,
			From: "from@loco",
			To:   []string{"to@remote"},
			Rcpt: []*Recipient{
				mkR("to@remote", Recipient_EMAIL, Recipient_PENDING,
					"deferred: connection refused", "to@remote"),
			},
			Data: []byte("data"),
		},
		CreatedAt: time.Now().Add(-2 * time.Hour),
	}
	if err := orig.WriteTo(dir); err != nil {
		t.Fatalf("failed to write item: %v", err)
	}

	// Reload it from disk (simulating a restart) and place it in a fresh queue
	// without launching the send loops, so the state stays stable while we
	// inspect it.
	reloaded, err := ItemFromFile(
		fmt.Sprintf("%s/%s%s", dir, itemFilePrefix, orig.ID))
	if err != nil {
		t.Fatalf("failed to reload item: %v", err)
	}
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, testlib.DumbCourier)
	q.q[reloaded.ID] = reloaded

	view := q.View("")
	if view.Length != 1 {
		t.Fatalf("expected 1 item after reload, got %d", view.Length)
	}

	got := view.Items[0]
	if got.ID != reloaded.ID || got.From != "from@loco" {
		t.Errorf("unexpected reloaded item: %+v", got)
	}
	if !reflect.DeepEqual(got.To, []string{"to@remote"}) {
		t.Errorf("reloaded To wrong: %v", got.To)
	}
	// The persisted timestamp survives the round-trip.
	if !got.CreatedAt.Equal(reloaded.CreatedAt) {
		t.Errorf("CreatedAt mismatch: got %v, want %v",
			got.CreatedAt, reloaded.CreatedAt)
	}
	if !view.OldestCreatedAt.Equal(reloaded.CreatedAt) {
		t.Errorf("OldestCreatedAt mismatch: got %v, want %v",
			view.OldestCreatedAt, reloaded.CreatedAt)
	}
	if view.PendingRcpts != 1 {
		t.Errorf("expected 1 pending rcpt, got %d", view.PendingRcpts)
	}
	if n := view.FailureReasons["deferred: connection refused"]; n != 1 {
		t.Errorf("expected failure reason count 1, got %d", n)
	}
	if n := view.ItemsByDomain["remote"]; n != 1 {
		t.Errorf("expected 1 item for domain remote, got %d", n)
	}
}
