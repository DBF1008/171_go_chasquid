package queue

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
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
	q.StartSendLoops()

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

// transientFailCourier is a courier that always fails with a transient error
// and counts delivery attempts.
type transientFailCourier struct {
	mu       sync.Mutex
	attempts int
}

func (c *transientFailCourier) Deliver(from, to string, data []byte) (error, bool) {
	c.mu.Lock()
	c.attempts++
	c.mu.Unlock()
	return fmt.Errorf("transient error"), false
}

func (c *transientFailCourier) Forward(from, to string, data []byte, servers []string) (error, bool) {
	return fmt.Errorf("transient error"), false
}

func (c *transientFailCourier) Attempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attempts
}

// TestLoadRespectsCustomGiveUpAfter verifies that items restored from disk are
// evaluated against the configured GiveUpAfter, not the hardcoded default
// (20h). This is a regression test: before the fix, Load started the sending
// loops before SetQueueLimits could apply the configured value, so items older
// than the default 20h but younger than the configured limit were immediately
// expired and DSN'd on restart.
func TestLoadRespectsCustomGiveUpAfter(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)

	remoteC := &transientFailCourier{}

	// Create a queue and write an item 25h old.
	// This is older than the default GiveUpAfter (20h), but younger than the
	// custom one we will configure (48h).
	_ = os.MkdirAll(dir, 0700)

	item := &Item{
		Message: Message{
			ID:   <-newID,
			From: "from@loco",
			Rcpt: []*Recipient{
				mkR("to@remote", Recipient_EMAIL, Recipient_PENDING, "err", "to@remote")},
			Data: []byte("data"),
		},
		CreatedAt: time.Now().Add(-25 * time.Hour),
	}
	err := item.WriteTo(dir)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	// Create a fresh queue (simulating a restart) and configure it with a
	// custom GiveUpAfter BEFORE starting the sending loops.
	q2, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, remoteC)
	q2.GiveUpAfter = 48 * time.Hour

	if err := q2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	q2.StartSendLoops()

	// The item should survive: 25h < 48h configured GiveUpAfter.
	// Wait long enough for the SendLoop to start, attempt delivery, and
	// determine the item is still within the retry window.
	time.Sleep(500 * time.Millisecond)

	if q2.Len() != 1 {
		t.Errorf("item was prematurely removed from queue (got len=%d, want 1);"+
			" likely DSN'd with default GiveUpAfter instead of configured 48h",
			q2.Len())
	}

	if remoteC.Attempts() == 0 {
		t.Error("no delivery attempt made on restored item")
	}
}

// TestLoadExpiredItemGetsDSN verifies that items restored from disk that have
// already exceeded the configured GiveUpAfter are properly expired and DSN'd.
func TestLoadExpiredItemGetsDSN(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)

	localC := testlib.NewTestCourier()
	remoteC := &transientFailCourier{}

	// Write an item 50h old, well beyond both the default (20h) and the custom
	// GiveUpAfter we will set (48h).
	item := &Item{
		Message: Message{
			ID:   <-newID,
			From: "from@loco",
			Rcpt: []*Recipient{
				mkR("to@remote", Recipient_EMAIL, Recipient_PENDING, "err", "to@remote")},
			Data: []byte("data"),
		},
		CreatedAt: time.Now().Add(-50 * time.Hour),
	}
	err := item.WriteTo(dir)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	// Restart: load with custom GiveUpAfter (48h). Item is 50h old > 48h.
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		localC, remoteC)
	q.GiveUpAfter = 48 * time.Hour

	localC.Expect(1) // The DSN to from@loco.

	if err := q.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	q.StartSendLoops()
	localC.Wait()

	// Verify a DSN was sent to the sender.
	req := localC.ReqFor["from@loco"]
	if req == nil {
		t.Fatal("no DSN sent for expired restored item")
	}
	if req.From != "<>" {
		t.Errorf("DSN should be from <>, got %q", req.From)
	}
	if !strings.Contains(string(req.Data), "X-Failed-Recipients: to@remote,") {
		t.Errorf("DSN missing failed recipients header")
	}

	// Item should be removed from the queue.
	testlib.WaitFor(func() bool { return q.Len() == 0 }, 2*time.Second)
	if q.Len() != 0 {
		t.Errorf("expired item not removed from queue")
	}
}

// TestLoadRestoredItemContinuesDelivery verifies that restored items with
// pending recipients continue delivery attempts after restart, and that the
// configured GiveUpAfter is properly applied to determine the retry window.
func TestLoadRestoredItemContinuesDelivery(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)

	remoteC := testlib.NewTestCourier()
	remoteC.Expect(1)

	// Write an item created 1h ago with a pending recipient.
	item := &Item{
		Message: Message{
			ID:   <-newID,
			From: "from@loco",
			Rcpt: []*Recipient{
				mkR("to@remote", Recipient_EMAIL, Recipient_PENDING, "", "to@remote")},
			Data: []byte("data"),
		},
		CreatedAt: time.Now().Add(-1 * time.Hour),
	}
	err := item.WriteTo(dir)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	// Restart with a custom GiveUpAfter (48h).
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, remoteC)
	q.GiveUpAfter = 48 * time.Hour

	if err := q.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Item should be loaded into memory.
	if q.Len() != 1 {
		t.Fatalf("expected 1 item after Load, got %d", q.Len())
	}

	q.StartSendLoops()

	// Delivery should succeed (item is 1h old < 48h GiveUpAfter).
	remoteC.Wait()

	req := remoteC.ReqFor["to@remote"]
	if req == nil {
		t.Fatal("restored item was not delivered")
	}
	if req.From != "from@loco" || req.To != "to@remote" {
		t.Errorf("wrong delivery: %+v", req)
	}

	// Item should be removed after successful delivery.
	testlib.WaitFor(func() bool { return q.Len() == 0 }, 2*time.Second)
	if q.Len() != 0 {
		t.Errorf("delivered item not removed from queue")
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
