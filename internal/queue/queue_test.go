package queue

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

// blockingCourier blocks every delivery until releaseAll is called. This lets
// tests hold items in the queue (so it stays at capacity) and then drain them
// on demand.
type blockingCourier struct {
	proceed chan struct{}
}

func newBlockingCourier() *blockingCourier {
	return &blockingCourier{proceed: make(chan struct{})}
}

func (c *blockingCourier) Deliver(from, to string, data []byte) (error, bool) {
	<-c.proceed
	return nil, false
}

func (c *blockingCourier) Forward(from, to string, data []byte, servers []string) (error, bool) {
	<-c.proceed
	return nil, false
}

// releaseAll unblocks all current and future deliveries.
func (c *blockingCourier) releaseAll() {
	close(c.proceed)
}

// countQueueFiles returns the number of item files currently on disk.
func countQueueFiles(t *testing.T, dir string) int {
	t.Helper()
	files, err := filepath.Glob(dir + "/" + itemFilePrefix + "*")
	if err != nil {
		t.Fatalf("failed to glob queue files: %v", err)
	}
	return len(files)
}

// TestConcurrentPutRespectsLimit checks that when many envelopes are submitted
// concurrently, the queue accepts exactly MaxItems and rejects the rest, and
// that the number of files written to disk never exceeds the limit.
//
// This is a regression test for a check-then-insert race: the capacity check
// used to be separate from (and the disk write used to happen before) the
// reservation of a slot, so concurrent Puts could collectively write far more
// than MaxItems files to disk.
func TestConcurrentPutRespectsLimit(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)

	// Use a blocking courier for both local and remote so accepted items stay
	// in the queue for the duration of the test.
	c := newBlockingCourier()
	q, _ := New(dir, set.NewString(),
		aliases.NewResolver(allUsersExist),
		c, c)
	q.MaxItems = 5

	const concurrency = 50
	var wg sync.WaitGroup
	var accepted, rejected atomic.Int64
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tr := trace.New("test", "concurrent-put")
			defer tr.Finish()
			_, err := q.Put(tr, fmt.Sprintf("from-%d", i),
				[]string{"to"}, []byte("data"))
			switch err {
			case nil:
				accepted.Add(1)
			case errQueueFull:
				rejected.Add(1)
			default:
				t.Errorf("unexpected Put error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Exactly MaxItems should have been accepted, the rest rejected.
	if got, want := accepted.Load(), int64(q.MaxItems); got != want {
		t.Errorf("accepted %d items, want %d", got, want)
	}
	if got, want := rejected.Load(), int64(concurrency-q.MaxItems); got != want {
		t.Errorf("rejected %d items, want %d", got, want)
	}

	// The in-memory queue and the on-disk state must both agree with the limit:
	// the SMTP responses (accepted count) must match what landed on disk.
	if got := q.Len(); got != q.MaxItems {
		t.Errorf("queue length is %d, want %d", got, q.MaxItems)
	}
	if got := countQueueFiles(t, dir); got != q.MaxItems {
		t.Errorf("queue has %d files on disk, want %d", got, q.MaxItems)
	}

	// Drain the queue so the temp dir can be cleaned up.
	c.releaseAll()
	if !testlib.WaitFor(func() bool { return q.Len() == 0 }, 5*time.Second) {
		t.Errorf("queue did not drain, still has %d items", q.Len())
	}
}

// TestQueueReleasesCapacityAfterDelivery checks that once items are delivered
// and removed from the queue, the freed capacity becomes available again for
// new envelopes.
func TestQueueReleasesCapacityAfterDelivery(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)

	c := newBlockingCourier()
	q, _ := New(dir, set.NewString(),
		aliases.NewResolver(allUsersExist),
		c, c)
	q.MaxItems = 3

	tr := trace.New("test", "release-capacity")
	defer tr.Finish()

	// Fill the queue to capacity. Deliveries block, so the items stay.
	for i := 0; i < q.MaxItems; i++ {
		if _, err := q.Put(tr, fmt.Sprintf("from-%d", i),
			[]string{"to"}, []byte("data")); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}
	if got := countQueueFiles(t, dir); got != q.MaxItems {
		t.Fatalf("queue has %d files on disk, want %d", got, q.MaxItems)
	}

	// The queue is full: the next Put must be rejected.
	if _, err := q.Put(tr, "overflow", []string{"to"}, []byte("data")); err != errQueueFull {
		t.Fatalf("Put on full queue: got err %v, want %v", err, errQueueFull)
	}

	// Let the blocked deliveries complete; the items should be delivered and
	// removed, freeing up capacity.
	c.releaseAll()
	if !testlib.WaitFor(func() bool { return q.Len() == 0 }, 5*time.Second) {
		t.Fatalf("queue did not drain, still has %d items", q.Len())
	}
	if got := countQueueFiles(t, dir); got != 0 {
		t.Errorf("queue has %d files on disk after drain, want 0", got)
	}

	// With capacity released, a new Put must succeed.
	id, err := q.Put(tr, "after", []string{"to"}, []byte("data"))
	if err != nil {
		t.Fatalf("Put after drain failed: %v", err)
	}
	if id == "" {
		t.Error("got empty id from successful Put")
	}
}
