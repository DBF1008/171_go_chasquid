package queue

import (
	"bytes"
	"fmt"
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

// blockingCourier is a test courier that blocks delivery until released.
// This lets tests control when items leave the queue.
type blockingCourier struct {
	release chan struct{}
	wg      sync.WaitGroup
}

func newBlockingCourier() *blockingCourier {
	return &blockingCourier{
		release: make(chan struct{}),
	}
}

func (c *blockingCourier) Deliver(from string, to string, data []byte) (error, bool) {
	<-c.release
	c.wg.Done()
	return nil, false
}

func (c *blockingCourier) Forward(from string, to string, data []byte, servers []string) (error, bool) {
	<-c.release
	c.wg.Done()
	return nil, false
}

func (c *blockingCourier) Expect(n int) {
	c.wg.Add(n)
}

func (c *blockingCourier) Release() {
	close(c.release)
}

func (c *blockingCourier) Wait() {
	c.wg.Wait()
}

// TestConcurrentPutRespectsMaxItems verifies that under concurrent Put calls,
// at most MaxItems items are accepted into the queue.
func TestConcurrentPutRespectsMaxItems(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)

	maxItems := 5
	nGoroutines := 20

	// Use a blocking courier so items stay in the queue during the test.
	localC := newBlockingCourier()
	remoteC := newBlockingCourier()

	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		localC, remoteC)
	q.MaxItems = maxItems

	// All goroutines will synchronize at this barrier before calling Put,
	// to maximize contention on the capacity check.
	barrier := make(chan struct{})
	var successCount atomic.Int32
	var failCount atomic.Int32

	var wg sync.WaitGroup
	for i := 0; i < nGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-barrier // wait for all goroutines to be ready

			tr := trace.New("test", "TestConcurrentPutRespectsMaxItems")
			defer tr.Finish()

			to := fmt.Sprintf("user%d@loco", idx)
			_, err := q.Put(tr, "from@loco", []string{to}, []byte("data"))
			if err == nil {
				successCount.Add(1)
			} else if err == errQueueFull {
				failCount.Add(1)
			} else {
				t.Errorf("unexpected error from Put: %v", err)
			}
		}(i)
	}

	// Release all goroutines simultaneously.
	close(barrier)
	wg.Wait()

	successes := int(successCount.Load())
	failures := int(failCount.Load())

	if successes != maxItems {
		t.Errorf("expected exactly %d successful Puts, got %d (failures: %d)",
			maxItems, successes, failures)
	}
	if successes+failures != nGoroutines {
		t.Errorf("expected %d total results, got %d", nGoroutines, successes+failures)
	}
	if q.Len() != maxItems {
		t.Errorf("expected queue length %d, got %d", maxItems, q.Len())
	}

	// Clean up: release the blocking courier and let delivery finish.
	localC.Expect(successes)
	localC.Release()
	localC.Wait()
	testlib.WaitFor(func() bool { return q.Len() == 0 }, 5*time.Second)
}

// TestConcurrentPutRejectsWhenFull verifies that concurrent Put calls are all
// rejected when the queue is already at capacity.
func TestConcurrentPutRejectsWhenFull(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)

	maxItems := 3
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, testlib.DumbCourier)
	q.MaxItems = maxItems

	// Fill the queue to capacity using direct insertion (to avoid triggering
	// delivery goroutines that would drain it).
	for i := 0; i < maxItems; i++ {
		item := &Item{
			Message: Message{
				ID:   <-newID,
				From: fmt.Sprintf("from-%d", i),
				Rcpt: []*Recipient{
					mkR("to@loco", Recipient_EMAIL, Recipient_PENDING, "", "")},
				Data: []byte("data"),
			},
			CreatedAt: time.Now(),
		}
		err := item.WriteTo(q.path)
		if err != nil {
			t.Fatalf("failed to write item: %v", err)
		}
		q.mu.Lock()
		q.q[item.ID] = item
		q.mu.Unlock()
	}

	if q.Len() != maxItems {
		t.Fatalf("expected queue length %d, got %d", maxItems, q.Len())
	}

	// Now try concurrent Puts - all should be rejected.
	nGoroutines := 10
	barrier := make(chan struct{})
	var failCount atomic.Int32

	var wg sync.WaitGroup
	for i := 0; i < nGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-barrier

			tr := trace.New("test", "TestConcurrentPutRejectsWhenFull")
			defer tr.Finish()

			to := fmt.Sprintf("extra%d@loco", idx)
			_, err := q.Put(tr, "from@loco", []string{to}, []byte("data"))
			if err == errQueueFull {
				failCount.Add(1)
			} else {
				t.Errorf("expected errQueueFull, got: %v", err)
			}
		}(i)
	}

	close(barrier)
	wg.Wait()

	if int(failCount.Load()) != nGoroutines {
		t.Errorf("expected all %d Puts to be rejected, only %d were",
			nGoroutines, failCount.Load())
	}

	// Queue length should still be exactly maxItems.
	if q.Len() != maxItems {
		t.Errorf("expected queue length %d after rejected Puts, got %d",
			maxItems, q.Len())
	}
}

// TestDeliveryReleasesCapacity verifies that after a successful delivery
// removes an item from the queue, new Put calls succeed again.
func TestDeliveryReleasesCapacity(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)

	maxItems := 2
	localC := testlib.NewTestCourier()
	remoteC := testlib.NewTestCourier()
	q, _ := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		localC, remoteC)
	q.MaxItems = maxItems

	// Fill the queue to capacity via Put.
	tr := trace.New("test", "TestDeliveryReleasesCapacity")
	defer tr.Finish()

	localC.Expect(maxItems)

	for i := 0; i < maxItems; i++ {
		to := fmt.Sprintf("user%d@loco", i)
		_, err := q.Put(tr, "from@loco", []string{to}, []byte("data"))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Queue should be full now.
	if q.Len() != maxItems {
		t.Fatalf("expected queue length %d, got %d", maxItems, q.Len())
	}

	// This Put should be rejected.
	_, err := q.Put(tr, "from@loco", []string{"overflow@loco"}, []byte("data"))
	if err != errQueueFull {
		t.Errorf("expected errQueueFull, got: %v", err)
	}

	// Wait for deliveries to complete and items to be removed.
	localC.Wait()
	testlib.WaitFor(func() bool { return q.Len() == 0 }, 5*time.Second)

	if q.Len() != 0 {
		t.Fatalf("expected empty queue after delivery, got %d items", q.Len())
	}

	// Now Put should succeed again.
	localC.Expect(1)
	id, err := q.Put(tr, "from@loco", []string{"new@loco"}, []byte("data"))
	if err != nil {
		t.Errorf("Put after delivery should succeed, got: %v", err)
	}
	if id == "" {
		t.Error("expected non-empty ID")
	}

	localC.Wait()
	testlib.WaitFor(func() bool { return q.Len() == 0 }, 5*time.Second)
}
