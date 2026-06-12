package queue

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"blitiri.com.ar/go/chasquid/internal/aliases"
	"blitiri.com.ar/go/chasquid/internal/set"
	"blitiri.com.ar/go/chasquid/internal/testlib"
)

// newTestQueue creates a fresh empty queue backed by a temp directory.
func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	dir := testlib.MustTempDir(t)
	t.Cleanup(func() { testlib.RemoveIfOk(t, dir) })
	q, err := New(dir, set.NewString("loco", "example.com"),
		aliases.NewResolver(allUsersExist),
		testlib.DumbCourier, testlib.DumbCourier)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

// forceInsert inserts an item directly into the queue map and persists it,
// bypassing SendLoop. Useful for setting up test state deterministically.
func forceInsert(t *testing.T, q *Queue, from string, to []string, rcpts []*Recipient, createdAt time.Time) *Item {
	t.Helper()
	item := &Item{
		Message: Message{
			ID:   <-newID,
			From: from,
			To:   to,
			Rcpt: rcpts,
			Data: []byte("Subject: test\r\n\r\nbody"),
		},
		CreatedAt: createdAt,
	}
	if err := item.WriteTo(q.path); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	q.q[item.ID] = item
	q.mu.Unlock()
	return item
}

// ---------------------------------------------------------------------------
// Empty queue
// ---------------------------------------------------------------------------

func TestSummaryEmptyQueue(t *testing.T) {
	q := newTestQueue(t)

	s := q.Summary(SummaryFilter{}, SortByAgeAsc)

	if s.Length != 0 {
		t.Errorf("Length: got %d, want 0", s.Length)
	}
	if s.PendingCount != 0 || s.SentCount != 0 || s.FailedCount != 0 {
		t.Errorf("counts: pending=%d sent=%d failed=%d, want all 0",
			s.PendingCount, s.SentCount, s.FailedCount)
	}
	if len(s.Items) != 0 {
		t.Errorf("Items: got %d items, want 0", len(s.Items))
	}
	if s.OldestAge != "" || s.NewestAge != "" {
		t.Errorf("expected empty OldestAge/NewestAge, got %q / %q", s.OldestAge, s.NewestAge)
	}
}

// ---------------------------------------------------------------------------
// Multiple backlogged items
// ---------------------------------------------------------------------------

func TestSummaryMultipleItems(t *testing.T) {
	q := newTestQueue(t)

	now := time.Now()

	forceInsert(t, q, "alice@loco", []string{"bob@example.com"},
		[]*Recipient{mkR("bob@example.com", Recipient_EMAIL, Recipient_PENDING, "conn refused", "bob@example.com")},
		now.Add(-3*time.Hour))

	forceInsert(t, q, "carol@loco", []string{"dave@loco"},
		[]*Recipient{
			mkR("dave@loco", Recipient_EMAIL, Recipient_SENT, "", "dave@loco"),
			mkR("eve@example.com", Recipient_EMAIL, Recipient_FAILED, "mailbox full", "eve@example.com"),
		},
		now.Add(-1*time.Hour))

	forceInsert(t, q, "frank@example.com", []string{"grace@loco"},
		[]*Recipient{mkR("grace@loco", Recipient_EMAIL, Recipient_PENDING, "timeout", "grace@loco")},
		now.Add(-30*time.Minute))

	s := q.Summary(SummaryFilter{}, SortByAgeAsc)

	if s.Length != 3 {
		t.Fatalf("Length: got %d, want 3", s.Length)
	}

	// Counts across all recipients (3 items × their recipients).
	if s.PendingCount != 2 {
		t.Errorf("PendingCount: got %d, want 2", s.PendingCount)
	}
	if s.SentCount != 1 {
		t.Errorf("SentCount: got %d, want 1", s.SentCount)
	}
	if s.FailedCount != 1 {
		t.Errorf("FailedCount: got %d, want 1", s.FailedCount)
	}

	if s.OldestAge == "" || s.NewestAge == "" {
		t.Errorf("expected non-empty OldestAge/NewestAge")
	}

	// Items should be sorted oldest-first.
	for i := 1; i < len(s.Items); i++ {
		if s.Items[i].CreatedAt.Before(s.Items[i-1].CreatedAt) {
			t.Errorf("items not sorted ascending: %v came after %v",
				s.Items[i].CreatedAt, s.Items[i-1].CreatedAt)
		}
	}

	// Spot-check the oldest item.
	oldest := s.Items[0]
	if oldest.From != "alice@loco" {
		t.Errorf("oldest item From: got %q, want %q", oldest.From, "alice@loco")
	}
	if len(oldest.Recipients) != 1 || oldest.Recipients[0].LastFailureMessage != "conn refused" {
		t.Errorf("oldest item recipient failure: got %+v", oldest.Recipients)
	}
}

// ---------------------------------------------------------------------------
// Domain filter
// ---------------------------------------------------------------------------

func TestSummaryFilterByDomain(t *testing.T) {
	q := newTestQueue(t)

	now := time.Now()

	forceInsert(t, q, "alice@loco", []string{"bob@example.com"},
		[]*Recipient{mkR("bob@example.com", Recipient_EMAIL, Recipient_PENDING, "", "bob@example.com")},
		now.Add(-2*time.Hour))

	forceInsert(t, q, "carol@loco", []string{"dave@loco"},
		[]*Recipient{mkR("dave@loco", Recipient_EMAIL, Recipient_PENDING, "", "dave@loco")},
		now.Add(-1*time.Hour))

	forceInsert(t, q, "frank@example.com", []string{"grace@other.org"},
		[]*Recipient{mkR("grace@other.org", Recipient_EMAIL, Recipient_PENDING, "", "grace@other.org")},
		now.Add(-30*time.Minute))

	// Filter to example.com domain.
	s := q.Summary(SummaryFilter{Domain: "example.com"}, SortByAgeAsc)

	if s.Length != 1 {
		t.Fatalf("Length: got %d, want 1", s.Length)
	}
	if s.Items[0].From != "alice@loco" {
		t.Errorf("From: got %q, want %q", s.Items[0].From, "alice@loco")
	}

	// Filter to loco domain (should match 1 item with loco recipient).
	s = q.Summary(SummaryFilter{Domain: "loco"}, SortByAgeAsc)
	if s.Length != 1 {
		t.Fatalf("loco filter Length: got %d, want 1", s.Length)
	}

	// Filter to other.org domain.
	s = q.Summary(SummaryFilter{Domain: "other.org"}, SortByAgeAsc)
	if s.Length != 1 {
		t.Fatalf("other.org filter Length: got %d, want 1", s.Length)
	}

	// Filter to a domain that has no items.
	s = q.Summary(SummaryFilter{Domain: "nowhere.test"}, SortByAgeAsc)
	if s.Length != 0 {
		t.Errorf("nowhere.test filter Length: got %d, want 0", s.Length)
	}
}

// ---------------------------------------------------------------------------
// Status filter
// ---------------------------------------------------------------------------

func TestSummaryFilterByStatus(t *testing.T) {
	q := newTestQueue(t)

	now := time.Now()

	forceInsert(t, q, "alice@loco", []string{"bob@example.com"},
		[]*Recipient{mkR("bob@example.com", Recipient_EMAIL, Recipient_PENDING, "conn refused", "bob@example.com")},
		now.Add(-2*time.Hour))

	forceInsert(t, q, "carol@loco", []string{"dave@loco"},
		[]*Recipient{mkR("dave@loco", Recipient_EMAIL, Recipient_SENT, "", "dave@loco")},
		now.Add(-1*time.Hour))

	forceInsert(t, q, "frank@example.com", []string{"grace@loco"},
		[]*Recipient{mkR("grace@loco", Recipient_EMAIL, Recipient_FAILED, "mailbox full", "grace@loco")},
		now.Add(-30*time.Minute))

	// Filter PENDING.
	s := q.Summary(SummaryFilter{Status: "PENDING"}, SortByAgeAsc)
	if s.Length != 1 || s.Items[0].From != "alice@loco" {
		t.Errorf("PENDING filter: got %d items, first=%+v", s.Length, s.Items)
	}

	// Filter SENT.
	s = q.Summary(SummaryFilter{Status: "SENT"}, SortByAgeAsc)
	if s.Length != 1 || s.Items[0].From != "carol@loco" {
		t.Errorf("SENT filter: got %d items, first=%+v", s.Length, s.Items)
	}

	// Filter FAILED.
	s = q.Summary(SummaryFilter{Status: "FAILED"}, SortByAgeAsc)
	if s.Length != 1 || s.Items[0].From != "frank@example.com" {
		t.Errorf("FAILED filter: got %d items, first=%+v", s.Length, s.Items)
	}

	// Case insensitivity on status.
	s = q.Summary(SummaryFilter{Status: "pending"}, SortByAgeAsc)
	if s.Length != 1 {
		t.Errorf("pending (lowercase) filter: got %d items, want 1", s.Length)
	}
}

// ---------------------------------------------------------------------------
// From filter
// ---------------------------------------------------------------------------

func TestSummaryFilterByFrom(t *testing.T) {
	q := newTestQueue(t)

	now := time.Now()

	forceInsert(t, q, "alice@loco", []string{"bob@example.com"},
		[]*Recipient{mkR("bob@example.com", Recipient_EMAIL, Recipient_PENDING, "", "bob@example.com")},
		now.Add(-2*time.Hour))

	forceInsert(t, q, "carol@loco", []string{"dave@loco"},
		[]*Recipient{mkR("dave@loco", Recipient_EMAIL, Recipient_PENDING, "", "dave@loco")},
		now.Add(-1*time.Hour))

	forceInsert(t, q, "admin@example.com", []string{"grace@loco"},
		[]*Recipient{mkR("grace@loco", Recipient_EMAIL, Recipient_PENDING, "", "grace@loco")},
		now.Add(-30*time.Minute))

	// Filter from addresses containing "loco".
	s := q.Summary(SummaryFilter{From: "loco"}, SortByAgeAsc)
	if s.Length != 2 {
		t.Errorf("from=loco: got %d items, want 2", s.Length)
	}

	// Filter from addresses containing "alice".
	s = q.Summary(SummaryFilter{From: "alice"}, SortByAgeAsc)
	if s.Length != 1 || s.Items[0].From != "alice@loco" {
		t.Errorf("from=alice: got %d items, want 1 (alice@loco)", s.Length)
	}

	// Case insensitivity.
	s = q.Summary(SummaryFilter{From: "ADMIN"}, SortByAgeAsc)
	if s.Length != 1 || s.Items[0].From != "admin@example.com" {
		t.Errorf("from=ADMIN: got %d items, want 1 (admin@example.com)", s.Length)
	}
}

// ---------------------------------------------------------------------------
// Combined filters
// ---------------------------------------------------------------------------

func TestSummaryCombinedFilters(t *testing.T) {
	q := newTestQueue(t)

	now := time.Now()

	forceInsert(t, q, "alice@loco", []string{"bob@example.com"},
		[]*Recipient{mkR("bob@example.com", Recipient_EMAIL, Recipient_PENDING, "err", "bob@example.com")},
		now.Add(-2*time.Hour))

	forceInsert(t, q, "alice@loco", []string{"carol@loco"},
		[]*Recipient{mkR("carol@loco", Recipient_EMAIL, Recipient_SENT, "", "carol@loco")},
		now.Add(-1*time.Hour))

	forceInsert(t, q, "bob@example.com", []string{"dave@example.com"},
		[]*Recipient{mkR("dave@example.com", Recipient_EMAIL, Recipient_PENDING, "timeout", "dave@example.com")},
		now.Add(-30*time.Minute))

	// Filter: from=alice AND status=PENDING
	s := q.Summary(SummaryFilter{From: "alice", Status: "PENDING"}, SortByAgeAsc)
	if s.Length != 1 {
		t.Fatalf("from=alice+status=PENDING: got %d, want 1", s.Length)
	}
	if s.Items[0].From != "alice@loco" {
		t.Errorf("got from=%q, want alice@loco", s.Items[0].From)
	}

	// Filter: domain=example.com AND status=PENDING
	s = q.Summary(SummaryFilter{Domain: "example.com", Status: "PENDING"}, SortByAgeAsc)
	// Both items 0 and 2 have example.com recipients with PENDING status.
	if s.Length != 2 {
		t.Errorf("domain=example.com+status=PENDING: got %d, want 2", s.Length)
	}
}

// ---------------------------------------------------------------------------
// Sorting
// ---------------------------------------------------------------------------

func TestSummarySorting(t *testing.T) {
	q := newTestQueue(t)

	now := time.Now()

	times := []time.Duration{-5 * time.Hour, -1 * time.Hour, -3 * time.Hour}
	for _, d := range times {
		forceInsert(t, q, "sender@loco", []string{"rcpt@example.com"},
			[]*Recipient{mkR("rcpt@example.com", Recipient_EMAIL, Recipient_PENDING, "", "rcpt@example.com")},
			now.Add(d))
	}

	// Ascending: oldest first (5h ago, 3h ago, 1h ago).
	s := q.Summary(SummaryFilter{}, SortByAgeAsc)
	if len(s.Items) != 3 {
		t.Fatalf("got %d items, want 3", len(s.Items))
	}
	for i := 1; i < len(s.Items); i++ {
		if s.Items[i].CreatedAt.Before(s.Items[i-1].CreatedAt) {
			t.Errorf("SortByAgeAsc violated at index %d", i)
		}
	}

	// Descending: newest first (1h ago, 3h ago, 5h ago).
	s = q.Summary(SummaryFilter{}, SortByAgeDesc)
	for i := 1; i < len(s.Items); i++ {
		if s.Items[i].CreatedAt.After(s.Items[i-1].CreatedAt) {
			t.Errorf("SortByAgeDesc violated at index %d", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Consistency with DumpString
// ---------------------------------------------------------------------------

func TestSummaryConsistentWithDumpString(t *testing.T) {
	q := newTestQueue(t)

	now := time.Now()

	forceInsert(t, q, "alice@loco", []string{"bob@example.com"},
		[]*Recipient{mkR("bob@example.com", Recipient_EMAIL, Recipient_PENDING, "", "bob@example.com")},
		now.Add(-2*time.Hour))

	forceInsert(t, q, "carol@loco", []string{"dave@loco"},
		[]*Recipient{mkR("dave@loco", Recipient_EMAIL, Recipient_SENT, "", "dave@loco")},
		now.Add(-1*time.Hour))

	dump := q.DumpString()
	s := q.Summary(SummaryFilter{}, SortByAgeAsc)

	// The DumpString output includes "length: N" which should match s.Length.
	wantLen := "length: " + strconv.Itoa(s.Length)
	if !strings.Contains(dump, wantLen) {
		t.Errorf("DumpString does not contain %q:\n%s", wantLen, dump)
	}

	// Every item ID in the summary should appear in the dump.
	for _, item := range s.Items {
		if !strings.Contains(dump, item.ID) {
			t.Errorf("item ID %q not found in DumpString output", item.ID)
		}
	}
}

// ---------------------------------------------------------------------------
// Persistence round-trip: items loaded from disk should appear in Summary
// ---------------------------------------------------------------------------

func TestSummaryAfterLoad(t *testing.T) {
	dir := testlib.MustTempDir(t)

	now := time.Now()

	// Write items to disk directly.
	items := []*Item{
		{
			Message: Message{
				ID:   <-newID,
				From: "alice@loco",
				To:   []string{"bob@example.com"},
				Rcpt: []*Recipient{mkR("bob@example.com", Recipient_EMAIL, Recipient_PENDING, "err1", "bob@example.com")},
				Data: []byte("data1"),
			},
			CreatedAt: now.Add(-2 * time.Hour),
		},
		{
			Message: Message{
				ID:   <-newID,
				From: "carol@loco",
				To:   []string{"dave@loco"},
				Rcpt: []*Recipient{mkR("dave@loco", Recipient_EMAIL, Recipient_FAILED, "err2", "dave@loco")},
				Data: []byte("data2"),
			},
			CreatedAt: now.Add(-1 * time.Hour),
		},
	}
	for _, item := range items {
		if err := item.WriteTo(dir); err != nil {
			t.Fatal(err)
		}
	}

	// Create a new queue that loads from disk.
	// We use a gatedCourier that blocks the first 2 deliveries (one per item),
	// giving us a stable window to call Summary() before items are removed.
	// After those 2 deliveries, the gate opens automatically so any follow-up
	// goroutines (e.g. DSN deliveries) proceed without blocking.
	gc := newGatedCourier(2)
	q, err := New(dir, set.NewString("loco"),
		aliases.NewResolver(allUsersExist),
		gc, gc)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Load(); err != nil {
		t.Fatal(err)
	}

	// Wait until all SendLoop goroutines have entered their first delivery.
	gc.awaitBlocked(t, 2)

	s := q.Summary(SummaryFilter{}, SortByAgeAsc)
	if s.Length != 2 {
		t.Errorf("after Load: got Length=%d, want 2", s.Length)
	}
	if s.PendingCount != 1 {
		t.Errorf("PendingCount: got %d, want 1", s.PendingCount)
	}
	if s.FailedCount != 1 {
		t.Errorf("FailedCount: got %d, want 1", s.FailedCount)
	}

	// Release the gate and wait for all SendLoop goroutines to finish so they
	// don't outlive the test and access a deleted directory.
	gc.release()
	gc.wg.Wait()

	testlib.RemoveIfOk(t, dir)
}

// gatedCourier blocks the first n Deliver/Forward calls on a gate channel.
// Once n calls have entered the blocked state, the gate opens automatically,
// allowing all subsequent calls to proceed immediately.
// A WaitGroup tracks every in-flight call so the test can wait for completion.
type gatedCourier struct {
	gate    chan struct{}
	wg      sync.WaitGroup
	n       int
	mu      sync.Mutex
	blocked int
	ready   chan struct{} // closed once n calls are blocked
}

func newGatedCourier(n int) *gatedCourier {
	return &gatedCourier{
		gate:  make(chan struct{}),
		n:     n,
		ready: make(chan struct{}),
	}
}

// awaitBlocked waits until n deliveries are blocked on the gate.
func (c *gatedCourier) awaitBlocked(t *testing.T, n int) {
	t.Helper()
	select {
	case <-c.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for deliveries to block")
	}
}

// release opens the gate and waits for all in-flight calls to finish.
func (c *gatedCourier) release() {
	close(c.gate)
}

func (c *gatedCourier) call() {
	c.wg.Add(1)
	defer c.wg.Done()

	c.mu.Lock()
	c.blocked++
	if c.blocked == c.n {
		close(c.ready)
	}
	c.mu.Unlock()

	// Block until the gate is opened by release().
	<-c.gate
}

func (c *gatedCourier) Deliver(from, to string, data []byte) (error, bool) {
	c.call()
	return nil, false
}

func (c *gatedCourier) Forward(from, to string, data []byte, servers []string) (error, bool) {
	c.call()
	return nil, false
}
