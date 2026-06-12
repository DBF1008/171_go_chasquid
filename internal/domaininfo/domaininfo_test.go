package domaininfo

import (
	"errors"
	"os"
	"testing"

	"blitiri.com.ar/go/chasquid/internal/testlib"
	"blitiri.com.ar/go/chasquid/internal/trace"
)

func TestBasic(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	db, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	tr := trace.New("test", "basic")
	defer tr.Finish()

	// IncomingSecLevel checks.
	if !db.IncomingSecLevel(tr, "d1", SecLevel_PLAIN) {
		t.Errorf("incoming: new domain as plain not allowed")
	}
	if !db.IncomingSecLevel(tr, "d1", SecLevel_TLS_SECURE) {
		t.Errorf("incoming: increment to tls-secure not allowed")
	}
	if db.IncomingSecLevel(tr, "d1", SecLevel_TLS_INSECURE) {
		t.Errorf("incoming: decrement to tls-insecure was allowed")
	}

	// OutgoingSecLevel checks.
	if !db.OutgoingSecLevel(tr, "d1", SecLevel_PLAIN) {
		t.Errorf("outgoing: new domain as plain not allowed")
	}
	if !db.OutgoingSecLevel(tr, "d1", SecLevel_TLS_SECURE) {
		t.Errorf("outgoing: increment to tls-secure not allowed")
	}
	if db.OutgoingSecLevel(tr, "d1", SecLevel_TLS_INSECURE) {
		t.Errorf("outgoing: decrement to tls-insecure was allowed")
	}

	// Check that it was added to the store and a new db sees it.
	db2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if db2.IncomingSecLevel(tr, "d1", SecLevel_TLS_INSECURE) {
		t.Errorf("decrement to tls-insecure was allowed in new DB")
	}

	// Check that Clear resets the entry back to plain.
	ok := db.Clear(tr, "d1")
	if !ok {
		t.Errorf("Clear(d1) did not find the domain")
	}
	if !db.IncomingSecLevel(tr, "d1", SecLevel_PLAIN) {
		t.Errorf("Clear did not reset the domain back to plain (incoming)")
	}
	if !db.OutgoingSecLevel(tr, "d1", SecLevel_PLAIN) {
		t.Errorf("Clear did not reset the domain back to plain (outgoing)")
	}

	// Check that Clear returns false if the domain does not exist.
	ok = db.Clear(tr, "notexist")
	if ok {
		t.Errorf("Clear(notexist) returned true")
	}
}

func TestNewDomain(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	db, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	tr := trace.New("test", "newdomain")
	defer tr.Finish()

	cases := []struct {
		domain string
		level  SecLevel
	}{
		{"plain", SecLevel_PLAIN},
		{"insecure", SecLevel_TLS_INSECURE},
		{"secure", SecLevel_TLS_SECURE},
	}
	for _, c := range cases {
		// The other tests do an incoming check first, so new domains would get
		// created via that path. We switch the order here to exercise that
		// OutgoingSecLevel also handles new domains successfully.
		if !db.OutgoingSecLevel(tr, c.domain, c.level) {
			t.Errorf("domain %q not allowed (out) at %s", c.domain, c.level)
		}
		if !db.IncomingSecLevel(tr, c.domain, c.level) {
			t.Errorf("domain %q not allowed (in) at %s", c.domain, c.level)
		}
	}
}

func TestProgressions(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	db, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	tr := trace.New("test", "progressions")
	defer tr.Finish()

	cases := []struct {
		domain string
		lvl    SecLevel
		ok     bool
	}{
		{"pisis", SecLevel_PLAIN, true},
		{"pisis", SecLevel_TLS_INSECURE, true},
		{"pisis", SecLevel_TLS_SECURE, true},
		{"pisis", SecLevel_TLS_INSECURE, false},
		{"pisis", SecLevel_TLS_SECURE, true},

		{"ssip", SecLevel_TLS_SECURE, true},
		{"ssip", SecLevel_TLS_SECURE, true},
		{"ssip", SecLevel_TLS_INSECURE, false},
		{"ssip", SecLevel_PLAIN, false},
	}
	for i, c := range cases {
		if ok := db.IncomingSecLevel(tr, c.domain, c.lvl); ok != c.ok {
			t.Errorf("%2d %q in  attempt for %s failed: got %v, expected %v",
				i, c.domain, c.lvl, ok, c.ok)
		}
		if ok := db.OutgoingSecLevel(tr, c.domain, c.lvl); ok != c.ok {
			t.Errorf("%2d %q out attempt for %s failed: got %v, expected %v",
				i, c.domain, c.lvl, ok, c.ok)
		}
	}
}

func TestErrors(t *testing.T) {
	// Non-existent directory.
	_, err := New("/doesnotexists")
	if err == nil {
		t.Error("could create a DB on a non-existent directory")
	}

	// Corrupt/invalid file.
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	db, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}

	tr := trace.New("test", "errors")
	defer tr.Finish()

	if !db.IncomingSecLevel(tr, "d1", SecLevel_TLS_SECURE) {
		t.Errorf("increment to tls-secure not allowed")
	}

	testlib.Rewrite(t, dir+"/s:d1", "invalid-text-protobuf-contents")

	err = db.Reload()
	if err == nil {
		t.Errorf("no error when reloading db with invalid file")
	}

	// Creating a db with an invalid file should also result in an error.
	_, err = New(dir)
	if err == nil {
		t.Errorf("no error when creating db with invalid file")
	}
}

func TestDirectoryErrors(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	db, err := New(dir + "/db")
	if err != nil {
		t.Fatal(err)
	}

	tr := trace.New("test", "direrrors")
	defer tr.Finish()

	// We want to cause store.ListIDs to return an error. To do so, we will
	// cause Readdir to fail by removing the underlying db directory.
	err = os.Remove(dir + "/db")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Reload()
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("got %v, expected %v", err, os.ErrNotExist)
	}

	// We expect write() to also fail to store data in this scenario.
	d := Domain{Name: "d1"}
	err = db.write(tr, &d)
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("got %v, expected %v", err, os.ErrNotExist)
	}
}

func findState(states []DomainState, name string) (DomainState, bool) {
	for _, s := range states {
		if s.Name == name {
			return s, true
		}
	}
	return DomainState{}, false
}

func TestDump(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	db, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	tr := trace.New("test", "dump")
	defer tr.Finish()

	// Empty database: the dump has no entries.
	states, err := db.Dump()
	if err != nil {
		t.Fatalf("Dump on empty db: %v", err)
	}
	if len(states) != 0 {
		t.Errorf("Dump on empty db: got %d entries, want 0: %v", len(states), states)
	}

	// Record a couple of domains at non-default levels.
	db.IncomingSecLevel(tr, "a-example", SecLevel_TLS_SECURE)
	db.OutgoingSecLevel(tr, "a-example", SecLevel_TLS_CLIENT)
	db.IncomingSecLevel(tr, "b-example", SecLevel_TLS_INSECURE)

	// After the update, re-querying reflects the new levels. Entries are
	// sorted by name, present in both the cache and on disk, and consistent.
	states, err = db.Dump()
	if err != nil {
		t.Fatalf("Dump after update: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("Dump after update: got %d entries, want 2: %v", len(states), states)
	}
	if states[0].Name != "a-example" || states[1].Name != "b-example" {
		t.Errorf("Dump not sorted by name: %v", states)
	}

	a, ok := findState(states, "a-example")
	if !ok {
		t.Fatalf("a-example missing from dump: %v", states)
	}
	if got := a.Effective().IncomingSecLevel; got != SecLevel_TLS_SECURE {
		t.Errorf("a-example incoming: got %s, want TLS_SECURE", got)
	}
	if got := a.Effective().OutgoingSecLevel; got != SecLevel_TLS_CLIENT {
		t.Errorf("a-example outgoing: got %s, want TLS_CLIENT", got)
	}
	if !a.InCache() || !a.Persisted() || !a.Consistent() {
		t.Errorf("a-example: cache=%v persisted=%v consistent=%v, want all true",
			a.InCache(), a.Persisted(), a.Consistent())
	}

	// Consistency with disk: a fresh DB loads purely from disk, so its dump
	// must match the running one.
	db2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	states2, err := db2.Dump()
	if err != nil {
		t.Fatalf("Dump from reloaded db: %v", err)
	}
	if len(states2) != len(states) {
		t.Fatalf("reloaded dump has %d entries, want %d", len(states2), len(states))
	}
	for i := range states {
		got, want := states2[i], states[i]
		if got.Name != want.Name ||
			got.Effective().IncomingSecLevel != want.Effective().IncomingSecLevel ||
			got.Effective().OutgoingSecLevel != want.Effective().OutgoingSecLevel {
			t.Errorf("reloaded dump[%d] = %v, want %v", i, got, want)
		}
		if !got.Consistent() {
			t.Errorf("reloaded dump[%d] (%s) not consistent with disk", i, got.Name)
		}
	}

	// After cleanup (Clear), the entry remains but is reset to plain, and the
	// change is reflected on disk too.
	if !db.Clear(tr, "a-example") {
		t.Fatal("Clear(a-example) returned false")
	}
	states, err = db.Dump()
	if err != nil {
		t.Fatalf("Dump after clear: %v", err)
	}
	a, ok = findState(states, "a-example")
	if !ok {
		t.Fatalf("a-example disappeared after clear: %v", states)
	}
	if a.Effective().IncomingSecLevel != SecLevel_PLAIN ||
		a.Effective().OutgoingSecLevel != SecLevel_PLAIN {
		t.Errorf("a-example not reset to plain after clear: %v", a.Effective())
	}
	if !a.InCache() || !a.Persisted() || !a.Consistent() {
		t.Errorf("a-example after clear: cache=%v persisted=%v consistent=%v, want all true",
			a.InCache(), a.Persisted(), a.Consistent())
	}
}

func TestDumpDivergence(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	db, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	tr := trace.New("test", "divergence")
	defer tr.Finish()

	// "cached": recorded normally, so it is in both the cache and on disk.
	db.IncomingSecLevel(tr, "cached", SecLevel_TLS_SECURE)

	// "diskonly": written straight to the store, bypassing the cache, to
	// simulate an on-disk entry the running server has not loaded.
	err = db.store.Put("diskonly", &Domain{
		Name:             "diskonly",
		IncomingSecLevel: SecLevel_TLS_SECURE,
	})
	if err != nil {
		t.Fatal(err)
	}

	// "cacheonly": remove the on-disk file of a cached entry, to simulate a
	// cached entry that is not persisted.
	if err := os.Remove(dir + "/s:cached"); err != nil {
		t.Fatal(err)
	}

	states, err := db.Dump()
	if err != nil {
		t.Fatalf("Dump: %v", err)
	}

	cacheonly, ok := findState(states, "cached")
	if !ok {
		t.Fatalf("cached entry missing: %v", states)
	}
	if !cacheonly.InCache() || cacheonly.Persisted() || cacheonly.Consistent() {
		t.Errorf("cache-only entry: cache=%v persisted=%v consistent=%v, want true/false/false",
			cacheonly.InCache(), cacheonly.Persisted(), cacheonly.Consistent())
	}

	diskonly, ok := findState(states, "diskonly")
	if !ok {
		t.Fatalf("diskonly entry missing: %v", states)
	}
	if diskonly.InCache() || !diskonly.Persisted() || diskonly.Consistent() {
		t.Errorf("disk-only entry: cache=%v persisted=%v consistent=%v, want false/true/false",
			diskonly.InCache(), diskonly.Persisted(), diskonly.Consistent())
	}
	if got := diskonly.Effective().IncomingSecLevel; got != SecLevel_TLS_SECURE {
		t.Errorf("disk-only effective incoming: got %s, want TLS_SECURE", got)
	}
}

func TestInfo(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	db, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	tr := trace.New("test", "info")
	defer tr.Finish()

	// Unknown domain.
	if _, ok, err := db.Info("nope"); err != nil {
		t.Fatalf("Info(nope): %v", err)
	} else if ok {
		t.Error("Info(nope) returned ok=true")
	}

	// Recorded domain: cached, persisted and consistent.
	db.IncomingSecLevel(tr, "d1", SecLevel_TLS_SECURE)
	st, ok, err := db.Info("d1")
	if err != nil {
		t.Fatalf("Info(d1): %v", err)
	}
	if !ok {
		t.Fatal("Info(d1) returned ok=false")
	}
	if got := st.Effective().IncomingSecLevel; got != SecLevel_TLS_SECURE {
		t.Errorf("Info(d1) incoming: got %s, want TLS_SECURE", got)
	}
	if !st.InCache() || !st.Persisted() || !st.Consistent() {
		t.Errorf("Info(d1): cache=%v persisted=%v consistent=%v, want all true",
			st.InCache(), st.Persisted(), st.Consistent())
	}

	// On-disk only entry is found and reported as not cached.
	if err := db.store.Put("d2", &Domain{
		Name:             "d2",
		OutgoingSecLevel: SecLevel_TLS_SECURE,
	}); err != nil {
		t.Fatal(err)
	}
	st, ok, err = db.Info("d2")
	if err != nil {
		t.Fatalf("Info(d2): %v", err)
	}
	if !ok {
		t.Fatal("Info(d2) returned ok=false")
	}
	if st.InCache() || !st.Persisted() {
		t.Errorf("Info(d2): cache=%v persisted=%v, want false/true",
			st.InCache(), st.Persisted())
	}
}
