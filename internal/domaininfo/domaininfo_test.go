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

func TestList(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	db, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	tr := trace.New("test", "list")
	defer tr.Finish()

	// Empty database should return an empty list.
	if got := db.List(); len(got) != 0 {
		t.Errorf("empty db: got %d entries, expected 0", len(got))
	}

	// Add some domains with different security levels.
	db.IncomingSecLevel(tr, "beta.com", SecLevel_TLS_SECURE)
	db.OutgoingSecLevel(tr, "beta.com", SecLevel_TLS_CLIENT)
	db.IncomingSecLevel(tr, "alpha.com", SecLevel_TLS_INSECURE)
	db.OutgoingSecLevel(tr, "alpha.com", SecLevel_TLS_SECURE)

	list := db.List()
	if len(list) != 2 {
		t.Fatalf("got %d entries, expected 2", len(list))
	}

	// List should be sorted by name.
	if list[0].Name != "alpha.com" {
		t.Errorf("list[0].Name = %q, expected alpha.com", list[0].Name)
	}
	if list[1].Name != "beta.com" {
		t.Errorf("list[1].Name = %q, expected beta.com", list[1].Name)
	}

	// Verify the returned entries have correct security levels.
	if list[0].IncomingSecLevel != SecLevel_TLS_INSECURE {
		t.Errorf("alpha incoming = %v, expected TLS_INSECURE",
			list[0].IncomingSecLevel)
	}
	if list[0].OutgoingSecLevel != SecLevel_TLS_SECURE {
		t.Errorf("alpha outgoing = %v, expected TLS_SECURE",
			list[0].OutgoingSecLevel)
	}
	if list[1].IncomingSecLevel != SecLevel_TLS_SECURE {
		t.Errorf("beta incoming = %v, expected TLS_SECURE",
			list[1].IncomingSecLevel)
	}
	if list[1].OutgoingSecLevel != SecLevel_TLS_CLIENT {
		t.Errorf("beta outgoing = %v, expected TLS_CLIENT",
			list[1].OutgoingSecLevel)
	}

	// Returned entries should be copies, not references to internal state.
	list[0].IncomingSecLevel = SecLevel_PLAIN
	if got, _ := db.Get("alpha.com"); got.IncomingSecLevel != SecLevel_TLS_INSECURE {
		t.Errorf("modifying list entry changed internal state")
	}
}

func TestGet(t *testing.T) {
	dir := testlib.MustTempDir(t)
	defer testlib.RemoveIfOk(t, dir)
	db, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	tr := trace.New("test", "get")
	defer tr.Finish()

	// Non-existent domain should return (nil, false).
	d, ok := db.Get("nonexistent.com")
	if ok || d != nil {
		t.Errorf("Get(nonexistent) = (%v, %v), expected (nil, false)", d, ok)
	}

	// Add a domain and verify it can be retrieved.
	db.IncomingSecLevel(tr, "example.com", SecLevel_TLS_SECURE)
	db.OutgoingSecLevel(tr, "example.com", SecLevel_TLS_INSECURE)

	d, ok = db.Get("example.com")
	if !ok {
		t.Fatal("Get(example.com) returned false")
	}
	if d.Name != "example.com" {
		t.Errorf("d.Name = %q, expected example.com", d.Name)
	}
	if d.IncomingSecLevel != SecLevel_TLS_SECURE {
		t.Errorf("incoming = %v, expected TLS_SECURE", d.IncomingSecLevel)
	}
	if d.OutgoingSecLevel != SecLevel_TLS_INSECURE {
		t.Errorf("outgoing = %v, expected TLS_INSECURE", d.OutgoingSecLevel)
	}

	// Returned entry should be a copy, not a reference to internal state.
	d.IncomingSecLevel = SecLevel_PLAIN
	if got, _ := db.Get("example.com"); got.IncomingSecLevel != SecLevel_TLS_SECURE {
		t.Errorf("modifying returned entry changed internal state")
	}

	// After Clear, Get should still return the entry but with PLAIN levels.
	db.Clear(tr, "example.com")
	d, ok = db.Get("example.com")
	if !ok {
		t.Fatal("Get(example.com) returned false after Clear")
	}
	if d.IncomingSecLevel != SecLevel_PLAIN {
		t.Errorf("incoming after clear = %v, expected PLAIN", d.IncomingSecLevel)
	}
	if d.OutgoingSecLevel != SecLevel_PLAIN {
		t.Errorf("outgoing after clear = %v, expected PLAIN", d.OutgoingSecLevel)
	}
}
