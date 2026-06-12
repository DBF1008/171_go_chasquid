// Package domaininfo implements a domain information database, to keep track
// of things we know about a particular domain.
package domaininfo

import (
	"fmt"
	"sort"
	"sync"

	"blitiri.com.ar/go/chasquid/internal/protoio"
	"blitiri.com.ar/go/chasquid/internal/trace"

	"google.golang.org/protobuf/proto"
)

// Command to generate domaininfo.pb.go.
//go:generate protoc --go_out=. --go_opt=paths=source_relative domaininfo.proto

// DB represents the persistent domain information database.
type DB struct {
	// Persistent store with the list of domains we know.
	store *protoio.Store

	info map[string]*Domain
	sync.Mutex
}

// New opens a domain information database on the given dir, creating it if
// necessary. The returned database will not be loaded.
func New(dir string) (*DB, error) {
	st, err := protoio.NewStore(dir)
	if err != nil {
		return nil, err
	}

	l := &DB{
		store: st,
		info:  map[string]*Domain{},
	}

	err = l.Reload()
	if err != nil {
		return nil, err
	}

	return l, nil
}

// Reload the database from disk.
func (db *DB) Reload() error {
	tr := trace.New("DomainInfo.Reload", "reload")
	defer tr.Finish()

	db.Lock()
	defer db.Unlock()

	// Clear the map, in case it has data.
	db.info = map[string]*Domain{}

	ids, err := db.store.ListIDs()
	if err != nil {
		tr.Error(err)
		return err
	}

	for _, id := range ids {
		d := &Domain{}
		_, err := db.store.Get(id, d)
		if err != nil {
			tr.Errorf("id %q: %v", id, err)
			return fmt.Errorf("error loading %q: %v", id, err)
		}

		db.info[d.Name] = d
	}

	tr.Debugf("loaded %d domains", len(ids))
	return nil
}

func (db *DB) write(tr *trace.Trace, d *Domain) error {
	tr = tr.NewChild("DomainInfo.write", d.Name)
	defer tr.Finish()

	err := db.store.Put(d.Name, d)
	if err != nil {
		tr.Error(err)
	} else {
		tr.Debugf("saved")
	}
	return err
}

// IncomingSecLevel checks an incoming security level for the domain.
// Returns true if allowed, false otherwise.
func (db *DB) IncomingSecLevel(tr *trace.Trace, domain string, level SecLevel) bool {
	tr = tr.NewChild("DomainInfo.Incoming", domain)
	defer tr.Finish()
	tr.Debugf("incoming at level %s", level)

	db.Lock()
	defer db.Unlock()

	d, exists := db.info[domain]
	if !exists {
		d = &Domain{Name: domain}
		db.info[domain] = d
		defer db.write(tr, d)
	}

	if level < d.IncomingSecLevel {
		tr.Errorf("%s incoming denied: %s < %s",
			d.Name, level, d.IncomingSecLevel)
		return false
	} else if level == d.IncomingSecLevel {
		tr.Debugf("%s incoming allowed: %s == %s",
			d.Name, level, d.IncomingSecLevel)
		return true
	} else {
		tr.Printf("%s incoming level raised: %s > %s",
			d.Name, level, d.IncomingSecLevel)
		d.IncomingSecLevel = level
		if exists {
			defer db.write(tr, d)
		}
		return true
	}
}

// OutgoingSecLevel checks an incoming security level for the domain.
// Returns true if allowed, false otherwise.
func (db *DB) OutgoingSecLevel(tr *trace.Trace, domain string, level SecLevel) bool {
	tr = tr.NewChild("DomainInfo.Outgoing", domain)
	defer tr.Finish()
	tr.Debugf("outgoing at level %s", level)

	db.Lock()
	defer db.Unlock()

	d, exists := db.info[domain]
	if !exists {
		d = &Domain{Name: domain}
		db.info[domain] = d
		defer db.write(tr, d)
	}

	if level < d.OutgoingSecLevel {
		tr.Errorf("%s outgoing denied: %s < %s",
			d.Name, level, d.OutgoingSecLevel)
		return false
	} else if level == d.OutgoingSecLevel {
		tr.Debugf("%s outgoing allowed: %s == %s",
			d.Name, level, d.OutgoingSecLevel)
		return true
	} else {
		tr.Printf("%s outgoing level raised: %s > %s",
			d.Name, level, d.OutgoingSecLevel)
		d.OutgoingSecLevel = level
		if exists {
			defer db.write(tr, d)
		}
		return true
	}
}

// Clear sets the security level for the given domain to plain.
// This can be used for manual overrides in case there's an operational need
// to do so.
func (db *DB) Clear(tr *trace.Trace, domain string) bool {
	tr = tr.NewChild("DomainInfo.SetToPlain", domain)
	defer tr.Finish()

	db.Lock()
	defer db.Unlock()

	d, exists := db.info[domain]
	if !exists {
		tr.Debugf("does not exist")
		return false
	}

	d.IncomingSecLevel = SecLevel_PLAIN
	d.OutgoingSecLevel = SecLevel_PLAIN
	db.write(tr, d)
	tr.Printf("set to plain")
	return true
}

// DomainState is a point-in-time snapshot of a domain's information. It
// combines the in-memory cache with what is persisted on disk, so callers can
// detect divergence between the two (e.g. an entry that is cached but not yet
// persisted, or persisted with a different value).
type DomainState struct {
	Name string

	// Cached is the domain as held in the in-memory cache, or nil if the
	// domain is not in the cache.
	Cached *Domain

	// Stored is the domain as persisted on disk, or nil if the domain is not
	// on disk.
	Stored *Domain
}

// Effective returns the domain record that is in effect: the cached one if
// present, otherwise the stored one. It is never nil for a DomainState
// returned by Dump or Info.
func (s DomainState) Effective() *Domain {
	if s.Cached != nil {
		return s.Cached
	}
	return s.Stored
}

// InCache returns whether the domain is present in the in-memory cache.
func (s DomainState) InCache() bool { return s.Cached != nil }

// Persisted returns whether the domain is persisted on disk.
func (s DomainState) Persisted() bool { return s.Stored != nil }

// Consistent returns whether the cached and stored records both exist and
// hold the same values.
func (s DomainState) Consistent() bool {
	return s.Cached != nil && s.Stored != nil && proto.Equal(s.Cached, s.Stored)
}

// Dump returns a snapshot of all known domains, sorted by name. The result is
// the union of the in-memory cache and the on-disk store, so divergence
// between them is visible to the caller. The returned records are copies; the
// caller may modify them freely.
func (db *DB) Dump() ([]DomainState, error) {
	db.Lock()
	defer db.Unlock()

	states := map[string]*DomainState{}
	for name, d := range db.info {
		states[name] = &DomainState{
			Name:   name,
			Cached: proto.Clone(d).(*Domain),
		}
	}

	ids, err := db.store.ListIDs()
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		d := &Domain{}
		ok, err := db.store.Get(id, d)
		if err != nil {
			return nil, fmt.Errorf("error loading %q: %v", id, err)
		}
		if !ok {
			continue
		}
		st, exists := states[d.Name]
		if !exists {
			st = &DomainState{Name: d.Name}
			states[d.Name] = st
		}
		st.Stored = d
	}

	names := make([]string, 0, len(states))
	for name := range states {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]DomainState, 0, len(names))
	for _, name := range names {
		out = append(out, *states[name])
	}
	return out, nil
}

// Info returns the snapshot for a single domain. ok is false if the domain is
// neither cached nor persisted on disk. The returned records are copies.
func (db *DB) Info(domain string) (state DomainState, ok bool, err error) {
	db.Lock()
	defer db.Unlock()

	st := DomainState{Name: domain}
	found := false

	if d, exists := db.info[domain]; exists {
		st.Cached = proto.Clone(d).(*Domain)
		found = true
	}

	d := &Domain{}
	onDisk, err := db.store.Get(domain, d)
	if err != nil {
		return DomainState{}, false, err
	}
	if onDisk {
		st.Stored = d
		found = true
	}

	return st, found, nil
}
