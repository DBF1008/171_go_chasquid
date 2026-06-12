// Package queue implements our email queue.
// Accepted envelopes get put in the queue, and processed asynchronously.
package queue

// Command to generate queue.pb.go from queue.proto.
//go:generate protoc --go_out=. --go_opt=paths=source_relative -I=${GOPATH}/src -I. queue.proto

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"blitiri.com.ar/go/chasquid/internal/aliases"
	"blitiri.com.ar/go/chasquid/internal/courier"
	"blitiri.com.ar/go/chasquid/internal/envelope"
	"blitiri.com.ar/go/chasquid/internal/expvarom"
	"blitiri.com.ar/go/chasquid/internal/maillog"
	"blitiri.com.ar/go/chasquid/internal/protoio"
	"blitiri.com.ar/go/chasquid/internal/safeio"
	"blitiri.com.ar/go/chasquid/internal/set"
	"blitiri.com.ar/go/chasquid/internal/trace"
	"blitiri.com.ar/go/log"

	"golang.org/x/net/idna"
)

const (
	// Prefix for item file names.
	// This is for convenience, versioning, and to be able to tell them apart
	// temporary files and other cruft.
	// It's important that it's outside the base64 space so it doesn't get
	// generated accidentally.
	itemFilePrefix = "m:"
)

var (
	errQueueFull = fmt.Errorf("Queue size too big, try again later")
)

// Exported variables.
var (
	putCount = expvarom.NewInt("chasquid/queue/putCount",
		"count of envelopes attempted to be put in the queue")
	itemsWritten = expvarom.NewInt("chasquid/queue/itemsWritten",
		"count of items the queue wrote to disk")
	dsnQueued = expvarom.NewInt("chasquid/queue/dsnQueued",
		"count of DSNs that we generated (queued)")
	deliverAttempts = expvarom.NewMap("chasquid/queue/deliverAttempts",
		"recipient_type", "attempts to deliver mail, by recipient type")
)

// Channel used to get random IDs for items in the queue.
var newID chan string

func generateNewIDs() {
	// The IDs are only used internally, we are ok with using a PRNG.
	// IDs are base64(8 random bytes), but the code doesn't care.
	buf := make([]byte, 8)
	for {
		binary.NativeEndian.PutUint64(buf, rand.Uint64())
		newID <- base64.RawURLEncoding.EncodeToString(buf)
	}
}

func init() {
	newID = make(chan string, 4)
	go generateNewIDs()
}

// Queue that keeps mail waiting for delivery.
type Queue struct {
	// Couriers to use to deliver mail.
	localC  courier.Courier
	remoteC courier.Courier

	// Domains we consider local.
	localDomains *set.String

	// Path where we store the queue.
	path string

	// Aliases resolver.
	aliases *aliases.Resolver

	// The maximum number of items in the queue.
	MaxItems int

	// Give up sending attempts after this long.
	GiveUpAfter time.Duration

	// Mutex protecting q.
	mu sync.RWMutex

	// Items in the queue. Map of id -> Item.
	q map[string]*Item
}

// New creates a new Queue instance.
func New(path string, localDomains *set.String, aliases *aliases.Resolver,
	localC, remoteC courier.Courier) (*Queue, error) {

	err := os.MkdirAll(path, 0700)
	q := &Queue{
		q:            map[string]*Item{},
		localC:       localC,
		remoteC:      remoteC,
		localDomains: localDomains,
		path:         path,
		aliases:      aliases,

		// We reject emails when we hit this.
		// Note the actual default used in the daemon is set in the config. We
		// put a non-zero value here just to be safe.
		MaxItems: 100,

		// We give up sending (and return a DSN) after this long.
		// Note the actual default used in the daemon is set in the config. We
		// put a non-zero value here just to be safe.
		GiveUpAfter: 20 * time.Hour,
	}
	return q, err
}

// Load the queue and launch the sending loops on startup.
func (q *Queue) Load() error {
	files, err := filepath.Glob(q.path + "/" + itemFilePrefix + "*")
	if err != nil {
		return err
	}

	for _, fname := range files {
		item, err := ItemFromFile(fname)
		if err != nil {
			log.Errorf("error loading queue item from %q: %v", fname, err)
			continue
		}

		q.mu.Lock()
		q.q[item.ID] = item
		q.mu.Unlock()

		go item.SendLoop(q)
	}

	return nil
}

// Len returns the number of elements in the queue.
func (q *Queue) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.q)
}

// Put an envelope in the queue.
func (q *Queue) Put(tr *trace.Trace, from string, to []string, data []byte) (string, error) {
	tr = tr.NewChild("Queue.Put", from)
	defer tr.Finish()

	if nItems := q.Len(); nItems >= q.MaxItems {
		tr.Errorf("queue full (%d items)", nItems)
		return "", errQueueFull
	}
	putCount.Add(1)

	item := &Item{
		Message: Message{
			ID:   <-newID,
			From: from,
			Data: data,
		},
		CreatedAt: time.Now(),
	}

	for _, t := range to {
		item.To = append(item.To, t)

		rcpts, err := q.aliases.Resolve(tr, t)
		if err != nil {
			return "", fmt.Errorf("error resolving aliases for %q: %v", t, err)
		}

		// Add the recipients (after resolving aliases); this conversion is
		// not very pretty but at least it's self contained.
		for _, aliasRcpt := range rcpts {
			r := &Recipient{
				Address:         aliasRcpt.Addr,
				Status:          Recipient_PENDING,
				OriginalAddress: t,
			}
			switch aliasRcpt.Type {
			case aliases.EMAIL:
				r.Type = Recipient_EMAIL
			case aliases.PIPE:
				r.Type = Recipient_PIPE
			case aliases.FORWARD:
				r.Type = Recipient_FORWARD
				r.Via = aliasRcpt.Via
			default:
				log.Errorf("unknown alias type %v when resolving %q",
					aliasRcpt.Type, t)
				return "", tr.Errorf("internal error - unknown alias type")
			}
			item.Rcpt = append(item.Rcpt, r)
			tr.Debugf("recipient: %v", r.Address)
		}
	}

	err := item.WriteTo(q.path)
	if err != nil {
		return "", tr.Errorf("failed to write item: %v", err)
	}

	q.mu.Lock()
	q.q[item.ID] = item
	q.mu.Unlock()

	// Begin to send it right away.
	go item.SendLoop(q)

	tr.Debugf("queued")
	return item.ID, nil
}

// Remove an item from the queue.
func (q *Queue) Remove(id string) {
	path := fmt.Sprintf("%s/%s%s", q.path, itemFilePrefix, id)
	err := os.Remove(path)
	if err != nil {
		log.Errorf("failed to remove queue file %q: %v", path, err)
	}

	q.mu.Lock()
	delete(q.q, id)
	q.mu.Unlock()
}

// DumpString returns a human-readable string with the current queue.
// Useful for debugging purposes.
func (q *Queue) DumpString() string {
	q.mu.RLock()
	defer q.mu.RUnlock()
	s := "# Queue status\n\n"
	s += fmt.Sprintf("date: %v\n", time.Now())
	s += fmt.Sprintf("length: %d\n\n", len(q.q))

	for id, item := range q.q {
		s += fmt.Sprintf("## Item %s\n", id)
		item.Lock()
		s += fmt.Sprintf("created at: %s\n", item.CreatedAt)
		s += fmt.Sprintf("from: %s\n", item.From)
		s += fmt.Sprintf("to: %s\n", item.To)
		for _, rcpt := range item.Rcpt {
			s += fmt.Sprintf("%s %s (%s)\n", rcpt.Status, rcpt.Address, rcpt.Type)
			s += fmt.Sprintf("  original address: %s\n", rcpt.OriginalAddress)
			s += fmt.Sprintf("  last failure: %q\n", rcpt.LastFailureMessage)
		}
		item.Unlock()
		s += "\n"
	}

	return s
}

// QueueView is a structured, point-in-time snapshot of the queue.
//
// It is meant for monitoring dashboards and automation, and is built from the
// same in-memory queue state as DumpString, so the two are consistent with each
// other (and, after a restart, with the queue reloaded from disk by Load).
type QueueView struct {
	// Date is when this view was generated.
	Date time.Time `json:"date"`

	// Length is the number of items (messages) included in this view. When a
	// domain filter is applied, it only counts the matching items.
	Length int `json:"length"`

	// OldestCreatedAt is the creation time of the oldest item in the view. It is
	// the zero time if the view is empty.
	OldestCreatedAt time.Time `json:"oldest_created_at"`

	// Recipient counts across all items in the view, broken down by status.
	PendingRcpts int `json:"pending_rcpts"`
	SentRcpts    int `json:"sent_rcpts"`
	FailedRcpts  int `json:"failed_rcpts"`

	// FailureReasons maps a recipient's last failure message to the number of
	// recipients that have it. Recipients without a failure message are not
	// counted.
	FailureReasons map[string]int `json:"failure_reasons"`

	// ItemsByDomain maps a recipient domain to the number of items in the view
	// that have at least one recipient in that domain.
	ItemsByDomain map[string]int `json:"items_by_domain"`

	// Items are the queue entries in the view, sorted by creation time (oldest
	// first), with the item ID as a tie-breaker for stable ordering.
	Items []*ItemView `json:"items"`
}

// ItemView is the structured view of a single queue item.
type ItemView struct {
	ID        string          `json:"id"`
	From      string          `json:"from"`
	To        []string        `json:"to"`
	CreatedAt time.Time       `json:"created_at"`
	Rcpt      []RecipientView `json:"rcpt"`
}

// RecipientView is the structured view of a single recipient of a queue item.
type RecipientView struct {
	Address            string `json:"address"`
	OriginalAddress    string `json:"original_address"`
	Type               string `json:"type"`
	Status             string `json:"status"`
	LastFailureMessage string `json:"last_failure_message"`
}

// View returns a structured snapshot of the queue.
//
// If domain is non-empty, only items that have at least one recipient in that
// domain are included, and all the aggregate statistics are computed over that
// filtered subset. If domain is empty, the whole queue is included.
func (q *Queue) View(domain string) *QueueView {
	q.mu.RLock()
	defer q.mu.RUnlock()

	view := &QueueView{
		Date:           time.Now(),
		FailureReasons: map[string]int{},
		ItemsByDomain:  map[string]int{},
	}

	for _, item := range q.q {
		item.Lock()
		iv, domains := itemToView(item)
		item.Unlock()

		if domain != "" && !domains[domain] {
			continue
		}

		view.Items = append(view.Items, iv)
		for d := range domains {
			view.ItemsByDomain[d]++
		}
		for _, rcpt := range iv.Rcpt {
			switch rcpt.Status {
			case Recipient_PENDING.String():
				view.PendingRcpts++
			case Recipient_SENT.String():
				view.SentRcpts++
			case Recipient_FAILED.String():
				view.FailedRcpts++
			}
			if rcpt.LastFailureMessage != "" {
				view.FailureReasons[rcpt.LastFailureMessage]++
			}
		}
	}

	view.Length = len(view.Items)

	// Sort oldest first, with the ID as a stable tie-breaker. This gives
	// automation a deterministic order regardless of map iteration, and puts
	// the most-stuck item at the head for backlog triage.
	sort.Slice(view.Items, func(i, j int) bool {
		a, b := view.Items[i], view.Items[j]
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.ID < b.ID
	})

	if len(view.Items) > 0 {
		view.OldestCreatedAt = view.Items[0].CreatedAt
	}

	return view
}

// itemToView converts a queue item to its structured view, and returns the set
// of recipient domains it touches. The caller must hold item's lock.
func itemToView(item *Item) (*ItemView, map[string]bool) {
	iv := &ItemView{
		ID:        item.ID,
		From:      item.From,
		To:        append([]string(nil), item.To...),
		CreatedAt: item.CreatedAt,
	}
	domains := map[string]bool{}
	for _, rcpt := range item.Rcpt {
		iv.Rcpt = append(iv.Rcpt, RecipientView{
			Address:            rcpt.Address,
			OriginalAddress:    rcpt.OriginalAddress,
			Type:               rcpt.Type.String(),
			Status:             rcpt.Status.String(),
			LastFailureMessage: rcpt.LastFailureMessage,
		})
		if d := envelope.DomainOf(rcpt.Address); d != "" {
			domains[d] = true
		}
	}
	return iv, domains
}

// An Item in the queue.
type Item struct {
	// Base the item on the protobuf message.
	// We will use this for serialization, so any fields below are NOT
	// serialized.
	Message

	// Protect the entire item.
	sync.Mutex

	// Go-friendly version of Message.CreatedAtTs.
	CreatedAt time.Time
}

// ItemFromFile loads an item from the given file.
func ItemFromFile(fname string) (*Item, error) {
	item := &Item{}
	err := protoio.ReadTextMessage(fname, &item.Message)
	if err != nil {
		return nil, err
	}

	item.CreatedAt = timeFromProto(item.CreatedAtTs)
	return item, nil
}

// WriteTo saves an item to the given directory.
func (item *Item) WriteTo(dir string) error {
	item.Lock()
	defer item.Unlock()
	itemsWritten.Add(1)

	item.CreatedAtTs = timeToProto(item.CreatedAt)

	path := fmt.Sprintf("%s/%s%s", dir, itemFilePrefix, item.ID)

	// Write the file in text format for ease of debugging, and use fsync to
	// improve durability.
	return protoio.WriteTextMessage(
		path, &item.Message, 0600, safeio.FsyncFileOp)
}

// SendLoop repeatedly attempts to send the item.
func (item *Item) SendLoop(q *Queue) {
	tr := trace.New("Queue.SendLoop", item.ID)
	defer tr.Finish()
	tr.Printf("from %s", item.From)

	for time.Since(item.CreatedAt) < q.GiveUpAfter {
		// Send to all recipients that are still pending.
		var wg sync.WaitGroup
		for _, rcpt := range item.Rcpt {
			if rcpt.Status != Recipient_PENDING {
				continue
			}

			wg.Add(1)
			go item.sendOneRcpt(&wg, tr, q, rcpt)
		}
		wg.Wait()

		// If they're all done, no need to wait.
		if item.countRcpt(Recipient_PENDING) == 0 {
			break
		}

		// TODO: Consider sending a non-final notification after 30m or so,
		// that some of the messages have been delayed.

		delay := nextDelay(item.CreatedAt)
		tr.Printf("waiting for %v", delay)
		maillog.QueueLoop(item.ID, item.From, delay)
		time.Sleep(delay)
	}

	// Completed to all recipients (some may not have succeeded).
	if item.countRcpt(Recipient_FAILED, Recipient_PENDING) > 0 && item.From != "<>" {
		sendDSN(tr, q, item)
	}

	tr.Printf("all done")
	maillog.QueueLoop(item.ID, item.From, 0)
	q.Remove(item.ID)
}

// sendOneRcpt, and update it with the results.
func (item *Item) sendOneRcpt(wg *sync.WaitGroup, tr *trace.Trace, q *Queue, rcpt *Recipient) {
	defer wg.Done()
	to := rcpt.Address
	tr.Debugf("%s sending", to)

	err, permanent := item.deliver(q, rcpt)

	item.Lock()
	if err != nil {
		rcpt.LastFailureMessage = err.Error()
		if permanent {
			tr.Errorf("%s permanent error: %v", to, err)
			maillog.SendAttempt(item.ID, item.From, to, err, true)
			rcpt.Status = Recipient_FAILED
		} else {
			tr.Printf("%s temporary error: %v", to, err)
			maillog.SendAttempt(item.ID, item.From, to, err, false)
		}
	} else {
		tr.Printf("%s sent", to)
		maillog.SendAttempt(item.ID, item.From, to, nil, false)
		rcpt.Status = Recipient_SENT
	}
	item.Unlock()

	err = item.WriteTo(q.path)
	if err != nil {
		tr.Errorf("failed to write: %v", err)
	}
}

// deliver the item to the given recipient, using the couriers from the queue.
// Return an error (if any), and whether it is permanent or not.
func (item *Item) deliver(q *Queue, rcpt *Recipient) (err error, permanent bool) {
	if rcpt.Type == Recipient_PIPE {
		deliverAttempts.Add("pipe", 1)
		c := strings.Fields(rcpt.Address)
		if len(c) == 0 {
			return fmt.Errorf("empty pipe"), true
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, c[0], c[1:]...)
		cmd.Stdin = bytes.NewReader(item.Data)
		return cmd.Run(), true
	}

	// Recipient type is FORWARD: we always use the remote courier, and pass
	// the list of servers that was given to us.
	if rcpt.Type == Recipient_FORWARD {
		deliverAttempts.Add("forward", 1)

		// When forwarding with an explicit list of servers, we use SRS if
		// we're sending from a non-local domain (regardless of the
		// destination).
		from := item.From
		if !envelope.DomainIn(item.From, q.localDomains) {
			from = rewriteSender(item.From, rcpt.OriginalAddress)
		}
		return q.remoteC.Forward(from, rcpt.Address, item.Data, rcpt.Via)
	}

	// Recipient type is EMAIL.
	if envelope.DomainIn(rcpt.Address, q.localDomains) {
		deliverAttempts.Add("email:local", 1)
		return q.localC.Deliver(item.From, rcpt.Address, item.Data)
	}

	deliverAttempts.Add("email:remote", 1)
	from := item.From
	if !envelope.DomainIn(item.From, q.localDomains) {
		// We're sending from a non-local to a non-local, need to do SRS.
		from = rewriteSender(item.From, rcpt.OriginalAddress)
	}
	return q.remoteC.Deliver(from, rcpt.Address, item.Data)
}

func rewriteSender(from, originalAddr string) string {
	// Apply a send-only Sender Rewriting Scheme (SRS).
	// This is used when we are sending from a (potentially) non-local domain,
	// to a non-local domain.
	// This should happen only when there's an alias to forward email to a
	// non-local domain (either a normal "email" alias with a remote
	// destination, or a "forward" alias with a list of servers).
	// In this case, using the original From is problematic, as we may not be
	// an authorized sender for this.
	// To do this, we use a sender rewriting scheme, similar to what other
	// MTAs do (e.g. gmail or postfix).
	// Note this assumes "+" is an alias suffix separator.
	// We use the IDNA version of the domain if possible, because
	// we can't know if the other side will support SMTPUTF8.
	return fmt.Sprintf("%s+fwd_from=%s@%s",
		envelope.UserOf(originalAddr),
		strings.Replace(from, "@", "=", -1),
		mustIDNAToASCII(envelope.DomainOf(originalAddr)))
}

// countRcpt counts how many recipients are in the given status.
func (item *Item) countRcpt(statuses ...Recipient_Status) int {
	c := 0
	for _, rcpt := range item.Rcpt {
		for _, status := range statuses {
			if rcpt.Status == status {
				c++
				break
			}
		}
	}
	return c
}

func sendDSN(tr *trace.Trace, q *Queue, item *Item) {
	tr.Debugf("sending DSN")

	// Pick a (local) domain to send the DSN from. We should always find one,
	// as otherwise we're relaying.
	domain := "unknown"
	if item.From != "<>" && envelope.DomainIn(item.From, q.localDomains) {
		domain = envelope.DomainOf(item.From)
	} else {
		for _, rcpt := range item.Rcpt {
			if envelope.DomainIn(rcpt.OriginalAddress, q.localDomains) {
				domain = envelope.DomainOf(rcpt.OriginalAddress)
				break
			}
		}
	}

	msg, err := deliveryStatusNotification(domain, item)
	if err != nil {
		tr.Errorf("failed to build DSN: %v", err)
		return
	}

	// TODO: DKIM signing.

	id, err := q.Put(tr, "<>", []string{item.From}, msg)
	if err != nil {
		tr.Errorf("failed to queue DSN: %v", err)
		return
	}

	tr.Printf("queued DSN: %s", id)
	dsnQueued.Add(1)
}

func nextDelay(createdAt time.Time) time.Duration {
	var delay time.Duration

	since := time.Since(createdAt)
	switch {
	case since < 1*time.Minute:
		delay = 1 * time.Minute
	case since < 5*time.Minute:
		delay = 5 * time.Minute
	case since < 10*time.Minute:
		delay = 10 * time.Minute
	default:
		delay = 20 * time.Minute
	}

	// Perturb the delay, to avoid all queued emails to be retried at the
	// exact same time after a restart.
	delay += rand.N(60 * time.Second)
	return delay
}

func mustIDNAToASCII(s string) string {
	a, err := idna.ToASCII(s)
	if err != nil {
		return a
	}
	return s
}

func timeFromProto(ts *Timestamp) time.Time {
	return time.Unix(ts.Seconds, int64(ts.Nanos)).UTC()
}

func timeToProto(t time.Time) *Timestamp {
	return &Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}
