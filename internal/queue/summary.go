package queue

import (
	"sort"
	"strings"
	"time"

	"blitiri.com.ar/go/chasquid/internal/envelope"
)

// RecipientSummary holds a structured view of a single recipient.
type RecipientSummary struct {
	Address            string `json:"address"`
	Type               string `json:"type"`
	Status             string `json:"status"`
	LastFailureMessage string `json:"last_failure_message,omitempty"`
	OriginalAddress    string `json:"original_address,omitempty"`
}

// ItemSummary holds a structured view of a single queue item.
type ItemSummary struct {
	ID        string             `json:"id"`
	From      string             `json:"from"`
	To        []string           `json:"to"`
	Recipients []RecipientSummary `json:"recipients"`
	CreatedAt time.Time          `json:"created_at"`
	Age       string             `json:"age"`
}

// QueueSummary holds a structured overview of the entire queue.
type QueueSummary struct {
	Date         time.Time     `json:"date"`
	Length       int           `json:"length"`
	PendingCount int           `json:"pending_count"`
	SentCount    int           `json:"sent_count"`
	FailedCount  int           `json:"failed_count"`
	OldestAge    string        `json:"oldest_age,omitempty"`
	NewestAge    string        `json:"newest_age,omitempty"`
	Items        []ItemSummary `json:"items"`
}

// SummaryFilter controls which items are included in the summary.
type SummaryFilter struct {
	// Domain filters items to those that have at least one recipient in this
	// domain (matched against recipient.Address or recipient.OriginalAddress).
	// Empty means no domain filter.
	Domain string

	// Status filters items to those that have at least one recipient with this
	// status. One of "PENDING", "SENT", "FAILED", or empty for all.
	Status string

	// From filters items whose From address contains this substring
	// (case-insensitive). Empty means no filter.
	From string
}

// SummarySort controls the ordering of items in the summary.
type SummarySort string

const (
	// SortByAgeAsc sorts oldest items first.
	SortByAgeAsc SummarySort = "age_asc"
	// SortByAgeDesc sorts newest items first.
	SortByAgeDesc SummarySort = "age_desc"
)

// Summary returns a structured overview of the current queue state.
// It honours the provided filter and sort options.
func (q *Queue) Summary(filter SummaryFilter, sortKey SummarySort) QueueSummary {
	q.mu.RLock()
	defer q.mu.RUnlock()

	now := time.Now()

	var (
		oldestTime time.Time
		newestTime time.Time
		items      []ItemSummary
		pending    int
		sent       int
		failed     int
	)

	statusUpper := strings.ToUpper(filter.Status)

	for _, item := range q.q {
		item.Lock()

		// Build recipient summaries and gather per-item stats.
		var rcptSummaries []RecipientSummary
		itemPending, itemSent, itemFailed := 0, 0, 0
		matched := false

		for _, rcpt := range item.Rcpt {
			switch rcpt.Status {
			case Recipient_PENDING:
				itemPending++
			case Recipient_SENT:
				itemSent++
			case Recipient_FAILED:
				itemFailed++
			}

			// Apply domain filter at the recipient level.
			if filter.Domain != "" {
				addrDomain := envelope.DomainOf(rcpt.Address)
				origDomain := envelope.DomainOf(rcpt.OriginalAddress)
				if !strings.EqualFold(addrDomain, filter.Domain) &&
					!strings.EqualFold(origDomain, filter.Domain) {
					continue
				}
				matched = true
			}

			// Apply status filter at the recipient level.
			if statusUpper != "" {
				if rcpt.Status.String() != statusUpper {
					continue
				}
				matched = true
			}

			rcptSummaries = append(rcptSummaries, RecipientSummary{
				Address:            rcpt.Address,
				Type:               rcpt.Type.String(),
				Status:             rcpt.Status.String(),
				LastFailureMessage: rcpt.LastFailureMessage,
				OriginalAddress:    rcpt.OriginalAddress,
			})
		}

		// Apply from filter (case-insensitive substring match).
		if filter.From != "" {
			if !strings.Contains(strings.ToLower(item.From), strings.ToLower(filter.From)) {
				item.Unlock()
				continue
			}
			matched = true
		}

		// If any filter is active and nothing matched, skip this item.
		if (filter.Domain != "" || statusUpper != "" || filter.From != "") && !matched {
			item.Unlock()
			continue
		}

		// Accumulate recipient-level counts (all recipients, not just filtered).
		pending += itemPending
		sent += itemSent
		failed += itemFailed

		// Track oldest/newest.
		if oldestTime.IsZero() || item.CreatedAt.Before(oldestTime) {
			oldestTime = item.CreatedAt
		}
		if newestTime.IsZero() || item.CreatedAt.After(newestTime) {
			newestTime = item.CreatedAt
		}

		age := now.Sub(item.CreatedAt)
		items = append(items, ItemSummary{
			ID:         item.ID,
			From:       item.From,
			To:         item.To,
			Recipients: rcptSummaries,
			CreatedAt:  item.CreatedAt.UTC(),
			Age:        age.Round(time.Second).String(),
		})

		item.Unlock()
	}

	// Sort items.
	switch sortKey {
	case SortByAgeDesc:
		sort.Slice(items, func(i, j int) bool {
			return items[i].CreatedAt.After(items[j].CreatedAt)
		})
	default: // SortByAgeAsc or unspecified
		sort.Slice(items, func(i, j int) bool {
			return items[i].CreatedAt.Before(items[j].CreatedAt)
		})
	}

	summary := QueueSummary{
		Date:         now.UTC(),
		Length:       len(items),
		PendingCount: pending,
		SentCount:    sent,
		FailedCount:  failed,
		Items:        items,
	}
	if !oldestTime.IsZero() {
		summary.OldestAge = now.Sub(oldestTime).Round(time.Second).String()
	}
	if !newestTime.IsZero() {
		summary.NewestAge = now.Sub(newestTime).Round(time.Second).String()
	}

	return summary
}
