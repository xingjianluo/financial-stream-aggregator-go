package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// eventBody is the version-agnostic serializable projection of an event.
type eventBody struct {
	SchemaVer int            `json:"schema_ver"`
	Symbol    string         `json:"symbol"`
	EventTime int64          `json:"event_time"`
	Price     int64          `json:"price"`
	Quantity  int64          `json:"quantity"`
	IsTrade   bool           `json:"is_trade"`
	Fields    map[string]any `json:"fields,omitempty"`
}

func newEventBody(e market.Event) eventBody {
	return eventBody{
		SchemaVer: e.SchemaVer,
		Symbol:    e.Symbol,
		EventTime: e.EventTime,
		Price:     e.Price,
		Quantity:  e.Quantity,
		IsTrade:   e.IsTrade,
		Fields:    sortedFields(e.Fields),
	}
}

// ContentFingerprint returns a deterministic digest of an event's RAW wire
// content. The logical identity (EventID) is deliberately excluded, but the
// declared schema version and all carried fields are included, so this digest
// distinguishes different encodings of the same logical event.
//
// Deduplication during a gray schema rollout instead uses
// Registry.DedupFingerprint, which strips the version label, normalizes the
// int64->float64 widening and treats default-valued optional fields as
// absent; ContentFingerprint remains available as the raw-content audit
// digest.
//
// Canonical encoding relies on JSON scalar payloads and encoding/json's
// sorted map keys, so identical content always yields the same digest.
func ContentFingerprint(e market.Event) string {
	b, _ := json.Marshal(newEventBody(e))
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:16]
}

// sortedFields copies m with deterministically ordered keys. encoding/json
// already sorts map keys; the helper states the canonicalization contract and
// is the extension point for nested types.
func sortedFields(m map[string]any) map[string]any {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]any, len(m))
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out[k] = m[k]
	}
	return out
}
