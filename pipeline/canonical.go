package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// canonicalEventBody is the version-independent projection used by the
// gray-rollout dedup layer. SchemaVer is deliberately excluded: the SAME
// logical event delivered once by an old producer and once by a new producer
// must hash identically, so a version label alone must never be a conflict.
type canonicalEventBody struct {
	Symbol    string         `json:"symbol"`
	EventTime int64          `json:"event_time"`
	Price     int64          `json:"price"`
	Quantity  int64          `json:"quantity"`
	IsTrade   bool           `json:"is_trade"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// Canonicalize rebuilds an event as it would be read under the newest
// registered schema version:
//
//   - every optional field absent from the event is backfilled with its
//     DECLARED default (stable and replayable; filling depends only on the
//     registered schema chain),
//   - numeric values on fields whose newest type is float64 are normalized to
//     float64, so the int64->float64 widening reads identically when the value
//     is unchanged,
//   - the returned event is stamped with the newest schema version.
//
// A field added WITHOUT a declared default is left absent on old events:
// carrying it vs. omitting it is then a substantive difference, not a version
// difference.
func (r *Registry) Canonicalize(e market.Event) (market.Event, []string, market.RejectCause, string) {
	latest, own, cause, detail := r.lookupChain(e.SchemaVer)
	if cause != market.CauseNone {
		return market.Event{}, nil, cause, detail
	}

	out := cloneEvent(e)
	if len(out.Fields) == 0 {
		out.Fields = map[string]any{}
	}
	var filled []string

	// Walk the schema chain from the event's own version up to the latest, so
	// a default introduced at any future version is applied in version order.
	for v := own; v <= latest; v++ {
		r.mu.RLock()
		s := r.schemas[v]
		r.mu.RUnlock()
		for _, f := range s.Fields {
			val, present := fieldValue(out, f.Name)
			if !present || val == nil {
				if f.HasDefault {
					setFieldValue(&out, f.Name, f.Default)
					filled = append(filled, f.Name)
				}
				continue
			}
			setFieldValue(&out, f.Name, normalizeNumeric(val, f.Type))
		}
	}

	out.SchemaVer = latest
	return out, filled, market.CauseNone, ""
}

func (r *Registry) lookupChain(version int) (latest, own int, cause market.RejectCause, detail string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.schemas[version]; !ok {
		return 0, 0, market.CauseSchemaUnknownVersion,
			fmt.Sprintf("schema version %d is not registered", version)
	}
	return r.latestVersionLocked(), version, market.CauseNone, ""
}

// DedupFingerprint returns the stable digest of an event's LOGICAL content for
// cross-version deduplication:
//
//   - the schema-version label is excluded;
//   - int64 payloads on float64-widened fields are normalized to float64, so
//     a pure type widening with an unchanged value compares equal;
//   - an optional field whose carried value equals its DECLARED default is
//     excluded (exactly the value a backfilled old event gets), so a new
//     producer sending the default and an old producer omitting the field
//     describe the same logical event.
//
// A real price/quantity difference or a new field carrying a NON-default
// value changes the digest and is therefore a conflict, never a duplicate.
// The digest is stable when newer versions are registered later: newly added
// defaults never alter the digest of previously stored events.
func (r *Registry) DedupFingerprint(e market.Event) (string, market.RejectCause, string) {
	latest, own, cause, detail := r.lookupChain(e.SchemaVer)
	if cause != market.CauseNone {
		return "", cause, detail
	}

	view := cloneEvent(e)
	view.SchemaVer = latest
	view.Fields = make(map[string]any)

	// Latest-known specs only: field types may widen across the chain but
	// never change in any other way, so the newest spec dominates.
	r.mu.RLock()
	latestSpec := r.schemas[latest]
	r.mu.RUnlock()
	for _, f := range latestSpec.Fields {
		val, present := fieldValue(e, f.Name)
		if !present || val == nil {
			continue // omitted = default value; excluded either way
		}
		if isCoreField(f.Name) {
			continue // core fields always hash from the struct fields
		}
		val = normalizeNumeric(val, f.Type)
		if f.HasDefault && defaultValueEqual(f.Default, f.Type, val) {
			continue // carried default == omitted field: not a distinction
		}
		view.Fields[f.Name] = val
	}
	_ = own // the event's own schema existence was checked above
	return CanonicalFingerprint(view), market.CauseNone, ""
}

// CanonicalFingerprint returns the deterministic digest of a version-stripped
// canonical projection.
func CanonicalFingerprint(e market.Event) string {
	body := canonicalEventBody{
		Symbol:    e.Symbol,
		EventTime: e.EventTime,
		Price:     e.Price,
		Quantity:  e.Quantity,
		IsTrade:   e.IsTrade,
		Fields:    sortedFields(e.Fields),
	}
	b, _ := json.Marshal(body)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:16]
}

// normalizeNumeric maps an int64 payload on a float64-declared field to
// float64; this is the transparent read-time form of the only allowed
// widening. All other values pass through unchanged.
func normalizeNumeric(v any, k market.TypeKind) any {
	if k != market.TypeFloat64 {
		return v
	}
	if f, ok := numericAsFloat(v); ok {
		return f
	}
	return v
}

func isCoreField(name string) bool {
	switch name {
	case FieldEventID, FieldSymbol, FieldEventTime, FieldPrice, FieldQuantity, FieldIsTrade:
		return true
	}
	return false
}

// setFieldValue writes a backfilled/normalized extension field value. Core
// fields live on the Event struct, are never optional and cannot be
// backfilled, so they are intentionally ignored here.
func setFieldValue(e *market.Event, name string, v any) {
	if isCoreField(name) {
		return
	}
	e.Fields[name] = v
}
