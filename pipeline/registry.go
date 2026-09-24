package pipeline

import (
	"fmt"
	"sort"
	"sync"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// FieldSpec describes one field of a schema version.
type FieldSpec struct {
	Name string          `json:"name"`
	Type market.TypeKind `json:"type"`
}

// Schema is one numbered, immutable revision of the event structure.
type Schema struct {
	Version int         `json:"version"`
	Fields  []FieldSpec `json:"fields"`
}

// CompatReport is the distinguishable result of a proposed schema evolution.
type CompatReport struct {
	OK       bool               `json:"ok"`
	From     int                `json:"from"`
	To       int                `json:"to"`
	Reason   market.RejectCause `json:"reason"`
	Field    string             `json:"field,omitempty"`
	FromType market.TypeKind    `json:"from_type,omitempty"`
	ToType   market.TypeKind    `json:"to_type,omitempty"`
	Detail   string             `json:"detail,omitempty"`
}

// Well-known field names mapped to Event struct fields. Every other declared
// field is taken from Event.Fields.
const (
	FieldEventID   = "event_id"
	FieldSymbol    = "symbol"
	FieldEventTime = "event_time"
	FieldPrice     = "price"
	FieldQuantity  = "quantity"
	FieldIsTrade   = "is_trade"
)

// CoreSchemaV1 is the mandatory initial event structure.
var CoreSchemaV1 = Schema{
	Version: 1,
	Fields: []FieldSpec{
		{FieldEventID, market.TypeString},
		{FieldSymbol, market.TypeString},
		{FieldEventTime, market.TypeInt64},
		{FieldPrice, market.TypeInt64},
		{FieldQuantity, market.TypeInt64},
		{FieldIsTrade, market.TypeBool},
	},
}

// Registry holds every schema version ever registered. It is safe for
// concurrent use. Registration order defines the history against which a
// replayed event is checked: an event is ALWAYS validated against the schema
// version it declares, so old events keep their original conclusions.
type Registry struct {
	mu      sync.RWMutex
	schemas map[int]Schema
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{schemas: make(map[int]Schema)}
}

// Register adds a new schema version.
//
// Compatibility rules (incremental evolution only):
//   - the first schema MUST be version 1;
//   - versions MUST be registered consecutively (no gaps, no regression);
//   - versions MUST advance consecutively (no gaps); once v3 exists,
//     replaying an identical v1/v2 registration is idempotent, while a
//     different body under an existing number is rejected
//     (schema_duplicate_version);
//   - fields may only be ADDED; removing or renaming a field is rejected
//     (schema_removing_field);
//   - the only allowed type change is the widening int64 -> float64; any
//     narrowing or cross-kind change is rejected (schema_type_incompatible).
func (r *Registry) Register(s Schema) (*CompatReport, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(s.Fields) == 0 {
		return r.bad(s.Version, s.Version, market.CauseSchemaEmpty, "", "", "",
			"schema declares no fields"), &RejectedError{Cause: market.CauseSchemaEmpty}
	}
	if dup := duplicateFieldName(s.Fields); dup != "" {
		return r.bad(s.Version, s.Version, market.CauseSchemaEmpty, dup, "", "",
				"duplicate field name in schema"), &RejectedError{Cause: market.CauseSchemaEmpty,
				Detail: "duplicate field: " + dup}
	}

	if len(r.schemas) == 0 {
		if s.Version != 1 {
			return r.bad(0, s.Version, market.CauseSchemaVersionGap, "", "", "",
					"first schema must be version 1"),
				&RejectedError{Cause: market.CauseSchemaVersionGap}
		}
		r.schemas[1] = s
		return &CompatReport{OK: true, From: 0, To: 1}, nil
	}

	latest := r.latestVersionLocked()

	if existing, ok := r.schemas[s.Version]; ok {
		// Re-registering ANY existing version with an identical body is
		// idempotent: this is what allows replaying the whole registration
		// history and obtaining the same conclusions as the first run.
		if schemaEqual(existing, s) {
			return &CompatReport{OK: true, From: s.Version, To: s.Version,
				Detail: "identical schema already registered"}, nil
		}
		// The version number already denotes an immutable historical schema;
		// a different body under that number is rejected.
		return r.bad(s.Version, s.Version, market.CauseSchemaDuplicateVersion, "", "", "",
				"schema version already registered with a different body"),
			&RejectedError{Cause: market.CauseSchemaDuplicateVersion}
	}

	if s.Version < latest {
		return r.bad(latest, s.Version, market.CauseSchemaVersionRegressed, "", "", "",
				fmt.Sprintf("version %d is below latest %d", s.Version, latest)),
			&RejectedError{Cause: market.CauseSchemaVersionRegressed}
	}
	if s.Version != latest+1 {
		return r.bad(latest, s.Version, market.CauseSchemaVersionGap, "", "", "",
				fmt.Sprintf("expected version %d, got %d", latest+1, s.Version)),
			&RejectedError{Cause: market.CauseSchemaVersionGap}
	}

	prev := r.schemas[latest]
	if rep, err := checkEvolution(prev, s); err != nil {
		return rep, err
	}

	r.schemas[s.Version] = s
	return &CompatReport{OK: true, From: latest, To: s.Version}, nil
}

// Get returns the schema with the given version.
func (r *Registry) Get(version int) (Schema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.schemas[version]
	return s, ok
}

// Versions returns all registered version numbers in ascending order.
func (r *Registry) Versions() []int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]int, 0, len(r.schemas))
	for v := range r.schemas {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

// Validate checks an event against the schema version it declares. Events are
// ALWAYS checked against their own version, which is what makes replaying
// historical data produce the same conclusion as the first processing.
func (r *Registry) Validate(e market.Event) (market.RejectCause, string) {
	r.mu.RLock()
	s, ok := r.schemas[e.SchemaVer]
	r.mu.RUnlock()
	if !ok {
		return market.CauseSchemaUnknownVersion,
			fmt.Sprintf("schema version %d is not registered", e.SchemaVer)
	}

	declared := make(map[string]market.TypeKind, len(s.Fields))
	for _, f := range s.Fields {
		declared[f.Name] = f.Type
	}

	for _, f := range s.Fields {
		v, present := fieldValue(e, f.Name)
		if !present || v == nil {
			return market.CauseSchemaMissingField,
				fmt.Sprintf("field %q required by schema v%d is absent", f.Name, s.Version)
		}
		if !valueMatchesType(v, f.Type) {
			return market.CauseSchemaFieldTypeMismatch,
				fmt.Sprintf("field %q: expected %s, got %T", f.Name, f.Type, v)
		}
	}

	// Any carried extra field that the declared schema does not know about is
	// a forward-incompatible payload and must be rejected.
	for name := range e.Fields {
		if _, ok := declared[name]; !ok {
			return market.CauseSchemaExtraField,
				fmt.Sprintf("field %q is not declared by schema v%d", name, s.Version)
		}
	}
	return market.CauseNone, ""
}

func (r *Registry) latestVersionLocked() int {
	latest := 0
	for v := range r.schemas {
		if v > latest {
			latest = v
		}
	}
	return latest
}

func (r *Registry) bad(from, to int, cause market.RejectCause, field string,
	fromType, toType market.TypeKind, detail string) *CompatReport {
	return &CompatReport{OK: false, From: from, To: to, Reason: cause,
		Field: field, FromType: fromType, ToType: toType, Detail: detail}
}

// checkEvolution verifies prev -> s under the incremental evolution rules.
func checkEvolution(prev, s Schema) (*CompatReport, error) {
	newFields := make(map[string]market.TypeKind, len(s.Fields))
	for _, f := range s.Fields {
		newFields[f.Name] = f.Type
	}
	for _, old := range prev.Fields {
		nt, ok := newFields[old.Name]
		if !ok {
			rep := &CompatReport{OK: false, From: prev.Version, To: s.Version,
				Reason: market.CauseSchemaRemovingField, Field: old.Name,
				FromType: old.Type, Detail: "fields cannot be removed or renamed"}
			return rep, &RejectedError{Cause: market.CauseSchemaRemovingField,
				Detail: "field removed or renamed: " + old.Name}
		}
		if nt != old.Type {
			if old.Type == market.TypeInt64 && nt == market.TypeFloat64 {
				continue // the single permitted widening
			}

			cause := market.CauseSchemaTypeIncompatible
			detail := fmt.Sprintf("field %q cannot change %s -> %s", old.Name, old.Type, nt)
			rep := &CompatReport{OK: false, From: prev.Version, To: s.Version,
				Reason: cause, Field: old.Name, FromType: old.Type, ToType: nt,
				Detail: detail}
			return rep, &RejectedError{Cause: cause, Detail: detail}
		}
	}
	return nil, nil
}

func schemaEqual(a, b Schema) bool {
	if a.Version != b.Version || len(a.Fields) != len(b.Fields) {
		return false
	}
	// Registration preserves order, but compare as sets to be order-agnostic.
	am := make(map[string]market.TypeKind, len(a.Fields))
	for _, f := range a.Fields {
		am[f.Name] = f.Type
	}
	for _, f := range b.Fields {
		if am[f.Name] != f.Type {
			return false
		}
	}
	return true
}

func duplicateFieldName(fs []FieldSpec) string {
	seen := make(map[string]struct{}, len(fs))
	for _, f := range fs {
		if _, ok := seen[f.Name]; ok {
			return f.Name
		}
		seen[f.Name] = struct{}{}
	}
	return ""
}

// fieldValue resolves a declared field name on an event.
func fieldValue(e market.Event, name string) (any, bool) {
	switch name {
	case FieldEventID:
		return e.EventID, e.EventID != ""
	case FieldSymbol:
		return e.Symbol, e.Symbol != ""
	case FieldEventTime:
		return e.EventTime, true
	case FieldPrice:
		return e.Price, true
	case FieldQuantity:
		return e.Quantity, true
	case FieldIsTrade:
		return e.IsTrade, true
	default:
		v, ok := e.Fields[name]
		return v, ok
	}
}

func valueMatchesType(v any, k market.TypeKind) bool {
	switch k {
	case market.TypeInt64:
		_, ok := v.(int64)
		return ok
	case market.TypeFloat64:
		// int64 payloads are accepted where float64 is declared (the allowed
		// widening is transparent on read).
		switch v.(type) {
		case float64, int64:
			return true
		}
		return false
	case market.TypeString:
		_, ok := v.(string)
		return ok
	case market.TypeBool:
		_, ok := v.(bool)
		return ok
	}
	return false
}
