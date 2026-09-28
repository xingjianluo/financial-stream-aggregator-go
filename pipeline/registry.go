package pipeline

import (
	"fmt"
	"sort"
	"sync"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// FieldSpec describes one field of a schema version.
//
// A field is OPTIONAL iff it declares a default (HasDefault=true): events that
// omit it are valid and are backfilled with Default when they are read or
// rebuilt under a newer schema version. A field without a default stays
// required, exactly as before the gray-rollout layer existed.
type FieldSpec struct {
	Name string          `json:"name"`
	Type market.TypeKind `json:"type"`
	// HasDefault marks the field optional and Default as its declared
	// backfill value. Default must match Type when HasDefault is set.
	HasDefault bool `json:"has_default,omitempty"`
	Default    any  `json:"default,omitempty"`
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
		{Name: FieldEventID, Type: market.TypeString},
		{Name: FieldSymbol, Type: market.TypeString},
		{Name: FieldEventTime, Type: market.TypeInt64},
		{Name: FieldPrice, Type: market.TypeInt64},
		{Name: FieldQuantity, Type: market.TypeInt64},
		{Name: FieldIsTrade, Type: market.TypeBool},
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
	return r.register(s, false)
}

// RegisterUpgrade registers a new schema version under the additional
// GRAY-ROLLOUT rule: every field the new version ADDS must declare a safe
// default (FieldSpec.HasDefault with a type-matching Default). An upgrade
// whose new fields have no safe default would invalidate or make
// un-rebuildable the events that already exist, so it is refused BEFORE it
// takes effect with the distinguishable cause schema_no_safe_default —
// never allowed through to fail later at runtime.
func (r *Registry) RegisterUpgrade(s Schema) (*CompatReport, error) {
	return r.register(s, true)
}

func (r *Registry) register(s Schema, grayRollout bool) (*CompatReport, error) {
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
	// A declared default must be a value of the declared type; a malformed
	// default would make every later backfill non-deterministic garbage.
	if f := badDefault(s.Fields); f != nil {
		detail := fmt.Sprintf("field %q: default value does not match declared type %s",
			f.Name, f.Type)
		return r.bad(s.Version, s.Version, market.CauseSchemaFieldTypeMismatch,
				f.Name, f.Type, "", detail),
			&RejectedError{Cause: market.CauseSchemaFieldTypeMismatch, Detail: detail}
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
	if grayRollout {
		if rep, err := checkGrayDefaults(prev, s); err != nil {
			return rep, err
		}
	}

	r.schemas[s.Version] = s
	return &CompatReport{OK: true, From: latest, To: s.Version}, nil
}

// checkGrayDefaults enforces the gray-rollout rule: every field ADDED by the
// new version must be optional with a declared, type-matching safe default.
func checkGrayDefaults(prev, s Schema) (*CompatReport, error) {
	old := make(map[string]struct{}, len(prev.Fields))
	for _, f := range prev.Fields {
		old[f.Name] = struct{}{}
	}
	for _, f := range s.Fields {
		if _, existed := old[f.Name]; existed {
			continue
		}
		if !f.HasDefault {
			detail := fmt.Sprintf(
				"new field %q has no safe default; existing events could not be rebuilt under v%d",
				f.Name, s.Version)
			rep := &CompatReport{OK: false, From: prev.Version, To: s.Version,
				Reason: market.CauseSchemaNoSafeDefault, Field: f.Name,
				FromType: f.Type, Detail: detail}
			return rep, &RejectedError{Cause: market.CauseSchemaNoSafeDefault,
				Detail: detail}
		}
	}
	return nil, nil
}

// Clone returns a deep, independent copy of the registry. It backs read-only
// dry-run previews: a proposed upgrade is rehearsed on the clone while the
// online registry stays untouched.
func (r *Registry) Clone() *Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := &Registry{schemas: make(map[int]Schema, len(r.schemas))}
	for v, s := range r.schemas {
		fs := make([]FieldSpec, len(s.Fields))
		copy(fs, s.Fields)
		s.Fields = fs
		out.schemas[v] = s
	}
	return out
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
			// Optional fields (declared default) may be absent on the wire;
			// they are deterministically backfilled at read/rebuild time
			// (see Canonicalize), so old events never fail as incomplete.
			if f.HasDefault {
				continue
			}
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
	am := make(map[string]FieldSpec, len(a.Fields))
	for _, f := range a.Fields {
		am[f.Name] = f
	}
	for _, f := range b.Fields {
		g, ok := am[f.Name]
		if !ok || g.Type != f.Type || g.HasDefault != f.HasDefault {
			return false
		}
		if g.HasDefault && !defaultValueEqual(g.Default, g.Type, f.Default) {
			return false
		}
	}
	return true
}

// defaultValueEqual compares a stored default with a candidate default under
// the field's declared type, applying the same numeric-widening transparency
// as valueMatchesType (an int64 default equals the integral float64 one).
func defaultValueEqual(stored any, k market.TypeKind, candidate any) bool {
	if k == market.TypeFloat64 {
		sf, sok := numericAsFloat(stored)
		cf, cok := numericAsFloat(candidate)
		return sok && cok && sf == cf
	}
	return stored == candidate
}

// numericAsFloat reads an int64/float64 payload as float64.
func numericAsFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// badDefault returns the first field whose declared default does not match
// its declared type, or nil.
func badDefault(fs []FieldSpec) *FieldSpec {
	for i := range fs {
		f := &fs[i]
		if f.HasDefault && !valueMatchesType(f.Default, f.Type) {
			return f
		}
	}
	return nil
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
