package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// FieldSpec describes one field of a schema version.
//
// HasDefault/Default declare an OPTIONAL field with an explicit safe default:
// an event whose own (older) schema does not carry the field can be read or
// rebuilt under this version by filling Default. A gray-upgrade schema may
// only ADD fields that carry such a default; adding a field without one is
// rejected before the upgrade takes effect.
type FieldSpec struct {
	Name       string          `json:"name"`
	Type       market.TypeKind `json:"type"`
	HasDefault bool            `json:"has_default,omitempty"`
	Default    any             `json:"default,omitempty"`
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
	r.mu.Lock()
	defer r.mu.Unlock()

	if rep, err := r.validateCandidateLocked(s); err != nil {
		return rep, err
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

// snapshotLocked copies the version->schema map. Schema values are immutable
// once registered, so a shallow copy of the map is a sufficient detached view.
// Callers must hold r.mu (read or write).
func (r *Registry) snapshotLocked() map[int]Schema {
	out := make(map[int]Schema, len(r.schemas))
	for v, s := range r.schemas {
		out[v] = s
	}
	return out
}

// newRegistryFromMap builds a registry over an independently owned copy of
// schemas. It is the constructor for the dry run's detached registry view:
// trial runs register the candidate schema ONLY on that copy, never on the
// live registry.
func newRegistryFromMap(schemas map[int]Schema) *Registry {
	out := make(map[int]Schema, len(schemas))
	for v, s := range schemas {
		out[v] = s
	}
	return &Registry{schemas: out}
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
	return validateAgainst(e, s)
}

// validateAgainst checks an event against an explicit schema. Events are
// ALWAYS checked against their own version during live processing, which is
// what makes replaying historical data reproduce the first conclusion; the
// dry-run uses the candidate schema before it is registered.
func validateAgainst(e market.Event, s Schema) (market.RejectCause, string) {
	declared := make(map[string]market.TypeKind, len(s.Fields))
	for _, f := range s.Fields {
		declared[f.Name] = f.Type
	}

	for _, f := range s.Fields {
		v, present := fieldValue(e, f.Name)
		if !present || v == nil {
			// An optional field with a declared safe default may be absent:
			// the read side fills it, so an old/partial payload is not a
			// structurally incomplete event.
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

// CommitUpgrade enables a candidate schema as the next version under the
// GRAY-UPGRADE safety rules. It performs every plain Register compatibility
// check and additionally requires every newly added field to carry a safe
// default: otherwise old producers' events would become structurally
// incomplete the moment the upgrade takes effect. The upgrade is refused
// BEFORE it can invalidate existing events (cause
// schema_added_field_without_default), distinct from all other structural
// causes.
//
// Re-registering an already-present, identical version is idempotent, exactly
// like Register, so replaying the commit history is reproducible.
func (r *Registry) CommitUpgrade(s Schema) (*CompatReport, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if rep, err := r.validateCandidateLocked(s); err != nil {
		return rep, err
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
		if schemaEqual(existing, s) {
			return &CompatReport{OK: true, From: s.Version, To: s.Version,
				Detail: "identical schema already registered"}, nil
		}
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
	if rep, err := checkGraySafety(prev, s); err != nil {
		return rep, err
	}
	r.schemas[s.Version] = s
	return &CompatReport{OK: true, From: latest, To: s.Version}, nil
}

// validateCandidateLocked runs the version-independent body checks shared by
// Register and CommitUpgrade.
func (r *Registry) validateCandidateLocked(s Schema) (*CompatReport, error) {
	if len(s.Fields) == 0 {
		return r.bad(s.Version, s.Version, market.CauseSchemaEmpty, "", "", "",
			"schema declares no fields"), &RejectedError{Cause: market.CauseSchemaEmpty}
	}
	if dup := duplicateFieldName(s.Fields); dup != "" {
		return r.bad(s.Version, s.Version, market.CauseSchemaEmpty, dup, "", "",
				"duplicate field name in schema"), &RejectedError{Cause: market.CauseSchemaEmpty,
				Detail: "duplicate field: " + dup}
	}
	for _, f := range s.Fields {
		if f.HasDefault && !valueMatchesType(f.Default, f.Type) {
			return r.bad(s.Version, s.Version,
					market.CauseSchemaDefaultTypeMismatch, f.Name, f.Type, f.Type,
					fmt.Sprintf("default value %v does not match type %s", f.Default, f.Type)),
				&RejectedError{Cause: market.CauseSchemaDefaultTypeMismatch,
					Detail: fmt.Sprintf("field %s default %v is not %s", f.Name, f.Default, f.Type)}
		}
	}
	return nil, nil
}

// checkGraySafety enforces the gray-upgrade-only rule: any field added by the
// new version MUST be optional with a declared safe default.
func checkGraySafety(prev, s Schema) (*CompatReport, error) {
	old := make(map[string]FieldSpec, len(prev.Fields))
	for _, f := range prev.Fields {
		old[f.Name] = f
	}
	for _, f := range s.Fields {
		if _, existed := old[f.Name]; existed {
			continue
		}
		if !f.HasDefault {
			rep := &CompatReport{OK: false, From: prev.Version, To: s.Version,
				Reason: market.CauseSchemaAddedFieldNoDefault, Field: f.Name,
				ToType: f.Type,
				Detail: "newly added field has no safe default; existing events would become invalid"}
			return rep, &RejectedError{Cause: market.CauseSchemaAddedFieldNoDefault,
				Detail: "field added without a default: " + f.Name}
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
		af, ok := am[f.Name]
		if !ok {
			return false
		}
		if af.Type != f.Type || af.HasDefault != f.HasDefault ||
			!reflect.DeepEqual(af.Default, f.Default) {
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
//
// A value carried in Event.Fields takes precedence over the fixed struct
// slots: when a numeric core field (e.g. quantity) is widened to float64 in a
// later schema, the new-version producer has no int64 struct slot to put the
// widened value in and therefore delivers it through Fields. Old events leave
// Fields empty and read from the struct slots as before.
func fieldValue(e market.Event, name string) (any, bool) {
	if e.Fields != nil {
		if v, ok := e.Fields[name]; ok && v != nil {
			return v, true
		}
	}
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
		return nil, false
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

// projectEventTo fills absent optional fields of e with the defaults declared
// by target, returning the rebuilt event plus the sorted list of field names
// that were defaulted. It does NOT mutate the input.
//
// A present value that cannot be read under target's declared type (anything
// other than the int64->float64 widening) is a per-data failure carrying a
// distinguishable cause.
func projectEventTo(e market.Event, target Schema) (market.Event, []string, market.RejectCause, string) {
	out := e
	if len(e.Fields) > 0 {
		out.Fields = make(map[string]any, len(e.Fields))
		for k, v := range e.Fields {
			out.Fields[k] = v
		}
	}
	defaulted := map[string]struct{}{}
	for _, f := range target.Fields {
		v, present := fieldValue(e, f.Name)
		if !present || v == nil {
			if !f.HasDefault {
				return out, nil, market.CauseSchemaMissingField,
					fmt.Sprintf("field %q has no value and no default under v%d", f.Name, target.Version)
			}
			if !isCoreField(f.Name) {
				if out.Fields == nil {
					out.Fields = make(map[string]any)
				}
				out.Fields[f.Name] = f.Default
			}
			defaulted[f.Name] = struct{}{}
			continue
		}
		if !valueMatchesType(v, f.Type) {
			return out, nil, market.CauseSchemaFieldTypeMismatch,
				fmt.Sprintf("field %q: cannot read %T as %s under v%d", f.Name, v, f.Type, target.Version)
		}
	}
	// Carried extras not declared by target stay as per-version extras; the
	// source-version validation already guarantees no undeclared extras.
	return out, sortedKeys(defaulted), market.CauseNone, ""
}

// semanticFP projects an event onto target (default filling) and then renders
// every declared field through target's type, so the ONLY permitted type
// change (int64 -> float64) collapses equal numeric magnitudes to the same
// bytes. The schema version is intentionally excluded: same logical content
// from the old and the new gray producer must hash identically, while a
// genuine difference in price/quantity/a new field hashes differently.
func semanticFP(e market.Event, target Schema) (string, []string, market.RejectCause, string) {
	proj, defaulted, cause, detail := projectEventTo(e, target)
	if cause != market.CauseNone {
		return "", nil, cause, detail
	}
	canon := make(map[string]any, len(target.Fields))
	for _, f := range target.Fields {
		v, _ := fieldValue(proj, f.Name)
		if f.Type == market.TypeFloat64 {
			// Normalize int64 and float64 to the same numeric representation;
			// JSON then renders equal magnitudes identically.
			switch n := v.(type) {
			case int64:
				v = float64(n)
			case float64:
				v = n
			}
		}
		canon[f.Name] = v
	}
	b, _ := json.Marshal(canon)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:16], defaulted, market.CauseNone, ""
}

// SemanticFingerprint canonicalizes a validated event against the registry's
// newest schema. It is the basis of cross-version dedup.
func (r *Registry) SemanticFingerprint(e market.Event) (string, []string, bool) {
	r.mu.RLock()
	latest, ok := r.schemas[r.latestVersionLocked()]
	r.mu.RUnlock()
	if !ok {
		return "", nil, false
	}
	fp, defaulted, cause, _ := semanticFP(e, latest)
	if cause != market.CauseNone {
		return "", nil, false
	}
	return fp, defaulted, true
}

func isCoreField(name string) bool {
	switch name {
	case FieldEventID, FieldSymbol, FieldEventTime, FieldPrice, FieldQuantity, FieldIsTrade:
		return true
	}
	return false
}

func sortedKeys(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
