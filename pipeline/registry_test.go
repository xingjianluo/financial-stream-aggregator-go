package pipeline

import (
	"testing"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// TestRegistryEvolution covers every compatibility rule: additive fields are
// allowed, int64->float64 widening is allowed, while removal/rename, type
// narrowing, duplicate re-registration, gaps and regressions are rejected with
// distinguishable causes.
func TestRegistryEvolution(t *testing.T) {
	// v1 -> v2 adds an optional field: compatible.
	// v2 -> v3 widens price int64 -> float64: compatible.
	// v3 -> v4 removes a field: rejected.
	// v4' narrows float64 -> int64: rejected.
	r := NewRegistry()

	if rep, err := r.Register(v1Schema()); err != nil || !rep.OK {
		t.Fatalf("register v1: rep=%+v err=%v", rep, err)
	}

	v2 := v2SchemaAddVenue()
	if rep, err := r.Register(v2); err != nil || !rep.OK || rep.To != 2 {
		t.Fatalf("register v2 (add venue): rep=%+v err=%v", rep, err)
	}
	t.Logf("[schema] v1->v2 add field venue: ok")

	v3 := Schema{Version: 3, Fields: []FieldSpec{
		{FieldEventID, market.TypeString},
		{FieldSymbol, market.TypeString},
		{FieldEventTime, market.TypeInt64},
		{FieldPrice, market.TypeFloat64}, // widened
		{FieldQuantity, market.TypeInt64},
		{FieldIsTrade, market.TypeBool},
		{"venue", market.TypeString},
	}}
	if rep, err := r.Register(v3); err != nil || !rep.OK {
		t.Fatalf("register v3 (widen price): rep=%+v err=%v", rep, err)
	}
	t.Logf("[schema] v2->v3 widen price int64->float64: ok")

	// Remove a field.
	v4Removed := Schema{Version: 4, Fields: []FieldSpec{
		{FieldEventID, market.TypeString},
		{FieldSymbol, market.TypeString},
		{FieldEventTime, market.TypeInt64},
		{FieldPrice, market.TypeFloat64},
		{FieldQuantity, market.TypeInt64},
		{FieldIsTrade, market.TypeBool},
		// venue dropped
	}}
	rep, err := r.Register(v4Removed)
	if err == nil || rep.OK || rep.Reason != market.CauseSchemaRemovingField || rep.Field != "venue" {
		t.Fatalf("removing field must be CauseSchemaRemovingField: rep=%+v err=%v", rep, err)
	}
	t.Logf("[schema] v3->v4 removing venue rejected reason=%s field=%s", rep.Reason, rep.Field)

	// Narrow a type (float64 -> int64).
	v4Narrow := Schema{Version: 4, Fields: []FieldSpec{
		{FieldEventID, market.TypeString},
		{FieldSymbol, market.TypeString},
		{FieldEventTime, market.TypeInt64},
		{FieldPrice, market.TypeInt64}, // narrowed
		{FieldQuantity, market.TypeInt64},
		{FieldIsTrade, market.TypeBool},
		{"venue", market.TypeString},
	}}
	rep, err = r.Register(v4Narrow)
	if err == nil || rep.OK || rep.Reason != market.CauseSchemaTypeIncompatible || rep.Field != "price" {
		t.Fatalf("narrowing must be CauseSchemaTypeIncompatible: rep=%+v err=%v", rep, err)
	}
	t.Logf("[schema] v3->v4 narrowing price float64->int64 rejected reason=%s field=%s %s->%s",
		rep.Reason, rep.Field, rep.FromType, rep.ToType)

	// Cross-kind change (string -> int64 on symbol).
	v4Cross := Schema{Version: 4, Fields: []FieldSpec{
		{FieldEventID, market.TypeString},
		{FieldSymbol, market.TypeInt64},
		{FieldEventTime, market.TypeInt64},
		{FieldPrice, market.TypeFloat64},
		{FieldQuantity, market.TypeInt64},
		{FieldIsTrade, market.TypeBool},
		{"venue", market.TypeString},
	}}
	rep, err = r.Register(v4Cross)
	if err == nil || rep.Reason != market.CauseSchemaTypeIncompatible {
		t.Fatalf("cross-kind change must be incompatible: rep=%+v err=%v", rep, err)
	}
	t.Logf("[schema] symbol string->int64 rejected reason=%s", rep.Reason)

	// Version gap and regression.
	rep, err = r.Register(Schema{Version: 9, Fields: v3.Fields})
	if err == nil || rep.Reason != market.CauseSchemaVersionGap {
		t.Fatalf("version gap must be rejected: rep=%+v", rep)
	}
	t.Logf("[schema] jumping v3->v9 rejected reason=%s detail=%s", rep.Reason, rep.Detail)

	// A brand-new version number below the latest one is a version regression.
	rep, err = r.Register(Schema{Version: 0, Fields: v3.Fields})
	if err == nil || rep.Reason != market.CauseSchemaVersionRegressed {
		t.Fatalf("lower new version must be CauseSchemaVersionRegressed: rep=%+v", rep)
	}
	t.Logf("[schema] registering new v0 after v3 rejected reason=%s detail=%s", rep.Reason, rep.Detail)

	// Replaying an identical historical registration is idempotent (this is
	// what makes registration-history replay conclude like the first run).
	if rep, err := r.Register(v1Schema()); err != nil || !rep.OK {
		t.Fatalf("identical replay of v1 must be idempotent: rep=%+v err=%v", rep, err)
	}
	t.Log("[schema] identical replay of v1: idempotent ok")

	v2Mutated := Schema{Version: 2, Fields: []FieldSpec{
		{FieldEventID, market.TypeString},
		{FieldSymbol, market.TypeString},
		{FieldEventTime, market.TypeInt64},
		{FieldPrice, market.TypeInt64},
		{FieldQuantity, market.TypeInt64},
		{FieldIsTrade, market.TypeBool},
		{"venue", market.TypeBool}, // different type
	}}
	rep, err = r.Register(v2Mutated)
	if err == nil || rep.Reason != market.CauseSchemaDuplicateVersion {
		t.Fatalf("mutated same-version schema must be rejected: rep=%+v", rep)
	}
	t.Logf("[schema] conflicting v2 body rejected reason=%s", rep.Reason)
}

// TestRegistryValidate checks per-event validation, including the guarantee
// that events are validated against their OWN declared version.
func TestRegistryValidate(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Register(v2SchemaAddVenue()); err != nil {
		t.Fatal(err)
	}

	base := market.Event{EventID: "e1", Symbol: "BTC", EventTime: 100,
		SchemaVer: 1, Price: 10, Quantity: 2, IsTrade: true}
	if cause, detail := r.Validate(base); cause != market.CauseNone {
		t.Fatalf("valid v1 event rejected: %s %s", cause, detail)
	}

	// Unknown schema version.
	bad := base
	bad.SchemaVer = 99
	if cause, _ := r.Validate(bad); cause != market.CauseSchemaUnknownVersion {
		t.Fatalf("want CauseSchemaUnknownVersion, got %s", cause)
	}

	// v2 requires the new field; missing it is rejected.
	bad = base
	bad.SchemaVer = 2
	if cause, _ := r.Validate(bad); cause != market.CauseSchemaMissingField {
		t.Fatalf("want CauseSchemaMissingField, got %s", cause)
	}

	// Wrong type for the new field.
	bad.Fields = map[string]any{"venue": 123}
	if cause, _ := r.Validate(bad); cause != market.CauseSchemaFieldTypeMismatch {
		t.Fatalf("want CauseSchemaFieldTypeMismatch, got %s", cause)
	}

	// Field not declared by the schema is rejected (forward-incompatible).
	bad = base
	bad.Fields = map[string]any{"unexpected": "x"}
	if cause, _ := r.Validate(bad); cause != market.CauseSchemaExtraField {
		t.Fatalf("want CauseSchemaExtraField, got %s", cause)
	}

	// Old events validated by the old schema after v2 exists: replay stability.
	if cause, _ := r.Validate(base); cause != market.CauseNone {
		t.Fatalf("historical v1 event must still validate after v2: %s", cause)
	}
	t.Log("[schema] historical v1 payload validates under v1 after v2 registered: replay stable")
}
