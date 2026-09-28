package pipeline

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

func logTrial(t *testing.T, rep *TrialReport) {
	t.Helper()
	t.Logf("[trial] v%d->v%d compatible=%v reason=%q field=%q detail=%s",
		rep.FromVer, rep.ToVer, rep.Compatible, rep.Reason, rep.Field, rep.Detail)
	for _, it := range rep.Items {
		t.Logf("[trial]   #%d event_id=%-8s ver=%d event_time=%d outcome=%-22s reason=%q defaulted=%v detail=%s",
			it.Index, it.EventID, it.SchemaVer, it.EventTime, it.Outcome,
			string(it.Reason), it.Defaulted, it.Detail)
	}
	t.Logf("[trial] summary=%+v", rep.Summary)
}

// TestDefaultFilling verifies old events are not treated as structurally
// incomplete after a new version adds defaulted fields, and that rebuilding an
// old event under the new version fills stable, replayable defaults.
func TestDefaultFilling(t *testing.T) {
	reg := NewRegistry()
	if _, err := reg.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.CommitUpgrade(v2GraySchema()); err != nil {
		t.Fatal(err)
	}
	old := v1Event("d1", 1, 10, 2)
	if cause, detail := reg.Validate(old); cause != market.CauseNone {
		t.Fatalf("old v1 event must validate after gray v2: %s %s", cause, detail)
	}

	// Rebuild under v2: venue and fee_bps are filled from declared defaults.
	rebuilt, defaulted, cause, detail := ProjectEvent(old, v2GraySchema())
	if cause != market.CauseNone {
		t.Fatalf("projection must succeed: %s %s", cause, detail)
	}
	if !reflect.DeepEqual(defaulted, []string{"fee_bps", "venue"}) {
		t.Fatalf("defaulted fields = %v", defaulted)
	}
	if rebuilt.Fields["venue"] != "UNKNOWN" || rebuilt.Fields["fee_bps"] != int64(0) {
		t.Fatalf("defaults not applied: %+v", rebuilt.Fields)
	}
	// The input must not be mutated and repeat projection is identical.
	if old.Fields != nil {
		t.Fatal("ProjectEvent must not mutate the source event")
	}
	again, _, _, _ := ProjectEvent(old, v2GraySchema())
	if !reflect.DeepEqual(again.Fields, rebuilt.Fields) {
		t.Fatal("repeated projection must be stable")
	}
	t.Logf("[default] rebuilt old event under v2: filled=%v values=%v", defaulted, rebuilt.Fields)
}

// TestUpgradeWithoutSafeDefaultRejected ensures a gray upgrade that adds a
// field WITHOUT a default is refused before it takes effect, with a cause
// distinct from other structural incompatibilities; the registry stays on the
// old version.
func TestUpgradeWithoutSafeDefaultRejected(t *testing.T) {
	reg := NewRegistry()
	if _, err := reg.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	unsafe := v2SchemaAddVenue() // adds venue, no default
	rep, err := reg.CommitUpgrade(unsafe)
	if err == nil || rep.OK || rep.Reason != market.CauseSchemaAddedFieldNoDefault || rep.Field != "venue" {
		t.Fatalf("field without default must be refused pre-upgrade: rep=%+v err=%v", rep, err)
	}
	t.Logf("[upgrade] no-default addition refused reason=%s field=%s detail=%s",
		rep.Reason, rep.Field, rep.Detail)
	if got := reg.Versions(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("registry must stay on v1 after a refused upgrade: %v", got)
	}
	if re, ok := AsRejected(err); !ok || re.Cause != market.CauseSchemaAddedFieldNoDefault {
		t.Fatalf("error cause mismatch: %v", err)
	}

	// A default whose type does not match the field is a distinct cause.
	broken := Schema{Version: 2, Fields: append(append([]FieldSpec{}, CoreSchemaV1.Fields...),
		FieldSpec{Name: "venue", Type: market.TypeString, HasDefault: true, Default: 123})}
	rep2, err := reg.CommitUpgrade(broken)
	if err == nil || rep2.Reason != market.CauseSchemaDefaultTypeMismatch {
		t.Fatalf("ill-typed default must be refused: rep=%+v err=%v", rep2, err)
	}
	t.Logf("[upgrade] ill-typed default refused reason=%s field=%s", rep2.Reason, rep2.Field)

	// Structural incompatibilities (e.g. field removal) stay their own cause.
	removed := Schema{Version: 2, Fields: CoreSchemaV1.Fields[:5]}
	rep3, err := reg.CommitUpgrade(removed)
	if err == nil || rep3.Reason != market.CauseSchemaRemovingField {
		t.Fatalf("removal must keep its own cause: rep=%+v err=%v", rep3, err)
	}
	t.Logf("[upgrade] removal still reason=%s distinct from no-default", rep3.Reason)
}

// TestTrialPlanIncompatible: an illegal evolution makes the whole plan
// incompatible; items are marked skipped, not "data rejected".
func TestTrialPlanIncompatible(t *testing.T) {
	reg := NewRegistry()
	if _, err := reg.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	planner := NewUpgradePlanner(reg)
	batch := []market.Event{v1Event("t1", 1, 1, 1), v1Event("t2", 2, 1, 1)}

	bad := v2SchemaAddVenue() // new field without default -> plan incompatible
	rep, err := planner.TrialUpgrade(bad, batch)
	if err != nil {
		t.Fatal(err)
	}
	logTrial(t, rep)
	if rep.Compatible || rep.Reason != market.CauseSchemaAddedFieldNoDefault {
		t.Fatalf("plan must be incompatible: %+v", rep)
	}
	if rep.Summary.PlanCompatible || rep.Summary.DataRejected != 0 {
		t.Fatalf("no per-data rejection when the plan is incompatible: %+v", rep.Summary)
	}
	for _, it := range rep.Items {
		if it.Outcome != TrialSkippedIncompatible {
			t.Fatalf("item must be skipped, got %+v", it)
		}
	}

	// Dry run is read-only: nothing got registered.
	if vs := reg.Versions(); len(vs) != 1 {
		t.Fatalf("trial must not mutate the registry: %v", vs)
	}
}

// TestTrialDataFailuresDistinctFromPlan: a safe plan with some historical
// data that cannot satisfy the candidate schema reports per-data rejections,
// while the plan itself is compatible. Repeated trials are identical.
func TestTrialDataFailuresDistinctFromPlan(t *testing.T) {
	reg := NewRegistry()
	if _, err := reg.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	planner := NewUpgradePlanner(reg)
	candidate := v2GraySchema()

	batch := []market.Event{
		v1Event("ok1", 1, 1, 1),                // old event: defaults fill venue/fee -> accepted
		v2EventWith("ok2", 2, 1, 1, "NYSE", 3), // complete new event -> accepted
		func() market.Event {
			e := v2EventWith("bad-type", 3, 1, 1, "NYSE", 3)
			e.Fields["venue"] = 42 // wrong type
			return e
		}(),
	}
	rep1, err := planner.TrialUpgrade(candidate, batch)
	if err != nil {
		t.Fatal(err)
	}
	logTrial(t, rep1)
	if !rep1.Compatible {
		t.Fatalf("plan with safe defaults must be compatible: %s", rep1.Reason)
	}
	if rep1.Summary.Accepted != 2 || rep1.Summary.DataRejected != 1 {
		t.Fatalf("summary = %+v", rep1.Summary)
	}
	if rep1.Items[2].Outcome != TrialDataRejected ||
		rep1.Items[2].Reason != market.CauseSchemaFieldTypeMismatch {
		t.Fatalf("per-data failure must be distinguishable: %+v", rep1.Items[2])
	}
	if rep1.Items[0].Outcome != TrialAccepted ||
		!reflect.DeepEqual(rep1.Items[0].Defaulted, []string{"fee_bps", "venue"}) {
		t.Fatalf("old event should be accepted via defaults: %+v", rep1.Items[0])
	}
	if vs := reg.Versions(); len(vs) != 1 {
		t.Fatalf("trial must remain read-only: %v", vs)
	}

	// Same inputs -> identical report (deterministic replay).
	rep2, _ := planner.TrialUpgrade(candidate, batch)
	if fmt.Sprint(rep1) != fmt.Sprint(rep2) {
		t.Fatalf("repeated dry run must be identical:\n%+v\n%+v", rep1, rep2)
	}
	t.Log("[trial] repeated dry run reproduced identical report")
}

// TestTrialCommitConsistency: after a green dry run, enabling the upgrade and
// submitting the SAME batch yields the same per-event structural conclusion
// (accepted vs rejected and cause).
func TestTrialCommitConsistency(t *testing.T) {
	build := func() (*Registry, *Aggregator) {
		reg := NewRegistry()
		if _, err := reg.Register(v1Schema()); err != nil {
			t.Fatal(err)
		}
		return reg, New(reg, WithWindowSize(10))
	}
	candidate := v2GraySchema()
	batch := []market.Event{
		v1Event("ok1", 1, 1, 1),
		v2EventWith("ok2", 2, 1, 1, "NYSE", 3),
		func() market.Event {
			e := v2EventWith("bad", 3, 1, 1, "NYSE", 3)
			e.Fields["venue"] = 9 // wrong type
			return e
		}(),
	}

	_, agg1 := build()
	rep, err := NewOnlineUpgradePlanner(agg1).TrialUpgrade(candidate, batch)
	if err != nil || !rep.Compatible {
		t.Fatalf("trial should be plan-compatible: %+v err=%v", rep, err)
	}
	logTrial(t, rep)

	// Commit on an independent aggregator with the SAME starting state and
	// process the batch online; item-by-item conclusions must coincide.
	reg2, agg2 := build()
	if c, err := reg2.CommitUpgrade(candidate); err != nil || !c.OK {
		t.Fatalf("green trial must enable cleanly: %+v err=%v", c, err)
	}
	for i, e := range batch {
		d, err := agg2.Submit(e)
		item := rep.Items[i]
		logGrayDecision(t, "commit", d)
		if item.OK != d.Accepted {
			t.Fatalf("event %s: trial ok=%v but online accepted=%v err=%v",
				e.EventID, item.OK, d.Accepted, err)
		}
		if !item.OK && item.Reason != d.Reason {
			t.Fatalf("event %s: trial reason %s != online reason %s",
				e.EventID, item.Reason, d.Reason)
		}
		if item.Outcome != TrialDataRejected && item.Outcome != TrialAccepted {
			t.Fatalf("event %s: unexpected trial outcome %s", e.EventID, item.Outcome)
		}
	}
}
