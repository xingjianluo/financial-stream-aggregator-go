package pipeline

import (
	"fmt"
	"testing"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// assertItemMatchesDecision is the core contract: a dry-run item and the later
// online Decision for the SAME event against the SAME starting state must
// agree on acceptance, reject cause, duplicate/cross-version markers, window
// and the watermark observed at decision time.
func assertItemMatchesDecision(t *testing.T, it TrialItem, d market.Decision) {
	t.Helper()
	if it.OK != d.Accepted {
		t.Fatalf("#%d %s: trial OK=%v but online accepted=%v",
			it.Index, it.EventID, it.OK, d.Accepted)
	}
	if it.Reason != d.Reason {
		t.Fatalf("#%d %s: trial reason=%q but online reason=%q",
			it.Index, it.EventID, it.Reason, d.Reason)
	}
	if it.Duplicate != d.Duplicate {
		t.Fatalf("#%d %s: trial duplicate=%v but online duplicate=%v",
			it.Index, it.EventID, it.Duplicate, d.Duplicate)
	}
	if it.CrossVersion != d.CrossVersion {
		t.Fatalf("#%d %s: trial cross=%v but online cross=%v",
			it.Index, it.EventID, it.CrossVersion, d.CrossVersion)
	}
	if it.MatchedVer != d.MatchedVer {
		t.Fatalf("#%d %s: trial matched_ver=%d but online=%d",
			it.Index, it.EventID, it.MatchedVer, d.MatchedVer)
	}
	if d.Window == nil {
		t.Fatalf("#%d %s: online decision must carry a window", it.Index, it.EventID)
	}
	if it.Window != *d.Window {
		t.Fatalf("#%d %s: trial window=%v but online=%v",
			it.Index, it.EventID, it.Window, *d.Window)
	}
	if it.Watermark != d.Watermark {
		t.Fatalf("#%d %s: trial watermark=%d but online=%d",
			it.Index, it.EventID, it.Watermark, d.Watermark)
	}
	want := TrialDataRejected
	switch {
	case !d.Accepted:
		want = TrialDataRejected
	case d.Duplicate:
		want = TrialDuplicate
	default:
		want = TrialAccepted
	}
	if it.Outcome != want {
		t.Fatalf("#%d %s: trial outcome=%s but online implies %s",
			it.Index, it.EventID, it.Outcome, want)
	}
}

// sealedScenario builds the reported-bug setup on v1 with [0,10) already
// sealed: the watermark was advanced to 10 AFTER "seen1" was counted, so
// history/replay events landing in that window must be judged window_sealed.
func sealedScenario(t *testing.T, opts ...Option) (*Registry, *Aggregator) {
	t.Helper()
	reg := NewRegistry()
	if _, err := reg.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	a := New(reg, append([]Option{WithWindowSize(10)}, opts...)...)
	if _, err := a.Submit(v1Event("seen1", 1, 100, 2)); err != nil {
		t.Fatal(err)
	}
	if _, sealed := a.AdvanceWatermark(10); len(sealed) != 1 {
		t.Fatalf("setup must seal exactly one window, got %d", len(sealed))
	}
	return reg, a
}

// TestTrialOnlineSealedWindow: the dry run must consult the live watermark and
// sealed windows, and its per-item verdicts must match online processing of
// the same batch after the candidate is committed.
func TestTrialOnlineSealedWindow(t *testing.T) {
	candidate := v2GraySchema()
	batch := []market.Event{
		v1Event("fresh-sealed", 2, 10, 1),             // new event landing in sealed window
		v1Event("seen1", 1, 100, 2),                   // exact replay of a seen event
		v2EventWith("seen1", 1, 100, 2, "UNKNOWN", 0), // cross-version equivalent redelivery
		v2EventWith("seen1", 1, 999, 2, "UNKNOWN", 0), // real cross-version conflict
		v2EventWith("seen1", 1, 998, 2, "NYSE", 0),    // differs also in venue
		v1Event("fresh-sealed", 2, 10, 1),             // replay of the rejected first delivery
		func() market.Event { // unknown schema version
			e := v1Event("bad-ver", 3, 10, 1)
			e.SchemaVer = 42
			return e
		}(),
		v1Event("on-time", 15, 10, 1), // open window, on time
	}
	// Trial against the live (sealed) state.
	_, aggTrial := sealedScenario(t)
	planner := NewOnlineUpgradePlanner(aggTrial)
	rep, err := planner.TrialUpgrade(candidate, batch)
	if err != nil || !rep.Compatible {
		t.Fatalf("trial: %+v err=%v", rep, err)
	}
	logTrial(t, rep)
	if !rep.OnlineBound || rep.OnlineWatermark != 10 || rep.OnlineSeen != 1 {
		t.Fatalf("report must record the online state: %+v", rep)
	}

	// Expected headline outcomes; the detailed equality is checked item by item.
	wantOutcomes := []TrialOutcome{
		TrialDataRejected, // fresh-sealed: window_sealed
		TrialDuplicate,    // seen1 replay
		TrialDuplicate,    // seen1 cross-version equivalent
		TrialDataRejected, // cross_version_conflict (also marked duplicate)
		TrialDataRejected, // cross_version_conflict (venue)
		TrialDataRejected, // replay of the rejected delivery -> rejected again
		TrialDataRejected, // schema_unknown_version
		TrialAccepted,     // on time into [10,20)
	}
	for i, it := range rep.Items {
		if it.Outcome != wantOutcomes[i] {
			t.Fatalf("#%d %s outcome=%s want %s", i, it.EventID, it.Outcome, wantOutcomes[i])
		}
	}
	if rep.Items[0].Reason != market.CauseWindowSealed {
		t.Fatalf("sealed-window history must be window_sealed, got %q", rep.Items[0].Reason)
	}
	if !rep.Items[1].Duplicate || !rep.Items[1].OK || rep.Items[1].Reason != "" {
		t.Fatalf("replay must repeat the first acceptance: %+v", rep.Items[1])
	}
	if !rep.Items[2].CrossVersion || rep.Items[2].MatchedVer != 1 {
		t.Fatalf("equivalent cross-version redelivery must dedup: %+v", rep.Items[2])
	}
	if rep.Items[3].Reason != market.CauseCrossVersionConflict ||
		rep.Items[4].Reason != market.CauseCrossVersionConflict {
		t.Fatalf("real differences must be cross_version_conflict: %+v %+v",
			rep.Items[3], rep.Items[4])
	}
	if it := rep.Items[5]; it.Reason != market.CauseWindowSealed || it.OK || !it.Duplicate {
		t.Fatalf("duplicate of a rejected delivery replays the rejection (dup marker kept): %+v", it)
	}
	if rep.Items[6].Reason != market.CauseSchemaUnknownVersion {
		t.Fatalf("unknown version: %+v", rep.Items[6])
	}
	if s := rep.Summary; s.Accepted != 1 || s.Duplicated != 2 || s.DataRejected != 5 {
		t.Fatalf("summary = %+v", s)
	}

	// Online run on an independent aggregator with the SAME starting state:
	// commit the candidate, then process the exact same batch.
	regRun, aggRun := sealedScenario(t)
	if c, err := regRun.CommitUpgrade(candidate); err != nil || !c.OK {
		t.Fatalf("commit: %+v %v", c, err)
	}
	for i, e := range batch {
		d, _ := aggRun.Submit(e)
		logGrayDecision(t, fmt.Sprintf("online #%d", i), d)
		assertItemMatchesDecision(t, rep.Items[i], d)
	}

	// Sealed stats stay exactly as before the batch: only seen1 was counted.
	ws := aggRun.SealedWindows()
	if len(ws) != 1 || ws[0].EventCount != 1 || ws[0].Volume != 2 {
		t.Fatalf("sealed window immutable: %+v", ws)
	}
}

// runTrialThenOnline is the shared verification harness: trial the batch
// against the live aggregator, then replay it on an identically built runner
// after committing the candidate, asserting item-by-item equality. It returns
// the trial report and the online aggregator for further state assertions.
func runTrialThenOnline(t *testing.T, build func() (*Registry, *Aggregator),
	candidate Schema, batch []market.Event) (*TrialReport, *Aggregator) {
	t.Helper()

	_, aggT := build()
	rep, err := NewOnlineUpgradePlanner(aggT).TrialUpgrade(candidate, batch)
	if err != nil {
		t.Fatalf("trial error: %v", err)
	}
	logTrial(t, rep)

	regR, aggR := build()
	if c, err := regR.CommitUpgrade(candidate); err != nil || !c.OK {
		t.Fatalf("commit after green trial: %+v %v", c, err)
	}
	for i, e := range batch {
		d, _ := aggR.Submit(e)
		logGrayDecision(t, fmt.Sprintf("online #%d", i), d)
		assertItemMatchesDecision(t, rep.Items[i], d)
	}
	return rep, aggR
}

// TestTrialOnlineLateClassification covers every late-event branch against a
// live watermark whose window is still open: beyond allowed lateness
// (late_dropped), within grace under the default policy (accepted + side
// output), and within grace under PolicyReject (late_dropped).
func TestTrialOnlineLateClassification(t *testing.T) {
	candidate := v2GraySchema()

	t.Run("drop_side_output_policy", func(t *testing.T) {
		build := func() (*Registry, *Aggregator) {
			reg := NewRegistry()
			if _, err := reg.Register(v1Schema()); err != nil {
				t.Fatal(err)
			}
			// [0,100) stays open at wm=50; grace 20.
			a := New(reg, WithWindowSize(100), WithAllowedLateness(20),
				WithLatePolicy(market.PolicyDrop))
			if _, _ = a.AdvanceWatermark(50); a.Watermark() != 50 {
				t.Fatal("setup watermark")
			}
			return reg, a
		}
		batch := []market.Event{
			v1Event("too-late", 10, 1, 1), // 40 behind wm > grace 20 -> late_dropped
			v1Event("in-grace", 35, 1, 1), // 15 behind wm <= grace -> counted, side-output
			v1Event("in-grace", 35, 1, 1), // replay -> duplicate, not counted again
			v1Event("on-time", 60, 1, 1),  // on time -> accepted
		}
		rep, agg := runTrialThenOnline(t, build, candidate, batch)
		if rep.Items[0].Reason != market.CauseLateDropped {
			t.Fatalf("beyond grace must be late_dropped: %+v", rep.Items[0])
		}
		if !rep.Items[1].OK || rep.Items[1].Outcome != TrialAccepted {
			t.Fatalf("in-grace under drop policy must count: %+v", rep.Items[1])
		}
		if rep.Items[2].Outcome != TrialDuplicate || rep.Items[2].Reason != "" {
			t.Fatalf("replay of the counted late event duplicates acceptance: %+v", rep.Items[2])
		}
		if s := rep.Summary; s.Accepted != 2 || s.Duplicated != 1 || s.DataRejected != 1 {
			t.Fatalf("summary=%+v", s)
		}
		// Exactly one late-but-counted event must reach the side output.
		if side := agg.SideOutput(); len(side) != 1 || side[0].EventID != "in-grace" {
			t.Fatalf("side output=%+v", side)
		}
	})

	t.Run("reject_policy", func(t *testing.T) {
		build := func() (*Registry, *Aggregator) {
			reg := NewRegistry()
			if _, err := reg.Register(v1Schema()); err != nil {
				t.Fatal(err)
			}
			a := New(reg, WithWindowSize(100), WithAllowedLateness(1000),
				WithLatePolicy(market.PolicyReject))
			a.AdvanceWatermark(50)
			return reg, a
		}
		batch := []market.Event{
			v1Event("rej", 40, 1, 1), // late within grace but policy=reject
			v1Event("rej", 40, 1, 1), // replay: same rejection again
		}
		rep, _ := runTrialThenOnline(t, build, candidate, batch)
		if rep.Items[0].Reason != market.CauseLateDropped || rep.Items[0].OK {
			t.Fatalf("PolicyReject: %+v", rep.Items[0])
		}
		if rep.Items[1].Outcome != TrialDataRejected ||
			rep.Items[1].Reason != market.CauseLateDropped {
			t.Fatalf("replay under reject policy repeats late_dropped: %+v", rep.Items[1])
		}
	})
}

// TestTrialOnlineCrossVersionBatch: with only v1 online history, a replay
// batch carrying the same logical events under v2 must, item by item, separate
// first-count, equivalent cross-version duplicate (not counted) and real
// cross-version conflict (rejected) — identically in trial and online.
func TestTrialOnlineCrossVersionBatch(t *testing.T) {
	candidate := v2GraySchema()
	build := func() (*Registry, *Aggregator) {
		reg := NewRegistry()
		if _, err := reg.Register(v1Schema()); err != nil {
			t.Fatal(err)
		}
		// One old-producer event already counted online; window stays open.
		a := New(reg, WithWindowSize(10))
		if _, err := a.Submit(v1Event("old1", 1, 100, 5)); err != nil {
			t.Fatal(err)
		}
		return reg, a
	}
	batch := []market.Event{
		v2EventWith("old1", 1, 100, 5, "UNKNOWN", 0), // cross-version dup of online event
		v2EventWith("new1", 2, 10, 1, "UNKNOWN", 0),  // first delivery, new producer (defaults)
		v1Event("new1", 2, 10, 1),                    // old producer redelivers -> cross dup
		v2EventWith("new2", 3, 10, 1, "UNKNOWN", 0),  // first v2 delivery
		v2EventWith("new2", 3, 10, 1, "NYSE", 0),     // same version+id, real venue conflict
		v2EventWith("old1", 1, 500, 5, "UNKNOWN", 0), // price conflict vs online old1
	}
	rep, agg := runTrialThenOnline(t, build, candidate, batch)

	if it := rep.Items[0]; it.Outcome != TrialDuplicate || !it.CrossVersion ||
		it.MatchedVer != 1 || !it.OK {
		t.Fatalf("online old1 redelivered under v2 is a cross-version duplicate: %+v", it)
	}
	if rep.Items[1].Outcome != TrialAccepted {
		t.Fatalf("new1 first delivery must count: %+v", rep.Items[1])
	}
	if it := rep.Items[2]; it.Outcome != TrialDuplicate || !it.CrossVersion ||
		it.MatchedVer != 2 {
		t.Fatalf("old producer after v2 first is cross dup matched to v2: %+v", it)
	}
	if rep.Items[3].Outcome != TrialAccepted {
		t.Fatalf("new2 first delivery must count: %+v", rep.Items[3])
	}
	if it := rep.Items[4]; it.Outcome != TrialDataRejected || it.Reason != market.CauseConflict {
		t.Fatalf("same-id/version different venue must be conflict: %+v", it)
	}
	if it := rep.Items[5]; it.Outcome != TrialDataRejected ||
		it.Reason != market.CauseCrossVersionConflict {
		t.Fatalf("price difference vs online history must cross-conflict: %+v", it)
	}
	if s := rep.Summary; s.Accepted != 2 || s.Duplicated != 2 || s.DataRejected != 2 {
		t.Fatalf("summary=%+v", s)
	}
	// old1 (pre-online) + new1 + new2 = 3 counted logical events; duplicates
	// and conflicts never add a count.
	_, sealed := agg.AdvanceWatermark(10)
	if len(sealed) != 1 || sealed[0].EventCount != 3 || sealed[0].Volume != 5+1+1 {
		t.Fatalf("counted logical events mismatch: %+v", sealed)
	}
}

// TestTrialOnlineWideningEquivalence: int64 -> float64 widening with equal
// magnitude is an equivalent cross-version redelivery; a changed magnitude is
// a real conflict. Trial and online agree on each.
func TestTrialOnlineWideningEquivalence(t *testing.T) {
	candidate := v3WidenSchema()
	build := func() (*Registry, *Aggregator) {
		reg := NewRegistry()
		if _, err := reg.Register(v1Schema()); err != nil {
			t.Fatal(err)
		}
		if _, err := reg.CommitUpgrade(v2GraySchema()); err != nil {
			t.Fatal(err)
		}
		a := New(reg, WithWindowSize(10))
		if _, err := a.Submit(v1Event("w1", 1, 100, 5)); err != nil {
			t.Fatal(err)
		}
		return reg, a
	}
	widened := market.Event{EventID: "w1", Symbol: "BTC", EventTime: 1, SchemaVer: 3,
		Price: 100, IsTrade: true,
		Fields: map[string]any{"quantity": float64(5), "venue": "UNKNOWN", "fee_bps": int64(0)}}
	changed := widened
	changed.Fields = map[string]any{"quantity": float64(5.5), "venue": "UNKNOWN", "fee_bps": int64(0)}
	batch := []market.Event{widened, changed}

	rep, _ := runTrialThenOnline(t, build, candidate, batch)
	if it := rep.Items[0]; it.Outcome != TrialDuplicate || !it.CrossVersion {
		t.Fatalf("equal-magnitude widening must dedup: %+v", it)
	}
	if it := rep.Items[1]; it.Outcome != TrialDataRejected ||
		it.Reason != market.CauseCrossVersionConflict {
		t.Fatalf("changed magnitude must conflict: %+v", it)
	}
}

// TestTrialReadOnlyAndReplayable: running a trial (including one whose batch
// contains sealed, late, duplicate and conflicting events) must not change the
// watermark, sealed windows, seen-event count or side output; repeating it
// against the same state yields an identical report.
func TestTrialReadOnlyAndReplayable(t *testing.T) {
	candidate := v2GraySchema()
	batch := []market.Event{
		v1Event("seen1", 1, 100, 2),                   // duplicate of sealed online event
		v2EventWith("seen1", 1, 100, 2, "UNKNOWN", 0), // cross-version dup
		v1Event("x", 2, 10, 1),                        // sealed-window rejection
		v2EventWith("seen1", 1, 9, 2, "UNKNOWN", 0),   // conflict
	}

	_, agg := sealedScenario(t)
	wmBefore := agg.Watermark()
	sealedBefore := agg.SealedWindows()
	sideBefore := len(agg.SideOutput())
	seenBefore := len(agg.seen)

	planner := NewOnlineUpgradePlanner(agg)
	rep1, err := planner.TrialUpgrade(candidate, batch)
	if err != nil {
		t.Fatal(err)
	}
	rep2, err := planner.TrialUpgrade(candidate, batch)
	if err != nil {
		t.Fatal(err)
	}
	logTrial(t, rep1)

	if agg.Watermark() != wmBefore {
		t.Fatalf("trial moved the watermark: %d -> %d", wmBefore, agg.Watermark())
	}
	if len(agg.seen) != seenBefore {
		t.Fatalf("trial changed seen events: %d -> %d", seenBefore, len(agg.seen))
	}
	if got := agg.SealedWindows(); fmt.Sprint(got) != fmt.Sprint(sealedBefore) {
		t.Fatalf("trial changed sealed windows:\n%+v\n%+v", got, sealedBefore)
	}
	if len(agg.SideOutput()) != sideBefore {
		t.Fatalf("trial changed side output length")
	}
	if fmt.Sprint(rep1) != fmt.Sprint(rep2) {
		t.Fatalf("repeated trial against the same state must be identical")
	}
	t.Log("[trial] read-only confirmed (watermark/sealed/seen/side unchanged); replay identical")
}

// TestTrialPlanIncompatibleStaysSkippedOnlineBound: even when bound to live
// state, a plan-level incompatibility dominates: every item is skipped, the
// plan is not confused with per-data failure, and nothing is enabled.
func TestTrialPlanIncompatibleStaysSkippedOnlineBound(t *testing.T) {
	_, agg := sealedScenario(t) // live wm=10, one sealed window
	bad := v2SchemaAddVenue()   // adds venue WITHOUT a default
	batch := []market.Event{v1Event("a", 2, 10, 1), v1Event("seen1", 1, 100, 2)}

	rep, err := NewOnlineUpgradePlanner(agg).TrialUpgrade(bad, batch)
	if err != nil {
		t.Fatal(err)
	}
	logTrial(t, rep)
	if rep.Compatible || rep.Reason != market.CauseSchemaAddedFieldNoDefault {
		t.Fatalf("plan must be incompatible: %+v", rep)
	}
	if !rep.OnlineBound || rep.OnlineWatermark != 10 {
		t.Fatalf("online snapshot still recorded: %+v", rep)
	}
	for _, it := range rep.Items {
		if it.Outcome != TrialSkippedIncompatible || it.OK {
			t.Fatalf("items must be skipped, not data-rejected: %+v", it)
		}
	}
	if rep.Summary.PlanCompatible || rep.Summary.DataRejected != 0 {
		t.Fatalf("plan failure must not be reported as data failures: %+v", rep.Summary)
	}
	// The live registry only ever had v1 and stays that way.
	if vs := agg.reg.Versions(); len(vs) != 1 || vs[0] != 1 {
		t.Fatalf("registry mutated by trial: %v", vs)
	}
	if agg.Watermark() != 10 {
		t.Fatalf("watermark changed: %d", agg.Watermark())
	}
}
