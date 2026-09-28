package pipeline

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// pairedState holds two registries/aggregators built from the SAME prelude:
// the trial side stays on the old schema (the dry run projects the candidate
// on a detached view), the live side commits the candidate right before the
// batch is processed. Identical preludes make the starting online state
// identical, which is the precondition of trial/live equivalence.
type pairedState struct {
	trialReg, liveReg *Registry
	trialAgg, liveAgg *Aggregator
	candidate         Schema
}

func newPairedState(t *testing.T, prelude func(a *Aggregator), opts ...Option) *pairedState {
	t.Helper()
	mk := func() (*Registry, *Aggregator) {
		reg := NewRegistry()
		if _, err := reg.Register(v1Schema()); err != nil {
			t.Fatal(err)
		}
		a := New(reg, opts...)
		if prelude != nil {
			prelude(a)
		}
		return reg, a
	}
	r1, a1 := mk()
	r2, a2 := mk()
	return &pairedState{trialReg: r1, trialAgg: a1, liveReg: r2, liveAgg: a2,
		candidate: v2GraySchema()}
}

// runTrialAndLive performs the dry run against p.trialAgg and, independently,
// commits the candidate on the live side and processes the same batch there.
// It returns the live Submit errors too, because an explicit rejection is
// signalled by a non-nil *RejectedError (a conflicting re-delivery carries
// Duplicate=true together with that error).
func runTrialAndLive(t *testing.T, p *pairedState, batch []market.Event,
) (*TrialReport, []market.Decision, []error) {
	t.Helper()
	rep, err := NewUpgradePlanner(p.trialReg, p.trialAgg).TrialUpgrade(p.candidate, batch)
	if err != nil {
		t.Fatalf("trial failed: %v", err)
	}
	logTrial(t, rep)
	if _, err := p.liveReg.CommitUpgrade(p.candidate); err != nil {
		t.Fatalf("live commit failed: %v", err)
	}
	var live []market.Decision
	var errs []error
	for _, e := range batch {
		d, err := p.liveAgg.Submit(e)
		live = append(live, d)
		errs = append(errs, err)
		logGrayDecision(t, "live", d)
	}
	return rep, live, errs
}

// assertTrialMatchesLive compares every trial item with the live decision for
// the same batch position field by field: accept/reject, cause, detail,
// duplicate/cross-version markers, matched version, window, watermark,
// fingerprint and default filling. This is the core "预演与正式处理逐条一致"
// assertion.
func assertTrialMatchesLive(t *testing.T, rep *TrialReport,
	live []market.Decision, errs []error) {
	t.Helper()
	if len(rep.Items) != len(live) {
		t.Fatalf("item count: trial=%d live=%d", len(rep.Items), len(live))
	}
	var nAcc, nDup, nRej int
	for i := range rep.Items {
		it, d := rep.Items[i], live[i]
		if it.EventID != d.EventID {
			t.Fatalf("#%d event id: trial=%q live=%q", i, it.EventID, d.EventID)
		}
		if it.SchemaVer != d.SchemaVer {
			t.Fatalf("#%d %s schema ver: trial=%d live=%d", i, it.EventID, it.SchemaVer, d.SchemaVer)
		}
		if it.Accepted != d.Accepted || it.Reason != d.Reason {
			t.Fatalf("#%d %s verdict: trial(accepted=%v reason=%q) vs "+
				"live(accepted=%v reason=%q)", i, it.EventID,
				it.Accepted, it.Reason, d.Accepted, d.Reason)
		}
		if it.Detail != d.Detail {
			t.Fatalf("#%d %s reject basis detail:\n trial=%q\n live =%q",
				i, it.EventID, it.Detail, d.Detail)
		}
		if it.Duplicate != d.Duplicate || it.CrossVersion != d.CrossVersion ||
			it.MatchedVer != d.MatchedVer {
			t.Fatalf("#%d %s dedup markers: trial(dup=%v cross=%v matched=%d) "+
				"vs live(dup=%v cross=%v matched=%d)", i, it.EventID,
				it.Duplicate, it.CrossVersion, it.MatchedVer,
				d.Duplicate, d.CrossVersion, d.MatchedVer)
		}
		if !reflect.DeepEqual(it.Defaulted, d.Defaulted) {
			t.Fatalf("#%d %s defaulted: trial=%v live=%v",
				i, it.EventID, it.Defaulted, d.Defaulted)
		}
		if it.ContentFP != d.ContentFP {
			t.Fatalf("#%d %s content fp: trial=%s live=%s",
				i, it.EventID, it.ContentFP, d.ContentFP)
		}
		if it.Watermark != d.Watermark {
			t.Fatalf("#%d %s watermark: trial=%d live=%d",
				i, it.EventID, it.Watermark, d.Watermark)
		}
		if (it.Window == nil) != (d.Window == nil) {
			t.Fatalf("#%d %s window presence differs", i, it.EventID)
		}
		if it.Window != nil && (it.Window.Start != d.Window.Start ||
			it.Window.End != d.Window.End) {
			t.Fatalf("#%d %s window: trial=[%d,%d) live=[%d,%d)", i, it.EventID,
				it.Window.Start, it.Window.End, d.Window.Start, d.Window.End)
		}
		wantCounted := d.Accepted && !d.Duplicate
		if it.Counted != wantCounted {
			t.Fatalf("#%d %s counted: trial=%v want=%v", i, it.EventID, it.Counted, wantCounted)
		}
		var want TrialOutcome
		switch {
		case errs[i] != nil:
			want = TrialDataRejected // explicit rejection (incl. conflicts)
		case d.Duplicate:
			want = TrialDuplicate
		default:
			want = TrialAccepted
		}
		if it.Outcome != want {
			t.Fatalf("#%d %s outcome: trial=%s want=%s", i, it.EventID, it.Outcome, want)
		}
		switch want {
		case TrialAccepted:
			nAcc++
		case TrialDuplicate:
			nDup++
		case TrialDataRejected:
			nRej++
		}
	}
	if rep.Summary.Accepted != nAcc || rep.Summary.Duplicated != nDup ||
		rep.Summary.DataRejected != nRej {
		t.Fatalf("summary %+v does not partition items (acc=%d dup=%d rej=%d)",
			rep.Summary, nAcc, nDup, nRej)
	}
}

// trialReportsEqual compares two reports by VALUE: TrialItem.Window is a
// pointer whose address changes between runs, so windows are compared by
// bounds. Returns "" when equal, otherwise the first mismatch description.
func trialReportsEqual(r1, r2 *TrialReport) string {
	if r1.FromVer != r2.FromVer || r1.ToVer != r2.ToVer ||
		r1.Compatible != r2.Compatible || r1.Reason != r2.Reason ||
		r1.Field != r2.Field || r1.Detail != r2.Detail ||
		r1.OnlineWatermark != r2.OnlineWatermark ||
		!reflect.DeepEqual(r1.Summary, r2.Summary) {
		return "report header/summary differs"
	}
	if len(r1.Items) != len(r2.Items) {
		return "item length differs"
	}
	for i := range r1.Items {
		a, b := r1.Items[i], r2.Items[i]
		a.Window, b.Window = nil, nil
		if !reflect.DeepEqual(a, b) {
			return fmt.Sprintf("item #%d differs:\n %+v\n %+v", i, r1.Items[i], r2.Items[i])
		}
		w1, w2 := r1.Items[i].Window, r2.Items[i].Window
		if (w1 == nil) != (w2 == nil) ||
			(w1 != nil && (w1.Start != w2.Start || w1.End != w2.End)) {
			return fmt.Sprintf("item #%d window differs: %+v vs %+v", i, w1, w2)
		}
	}
	return ""
}

// TestTrialSealedWindowRedelivery is the reported bug: advance the watermark
// to seal a window, then replay history into it. The trial must reject FIRST
// deliveries with window_sealed exactly like live processing, while
// re-deliveries of ids already accepted online replay the first verdict
// (acceptance, not counted) — same-content and cross-version equivalent alike.
func TestTrialSealedWindowRedelivery(t *testing.T) {
	prelude := func(a *Aggregator) {
		if _, err := a.Submit(v1Event("seen-ok", 1, 100, 2)); err != nil {
			t.Fatal(err)
		}
		if _, sealed := a.AdvanceWatermark(10); len(sealed) != 1 ||
			sealed[0].Window.Start != 0 {
			t.Fatalf("prelude must seal [0,10): %+v", sealed)
		}
	}
	p := newPairedState(t, prelude, WithWindowSize(10), WithAllowedLateness(5))

	batch := []market.Event{
		v1Event("fresh-sealed", 2, 10, 1),               // new id, sealed window -> reject
		v1Event("seen-ok", 1, 100, 2),                   // identical redelivery -> dup, replays accept
		v2EventWith("seen-ok", 1, 100, 2, "UNKNOWN", 0), // cross-version equivalent -> cross dup
		v1Event("fresh-sealed-2", 9, 11, 1),             // new id, still sealed -> reject
	}
	rep, live, errs := runTrialAndLive(t, p, batch)
	assertTrialMatchesLive(t, rep, live, errs)

	want := []struct {
		id      string
		outcome TrialOutcome
		reason  market.RejectCause
		dup     bool
		cross   bool
	}{
		{"fresh-sealed", TrialDataRejected, market.CauseWindowSealed, false, false},
		{"seen-ok", TrialDuplicate, "", true, false},
		{"seen-ok", TrialDuplicate, "", true, true},
		{"fresh-sealed-2", TrialDataRejected, market.CauseWindowSealed, false, false},
	}
	for i, w := range want {
		it := rep.Items[i]
		if it.Outcome != w.outcome || it.Reason != w.reason ||
			it.Duplicate != w.dup || it.CrossVersion != w.cross {
			t.Fatalf("#%d %s: got outcome=%s reason=%q dup=%v cross=%v, want %+v",
				i, it.EventID, it.Outcome, it.Reason, it.Duplicate, it.CrossVersion, w)
		}
		if it.Accepted != (w.reason == "") {
			t.Fatalf("#%d %s accepted=%v", i, it.EventID, it.Accepted)
		}
		if it.Counted {
			t.Fatalf("#%d %s sealed-window replay must never count", i, it.EventID)
		}
	}
	if rep.AllPassed() {
		t.Fatal("AllPassed must be false with window_sealed items")
	}
}

// TestTrialLateEvents checks late-event classification from the current
// watermark: within the allowed grace the default policy counts (and
// side-outputs), beyond the grace it is late_dropped, and with PolicyReject
// even in-grace late events are rejected — trial and live agree item by item.
func TestTrialLateEvents(t *testing.T) {
	t.Run("drop_side_output", func(t *testing.T) {
		prelude := func(a *Aggregator) {
			if d, _ := a.AdvanceWatermark(8); d.Reason != "" {
				t.Fatalf("wm: %s", d.Reason)
			}
		}
		p := newPairedState(t, prelude, WithWindowSize(10), WithAllowedLateness(5))
		batch := []market.Event{
			v1Event("late-in", 5, 10, 1),  // 3 behind wm, within grace -> counted+side
			v1Event("late-in", 5, 10, 1),  // duplicate: replays acceptance, not counted
			v1Event("late-out", 2, 10, 1), // 6 behind wm > grace -> late_dropped
			v1Event("on-time", 9, 10, 1),  // t >= wm -> on-time accept
		}
		rep, live, errs := runTrialAndLive(t, p, batch)
		assertTrialMatchesLive(t, rep, live, errs)
		if rep.Items[0].Outcome != TrialAccepted || !rep.Items[0].Counted {
			t.Fatalf("in-grace late event must count: %+v", rep.Items[0])
		}
		if rep.Items[1].Outcome != TrialDuplicate || rep.Items[1].Counted {
			t.Fatalf("its retry is a non-counting duplicate: %+v", rep.Items[1])
		}
		if rep.Items[2].Outcome != TrialDataRejected ||
			rep.Items[2].Reason != market.CauseLateDropped {
			t.Fatalf("beyond-grace event must be late_dropped: %+v", rep.Items[2])
		}
		// The shadow side output must not leak into the TRIAL (dry-run)
		// aggregator; the live aggregator has its own real copy.
		if side := p.trialAgg.SideOutput(); len(side) != 0 {
			t.Fatalf("trial side output must not leak into the dry-run agg: %+v", side)
		}
	})

	t.Run("reject_policy", func(t *testing.T) {
		prelude := func(a *Aggregator) {
			if d, _ := a.AdvanceWatermark(8); d.Reason != "" {
				t.Fatalf("wm: %s", d.Reason)
			}
		}
		p := newPairedState(t, prelude, WithWindowSize(10),
			WithAllowedLateness(5), WithLatePolicy(market.PolicyReject))
		batch := []market.Event{v1Event("late-in", 5, 10, 1)}
		rep, live, errs := runTrialAndLive(t, p, batch)
		assertTrialMatchesLive(t, rep, live, errs)
		if rep.Items[0].Reason != market.CauseLateDropped || rep.Items[0].Accepted {
			t.Fatalf("PolicyReject must refuse in-grace late events: %+v", rep.Items[0])
		}
	})
}

// TestTrialBatchDuplicates verifies that WITHIN one replay batch the three
// repeated-delivery cases are distinguished per item: first delivery counts,
// same-content and cross-version-equivalent re-deliveries do not, and real
// conflicts are explicitly rejected (same-version conflict vs cross-version
// conflict remain distinguishable).
func TestTrialBatchDuplicates(t *testing.T) {
	p := newPairedState(t, nil, WithWindowSize(10))
	batch := []market.Event{
		v1Event("same", 1, 100, 5),                     // first -> counted
		v1Event("same", 1, 100, 5),                     // identical -> duplicate
		v1Event("cross", 2, 100, 5),                    // first -> counted
		v2EventWith("cross", 2, 100, 5, "UNKNOWN", 0),  // equivalent -> cross dup, matched 1
		v2EventWith("cross2", 3, 100, 5, "UNKNOWN", 0), // first (new producer) -> counted
		v1Event("cross2", 3, 100, 5),                   // equivalent old redelivery -> cross dup, matched 2
		v1Event("conflict", 4, 100, 5),                 // first -> counted
		func() market.Event { // same-version different content -> conflict
			e := v1Event("conflict", 4, 999, 5)
			return e
		}(),
		v1Event("xconflict", 5, 100, 5),                   // first -> counted
		v2EventWith("xconflict", 5, 100, 9, "UNKNOWN", 0), // real diff -> cross_version_conflict
	}
	rep, live, errs := runTrialAndLive(t, p, batch)
	assertTrialMatchesLive(t, rep, live, errs)

	wantOut := []TrialOutcome{
		TrialAccepted, TrialDuplicate,
		TrialAccepted, TrialDuplicate,
		TrialAccepted, TrialDuplicate,
		TrialAccepted, TrialDataRejected,
		TrialAccepted, TrialDataRejected,
	}
	// CrossVersion is only set on a benign cross-version-equivalent
	// re-delivery; both kinds of conflict carry CrossVersion=false (they are
	// explicit rejections, not reconciled duplicates).
	wantCross := []bool{false, false, false, true, false, true, false, false, false, false}
	for i := range batch {
		if rep.Items[i].Outcome != wantOut[i] {
			t.Fatalf("#%d %s outcome=%s want=%s", i, rep.Items[i].EventID,
				rep.Items[i].Outcome, wantOut[i])
		}
		if rep.Items[i].CrossVersion != wantCross[i] {
			t.Fatalf("#%d %s cross=%v want=%v", i, rep.Items[i].EventID,
				rep.Items[i].CrossVersion, wantCross[i])
		}
	}
	if rep.Items[3].MatchedVer != 1 || rep.Items[5].MatchedVer != 2 {
		t.Fatalf("matched versions: #3=%d #5=%d", rep.Items[3].MatchedVer, rep.Items[5].MatchedVer)
	}
	if rep.Items[7].Reason != market.CauseConflict {
		t.Fatalf("same-version disagreement: %+v", rep.Items[7])
	}
	if rep.Items[9].Reason != market.CauseCrossVersionConflict {
		t.Fatalf("cross-version real disagreement: %+v", rep.Items[9])
	}
	// Five first deliveries count; the three equivalent repeats are
	// duplicates; the two conflicts are explicit rejections.
	if rep.Summary.Accepted != 5 || rep.Summary.Duplicated != 3 ||
		rep.Summary.DataRejected != 2 {
		t.Fatalf("summary = %+v", rep.Summary)
	}
	if rep.AllPassed() {
		t.Fatal("batch contains explicit rejections")
	}
}

// TestTrialReplayRejectedFirstVerdict: an id whose FIRST delivery was already
// rejected online (window sealed) and is redelivered in the batch must replay
// that rejection AS A DUPLICATE (same cause), not count and not flip to
// accepted — identical to live behavior.
func TestTrialReplayRejectedFirstVerdict(t *testing.T) {
	prelude := func(a *Aggregator) {
		if _, err := a.Submit(v1Event("late-seen", 1, 10, 1)); err != nil {
			t.Fatal(err)
		}
		if _, s := a.AdvanceWatermark(10); len(s) != 1 {
			t.Fatal("prelude seal failed")
		}
		// First delivery AFTER sealing: rejected and remembered.
		d, err := a.Submit(v1Event("rej-seen", 2, 10, 1))
		if err == nil || d.Reason != market.CauseWindowSealed {
			t.Fatalf("prelude: want window_sealed, got %+v", d)
		}
	}
	p := newPairedState(t, prelude, WithWindowSize(10))
	batch := []market.Event{
		v1Event("rej-seen", 2, 10, 1),                   // duplicate replaying rejection
		v2EventWith("rej-seen", 2, 10, 1, "UNKNOWN", 0), // cross-version form too
	}
	rep, live, errs := runTrialAndLive(t, p, batch)
	assertTrialMatchesLive(t, rep, live, errs)
	for i, it := range rep.Items {
		if it.Outcome != TrialDuplicate || !it.Duplicate || it.Accepted ||
			it.Reason != market.CauseWindowSealed {
			t.Fatalf("#%d rejected-first replay: %+v", i, it)
		}
		if it.Counted {
			t.Fatalf("#%d must not count", i)
		}
	}
}

// TestTrialDeterministicAndReadOnly: repeated trials against the same state
// are identical, and the trial changes no watermark, sealed window, seen-event
// ledger or side output. An id the trial "accepted" is still unknown online
// afterwards and gets counted exactly once.
func TestTrialDeterministicAndReadOnly(t *testing.T) {
	reg := NewRegistry()
	if _, err := reg.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	agg := New(reg, WithWindowSize(10), WithAllowedLateness(5))
	if _, err := agg.Submit(v1Event("seen-ok", 1, 100, 2)); err != nil {
		t.Fatal(err)
	}
	if _, s := agg.AdvanceWatermark(10); len(s) != 1 {
		t.Fatal("seal failed")
	}

	candidate := v2GraySchema()
	batch := []market.Event{
		v1Event("fresh-sealed", 2, 10, 1),
		v1Event("seen-ok", 1, 100, 2),
		v1Event("future", 15, 10, 3), // open window [10,20): accepted by the trial
	}
	wmBefore, sealedBefore := agg.Watermark(), agg.SealedWindows()
	sideBefore := agg.SideOutput()

	planner := NewUpgradePlanner(reg, agg)
	rep1, err := planner.TrialUpgrade(candidate, batch)
	if err != nil {
		t.Fatal(err)
	}
	rep2, err := planner.TrialUpgrade(candidate, batch)
	if err != nil {
		t.Fatal(err)
	}
	logTrial(t, rep1)
	if diff := trialReportsEqual(rep1, rep2); diff != "" {
		t.Fatalf("repeated trials differ: %s", diff)
	}
	if rep1.OnlineWatermark != 10 {
		t.Fatalf("online watermark not recorded: %d", rep1.OnlineWatermark)
	}

	if agg.Watermark() != wmBefore {
		t.Fatalf("trial moved the watermark: %d -> %d", wmBefore, agg.Watermark())
	}
	if !reflect.DeepEqual(agg.SealedWindows(), sealedBefore) {
		t.Fatal("trial changed sealed windows")
	}
	if !reflect.DeepEqual(agg.SideOutput(), sideBefore) {
		t.Fatal("trial changed the side output")
	}
	if vs := reg.Versions(); len(vs) != 1 || vs[0] != 1 {
		t.Fatalf("trial must not register the candidate: %v", vs)
	}

	// "future" was accepted inside the trial but must NOT be remembered online.
	d, err := agg.Submit(v1Event("future", 15, 10, 3))
	if err != nil || !d.Accepted || d.Duplicate {
		t.Fatalf("trial must not teach the online ledger the id: %+v err=%v", d, err)
	}
	if _, s := agg.AdvanceWatermark(20); len(s) != 1 || s[0].EventCount != 1 ||
		s[0].Volume != 3 {
		t.Fatalf("the real first delivery counts exactly once: %+v", s)
	}
}

// TestTrialGreenBatchAllPassed: a batch whose items are all either first
// acceptances or acceptance-replaying duplicates (in the sealed window too)
// reports AllPassed with zero data rejections.
func TestTrialGreenBatchAllPassed(t *testing.T) {
	prelude := func(a *Aggregator) {
		if _, err := a.Submit(v1Event("seen-ok", 1, 100, 2)); err != nil {
			t.Fatal(err)
		}
		if _, s := a.AdvanceWatermark(10); len(s) != 1 {
			t.Fatal("seal failed")
		}
	}
	p := newPairedState(t, prelude, WithWindowSize(10))
	batch := []market.Event{
		v1Event("new-open", 15, 10, 1),
		v1Event("seen-ok", 1, 100, 2),
		v2EventWith("seen-ok", 1, 100, 2, "UNKNOWN", 0),
	}
	rep, live, errs := runTrialAndLive(t, p, batch)
	assertTrialMatchesLive(t, rep, live, errs)
	if !rep.AllPassed() {
		t.Fatalf("green batch must pass: %+v", rep.Summary)
	}
	if rep.Summary.DataRejected != 0 || rep.Summary.Accepted != 1 ||
		rep.Summary.Duplicated != 2 {
		t.Fatalf("summary = %+v", rep.Summary)
	}
}

// TestTrialThenCommitSameInstance mirrors the real gray rollout flow on ONE
// aggregator: trial a batch against the current online state (with a sealed
// window), gate on AllPassed, CommitUpgrade, then process the SAME batch on
// the SAME instance. Item by item the post-commit verdicts must equal the
// trial verdicts. A second trial after commit stays identical (idempotent).
func TestTrialThenCommitSameInstance(t *testing.T) {
	reg := NewRegistry()
	if _, err := reg.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	agg := New(reg, WithWindowSize(10), WithAllowedLateness(5))
	// Online history: one accepted event, then [0,10) is sealed.
	if _, err := agg.Submit(v1Event("hist", 1, 100, 2)); err != nil {
		t.Fatal(err)
	}
	if _, s := agg.AdvanceWatermark(10); len(s) != 1 {
		t.Fatal("seal failed")
	}

	candidate := v2GraySchema()
	batch := []market.Event{
		v1Event("fresh-open", 15, 10, 4),             // open window -> accept+count
		v1Event("hist", 1, 100, 2),                   // sealed redelivery -> dup, replay accept
		v2EventWith("hist", 1, 100, 2, "UNKNOWN", 0), // cross-version dup
		v1Event("fresh-sealed", 2, 10, 1),            // sealed first delivery -> reject
	}

	planner := NewUpgradePlanner(reg, agg)
	rep, err := planner.TrialUpgrade(candidate, batch)
	if err != nil {
		t.Fatal(err)
	}
	logTrial(t, rep)
	// The batch contains one genuine sealed-window rejection, so this upgrade
	// must NOT be declared rollout-safe for this batch even though the plan
	// itself is compatible.
	if rep.Compatible != true || rep.AllPassed() {
		t.Fatalf("plan compatible but batch must not all-pass: %+v", rep.Summary)
	}
	if rep.Items[3].Reason != market.CauseWindowSealed {
		t.Fatalf("sealed item must be window_sealed: %+v", rep.Items[3])
	}

	// Operators fix/remove the offending item and re-trial a green batch.
	green := batch[:3]
	greenRep, err := planner.TrialUpgrade(candidate, green)
	if err != nil || !greenRep.AllPassed() {
		t.Fatalf("green batch must all-pass: %+v err=%v", greenRep, err)
	}

	// Enable the upgrade and process the same green batch on the SAME state.
	if c, err := reg.CommitUpgrade(candidate); err != nil || !c.OK {
		t.Fatalf("commit: %+v %v", c, err)
	}
	var live []market.Decision
	var liveErrs []error
	for _, e := range green {
		d, err := agg.Submit(e)
		live = append(live, d)
		liveErrs = append(liveErrs, err)
	}
	assertTrialMatchesLive(t, greenRep, live, liveErrs)

	// After commit, re-trialling the committed schema is idempotent and the
	// verdicts against the now-mutated state stay stable: "fresh-open" is now
	// already seen online, so it becomes a duplicate rather than a count.
	repAgain, err := planner.TrialUpgrade(candidate, green)
	if err != nil || !repAgain.Compatible {
		t.Fatalf("re-trial after commit: %+v err=%v", repAgain, err)
	}
	var again []market.Decision
	var againErrs []error
	for _, e := range green {
		d, err := agg.Submit(e)
		again = append(again, d)
		againErrs = append(againErrs, err)
	}
	assertTrialMatchesLive(t, repAgain, again, againErrs)
	if repAgain.Summary.Accepted != 0 || repAgain.Summary.Duplicated != 3 {
		t.Fatalf("post-commit re-trial sees all ids as duplicates: %+v", repAgain.Summary)
	}
}
