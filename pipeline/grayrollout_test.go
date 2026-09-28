package pipeline

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// ---- Gray-rollout schemas -------------------------------------------------

// v2 adds two optional fields WITH safe defaults:
//   - venue  string, default ""
//   - score  int64,  default 0
//
// v3 widens score int64 -> float64 (value-preserving type change).
func v2GraySchema() Schema {
	return Schema{Version: 2, Fields: []FieldSpec{
		{Name: FieldEventID, Type: market.TypeString},
		{Name: FieldSymbol, Type: market.TypeString},
		{Name: FieldEventTime, Type: market.TypeInt64},
		{Name: FieldPrice, Type: market.TypeInt64},
		{Name: FieldQuantity, Type: market.TypeInt64},
		{Name: FieldIsTrade, Type: market.TypeBool},
		{Name: "venue", Type: market.TypeString, HasDefault: true, Default: ""},
		{Name: "score", Type: market.TypeInt64, HasDefault: true, Default: int64(0)},
	}}
}

func v3WidenScore() Schema {
	return Schema{Version: 3, Fields: []FieldSpec{
		{Name: FieldEventID, Type: market.TypeString},
		{Name: FieldSymbol, Type: market.TypeString},
		{Name: FieldEventTime, Type: market.TypeInt64},
		{Name: FieldPrice, Type: market.TypeInt64},
		{Name: FieldQuantity, Type: market.TypeInt64},
		{Name: FieldIsTrade, Type: market.TypeBool},
		{Name: "venue", Type: market.TypeString, HasDefault: true, Default: ""},
		{Name: "score", Type: market.TypeFloat64, HasDefault: true, Default: float64(0)},
	}}
}

// v2GraySchemaNoDefault is an UNSAFE rollout: the new field has no default.
func v2GraySchemaNoDefault() Schema {
	return Schema{Version: 2, Fields: append(append([]FieldSpec{},
		CoreSchemaV1.Fields...),
		FieldSpec{Name: "venue", Type: market.TypeString})}
}

func newGrayAgg(t *testing.T, opts ...Option) *Aggregator {
	t.Helper()
	reg := NewRegistry()
	if _, err := reg.Register(CoreSchemaV1); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.RegisterUpgrade(v2GraySchema()); err != nil {
		t.Fatal(err)
	}
	return New(reg, opts...)
}

func v1Of(e market.Event) market.Event {
	e.SchemaVer = 1
	e.Fields = nil
	return e
}

func v2Of(e market.Event, venue string, score int64) market.Event {
	e.SchemaVer = 2
	e.Fields = map[string]any{"venue": venue, "score": score}
	return e
}

func logPreview(t *testing.T, p UpgradePreview) {
	t.Helper()
	if !p.UpgradeOK {
		t.Logf("[dryrun] UPGRADE ITSELF REJECTED from=%d to=%d reason=%q field=%q detail=%s",
			p.From, p.To, p.Reason, p.Field, p.Detail)
		return
	}
	t.Logf("[dryrun] upgrade compatible v%d->v%d summary=%s", p.From, p.To, p.String())
	for _, e := range p.Events {
		win := "<none>"
		if e.Window != nil {
			win = fmt.Sprintf("[%d,%d)", e.Window.Start, e.Window.End)
		}
		t.Logf("[dryrun] idx=%d event_id=%-10s schema_ver=%d event_time=%7d window=%-14s wm=%7d accepted=%-5v duplicate=%-5v reason=%-30q backfilled=%v fp=%s detail=%s",
			e.Index, e.EventID, e.SchemaVer, e.EventTime, win, e.Watermark,
			e.Accepted, e.Duplicate, string(e.Reason), e.Backfilled, e.ContentFP, e.Detail)
	}
}

// ---- 1. Mixed-version concurrent producers --------------------------------

// TestGrayMixedConcurrentWriters: every logical event is produced twice
// concurrently, once by a v1 producer (no new fields) and once by a v2
// producer (new fields present but at their defaults). Exactly one delivery
// per logical event may ever be counted, regardless of scheduling.
func TestGrayMixedConcurrentWriters(t *testing.T) {
	a := newGrayAgg(t, WithWindowSize(1_000_000))
	const pairs = 40
	var wg sync.WaitGroup
	for i := 0; i < pairs; i++ {
		base := mkEvent(fmt.Sprintf("g%02d", i), fmt.Sprintf("S%02d", i%4),
			int64(i), int64(100+i), 2, i%2 == 0)
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = a.Submit(v1Of(base)) }()
		go func() { defer wg.Done(); _, _ = a.Submit(v2Of(base, "", 0)) }()
	}
	wg.Wait()
	_, sealed := a.AdvanceWatermark(1_000_000)
	logWindows(t, "gray-concurrent", sealed)
	var total, vol int64
	for _, w := range sealed {
		total += w.EventCount
		vol += w.Volume
	}
	if total != pairs {
		t.Fatalf("each logical event must count once: counted=%d want=%d", total, pairs)
	}
	if vol != pairs*2 {
		t.Fatalf("volume must come from one delivery per id: got=%d want=%d", vol, pairs*2)
	}
}

// ---- 2. Cross-version duplicate: order-independent, reproducible ----------

func TestGrayCrossVersionDuplicate(t *testing.T) {
	cases := []struct {
		name   string
		fst    func(market.Event) market.Event
		snd    func(market.Event) market.Event
		withV3 bool
	}{
		{"v1_then_v2_defaults", v1Of, func(e market.Event) market.Event { return v2Of(e, "", 0) }, false},
		{"v2_defaults_then_v1", func(e market.Event) market.Event { return v2Of(e, "", 0) }, v1Of, false},
		// Explicit defaults sent as float64 by a v3 producer vs. int64 by v2.
		{"v2_then_v3_widened_zero",
			func(e market.Event) market.Event { return v2Of(e, "", 0) },
			func(e market.Event) market.Event {
				e.SchemaVer = 3
				e.Fields = map[string]any{"venue": "", "score": float64(0)}
				return e
			}, true},
	}
	base := mkEvent("xv", "BTC", 2, 100, 3, true)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newGrayAgg(t, WithWindowSize(10))
			if tc.withV3 {
				if _, err := a.reg.RegisterUpgrade(v3WidenScore()); err != nil {
					t.Fatal(err)
				}
			}
			d1, err := a.Submit(tc.fst(base))
			if err != nil {
				t.Fatal(err)
			}
			logDecision(t, d1)
			d2, err := a.Submit(tc.snd(base))
			logDecision(t, d2)
			if err != nil || !d2.Duplicate || !d2.Accepted {
				t.Fatalf("cross-version redelivery must replay as accepted duplicate: %+v err=%v", d2, err)
			}
			if d2.Accepted != d1.Accepted || d2.Reason != d1.Reason || d2.ContentFP != d1.ContentFP {
				t.Fatalf("duplicate verdict must match first delivery: %+v vs %+v", d2, d1)
			}
			if d2.ContentFP == "" {
				t.Fatal("canonical fingerprint must be reported")
			}
			_, sealed := a.AdvanceWatermark(10)
			if len(sealed) != 1 || sealed[0].EventCount != 1 || sealed[0].Volume != 3 {
				t.Fatalf("cross-version pair counts once: %+v", sealed)
			}
		})
	}

	// Widened numeric field with the SAME substantive value (int64 7 vs
	// float64 7) is a duplicate; with a different value it is a conflict.
	t.Run("widened_same_value_then_different", func(t *testing.T) {
		reg := NewRegistry()
		if _, err := reg.Register(CoreSchemaV1); err != nil {
			t.Fatal(err)
		}
		if _, err := reg.RegisterUpgrade(v2GraySchema()); err != nil {
			t.Fatal(err)
		}
		if _, err := reg.RegisterUpgrade(v3WidenScore()); err != nil {
			t.Fatal(err)
		}
		a := New(reg, WithWindowSize(10))
		first := mkEvent("w1", "BTC", 1, 1, 1, true)
		first = v2Of(first, "X", 7) // score int64(7)
		d1, err := a.Submit(first)
		logDecision(t, d1)
		if err != nil {
			t.Fatal(err)
		}
		sameVal := first
		sameVal.SchemaVer = 3
		sameVal.Fields = map[string]any{"venue": "X", "score": float64(7)}
		d2, err := a.Submit(sameVal)
		logDecision(t, d2)
		if err != nil || !d2.Duplicate || d2.ContentFP != d1.ContentFP {
			t.Fatalf("widened unchanged value must be a duplicate: %+v err=%v", d2, err)
		}
		diffVal := first
		diffVal.SchemaVer = 3
		diffVal.Fields = map[string]any{"venue": "X", "score": float64(7.5)}
		d3, err := a.Submit(diffVal)
		logDecision(t, d3)
		if err == nil || d3.Accepted || d3.Reason != market.CauseConflict {
			t.Fatalf("changed widened value must be a rejected real conflict: %+v err=%v", d3, err)
		}
	})
}

// ---- 2b. Real conflicts stay distinguishable from cross-version dup -------

func TestGrayRealConflicts(t *testing.T) {
	type pair struct {
		name string
		mut  func(market.Event) market.Event
	}
	pairs := []pair{
		{"price_diff", func(e market.Event) market.Event { e.Price = 999; return e }},
		{"qty_diff", func(e market.Event) market.Event { e.Quantity = 42; return e }},
		{"new_field_real_value", func(e market.Event) market.Event {
			return v2Of(e, "NSDQ", 0)
		}},
		{"new_field_score_real", func(e market.Event) market.Event {
			return v2Of(e, "", 9)
		}},
	}
	for _, tc := range pairs {
		t.Run(tc.name, func(t *testing.T) {
			a := newGrayAgg(t, WithWindowSize(10))
			base := mkEvent("c1", "BTC", 2, 100, 3, true)
			d1, err := a.Submit(v1Of(base))
			logDecision(t, d1)
			if err != nil {
				t.Fatal(err)
			}
			other := v2Of(base, "", 0)
			other = tc.mut(other)
			d2, err := a.Submit(other)
			logDecision(t, d2)
			if err == nil || d2.Accepted || d2.Reason != market.CauseConflict {
				t.Fatalf("%s must be rejected with CauseConflict, not a cross-version duplicate: %+v err=%v",
					tc.name, d2, err)
			}
			if d2.ContentFP == d1.ContentFP {
				t.Fatalf("%s must change the canonical fingerprint", tc.name)
			}
			re, ok := AsRejected(err)
			if !ok || re.Cause != market.CauseConflict {
				t.Fatalf("%s error must carry CauseConflict: %v", tc.name, err)
			}
		})
	}
}

// ---- 3. Default backfill on rebuild ----------------------------------------

func TestGrayDefaultBackfill(t *testing.T) {
	reg := NewRegistry()
	if _, err := reg.Register(CoreSchemaV1); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.RegisterUpgrade(v2GraySchema()); err != nil {
		t.Fatal(err)
	}
	old := v1Of(mkEvent("old1", "ETH", 5, 10, 1, false))

	// Old events must not fail as structurally incomplete after the upgrade.
	if c, detail := reg.Validate(old); c != market.CauseNone {
		t.Fatalf("old event must remain structurally valid: %s %s", c, detail)
	}

	// Rebuild under the new schema: declared defaults are filled, stably.
	built1, filled, c, detail := reg.Canonicalize(old)
	if c != market.CauseNone {
		t.Fatalf("rebuild failed: %s %s", c, detail)
	}
	t.Logf("[backfill] event old1 rebuilt under v%d filled=%v venue=%q score=%d",
		built1.SchemaVer, filled, built1.Fields["venue"], built1.Fields["score"])
	if built1.SchemaVer != 2 || built1.Fields["venue"] != "" || built1.Fields["score"] != int64(0) {
		t.Fatalf("backfill must use declared defaults: %+v", built1.Fields)
	}
	if len(filled) != 2 || filled[0] != "venue" || filled[1] != "score" {
		t.Fatalf("filled-field audit list wrong: %v", filled)
	}
	built2, _, _, _ := reg.Canonicalize(old)
	if fmt.Sprint(built1) != fmt.Sprint(built2) {
		t.Fatalf("backfill must be deterministic and replayable")
	}

	// After v3 registers, the rebuild of the SAME old event stays stable
	// (score normalizes to the widened type with the same numeric value).
	if _, err := reg.RegisterUpgrade(v3WidenScore()); err != nil {
		t.Fatal(err)
	}
	built3, filled3, _, _ := reg.Canonicalize(old)
	t.Logf("[backfill] event old1 rebuilt under v%d filled=%v score=%v(type=%T)",
		built3.SchemaVer, filled3, built3.Fields["score"], built3.Fields["score"])
	if built3.SchemaVer != 3 || built3.Fields["venue"] != "" || built3.Fields["score"] != float64(0) {
		t.Fatalf("backfill across widening must stay value-stable: %+v", built3.Fields)
	}
}

// ---- 4. Upgrade without a safe default is refused BEFORE it takes effect --

func TestGrayUnsafeUpgradeRejected(t *testing.T) {
	reg := NewRegistry()
	if _, err := reg.Register(CoreSchemaV1); err != nil {
		t.Fatal(err)
	}
	rep, err := reg.RegisterUpgrade(v2GraySchemaNoDefault())
	if err == nil || rep.OK || rep.Reason != market.CauseSchemaNoSafeDefault || rep.Field != "venue" {
		t.Fatalf("unsafe upgrade must be schema_no_safe_default: rep=%+v err=%v", rep, err)
	}
	t.Logf("[upgrade] unsafe v2 refused before activation: reason=%s field=%s detail=%s",
		rep.Reason, rep.Field, rep.Detail)
	re, ok := AsRejected(err)
	if !ok || re.Cause != market.CauseSchemaNoSafeDefault {
		t.Fatalf("error must carry CauseSchemaNoSafeDefault: %v", err)
	}
	if vs := reg.Versions(); fmt.Sprint(vs) != "[1]" {
		t.Fatalf("refused upgrade must not register anything: %v", vs)
	}

	// The legacy permissive path is unaffected: without the gray-rollout
	// contract the same schema body may still register the old way.
	if rep, err := reg.Register(v2GraySchemaNoDefault()); err != nil || !rep.OK {
		t.Fatalf("plain Register keeps its historical behavior: rep=%+v err=%v", rep, err)
	}
	t.Log("[upgrade] plain Register still allows the legacy required-add path")
}

// ---- 5. Sealed-window resubmission across versions -------------------------

func TestGraySealedResubmitAcrossVersions(t *testing.T) {
	a := newGrayAgg(t, WithWindowSize(10))
	first := v1Of(mkEvent("seal1", "BTC", 1, 10, 2, true))
	d1, err := a.Submit(first)
	logDecision(t, d1)
	if err != nil {
		t.Fatal(err)
	}
	_, sealed := a.AdvanceWatermark(10) // seals [0,10)
	logWindows(t, "seal wm=10", sealed)

	// Same logical event (new producer, defaulted new fields) arrives AFTER
	// sealing: it is a cross-version duplicate and replays the FIRST verdict
	// (accepted) — never window_sealed, never counted twice.
	redelivery := v2Of(first, "", 0)
	d2, err := a.Submit(redelivery)
	logDecision(t, d2)
	if err != nil || !d2.Duplicate || !d2.Accepted || d2.Reason != d1.Reason {
		t.Fatalf("post-seal cross-version resubmit must replay the first verdict: %+v err=%v", d2, err)
	}
	if win := a.SealedWindows(); len(win) != 1 || win[0].EventCount != 1 || win[0].Volume != 2 {
		t.Fatalf("sealed stats immutable after cross-version resubmit: %+v", win)
	}

	// A genuinely different resubmission is still a conflict, even post-seal.
	conflict := v2Of(first, "LSE", 0)
	d3, err := a.Submit(conflict)
	logDecision(t, d3)
	if err == nil || d3.Reason != market.CauseConflict {
		t.Fatalf("post-seal changed content must be conflict: %+v err=%v", d3, err)
	}
}

// ---- 6. Dry-run: read-only, repeatable, distinguishable conclusions --------

func TestGrayDryRun(t *testing.T) {
	t.Run("incompatible_upgrade_vs_individual_data", func(t *testing.T) {
		// (a) The upgrade ITSELF is unsafe -> UpgradeOK=false, event list
		// empty, and online state untouched. Probed on a v1-only aggregator
		// (a v2 with defaults already exists on the gray test aggregator).
		baseReg := NewRegistry()
		if _, err := baseReg.Register(CoreSchemaV1); err != nil {
			t.Fatal(err)
		}
		probe := New(baseReg, WithWindowSize(10))
		batch := []market.Event{v1Of(mkEvent("d1", "BTC", 1, 1, 1, true))}
		before := baseReg.Versions()
		pv := probe.DryRunUpgrade(v2GraySchemaNoDefault(), batch)
		logPreview(t, pv)
		if pv.UpgradeOK || pv.Reason != market.CauseSchemaNoSafeDefault ||
			pv.Field != "venue" || len(pv.Events) != 0 {
			t.Fatalf("unsafe upgrade must be reported as upgrade-level failure: %+v", pv)
		}
		if got := baseReg.Versions(); fmt.Sprint(got) != fmt.Sprint(before) {
			t.Fatalf("dry-run must not change the online registry: %v vs %v", got, before)
		}

		// (b) The upgrade is sound, but ONE historical datum does not satisfy
		// it: UpgradeOK=true with a per-event failure.
		a := newGrayAgg(t, WithWindowSize(10))
		beforeGray := a.reg.Versions()
		sound := v3WidenScore()
		goodV1 := v1Of(mkEvent("d1", "BTC", 1, 1, 1, true))
		dupV1 := goodV1 // same logical event again
		goodV2 := v2Of(mkEvent("d2", "ETH", 2, 2, 1, false), "X", 1)
		badTyped := v2Of(mkEvent("d3", "BTC", 3, 3, 1, true), "X", 1)
		badTyped.Fields = map[string]any{"venue": 77, "score": int64(1)} // wrong type
		batch2 := []market.Event{goodV1, dupV1, goodV2, badTyped}
		pv2 := a.DryRunUpgrade(sound, batch2)
		logPreview(t, pv2)
		if !pv2.UpgradeOK || pv2.To != 3 {
			t.Fatalf("sound widening upgrade must be upgrade-ok: %+v", pv2)
		}
		if pv2.Summary.Total != 4 || pv2.Summary.Accepted != 2 ||
			pv2.Summary.Duplicates != 1 || pv2.Summary.Rejected != 1 {
			t.Fatalf("summary counts wrong: %+v", pv2.Summary)
		}
		if pv2.Summary.ByReason[market.CauseSchemaFieldTypeMismatch] != 1 {
			t.Fatalf("the individual bad datum must carry its own cause: %+v", pv2.Summary)
		}
		if pv2.Events[1].Duplicate != true {
			t.Fatalf("second identical delivery must preview as duplicate")
		}
		if got := a.reg.Versions(); fmt.Sprint(got) != fmt.Sprint(beforeGray) {
			t.Fatalf("dry-run must not register the sound upgrade either: %v", got)
		}
		if a.Watermark() != 0 {
			t.Fatal("dry-run must not move the online watermark")
		}

		// Repeatability: same batch, same state -> identical preview.
		pv2Again := a.DryRunUpgrade(sound, batch2)
		j1, _ := json.Marshal(pv2)
		j2, _ := json.Marshal(pv2Again)
		if string(j1) != string(j2) {
			t.Fatalf("dry-run must be repeatable:\n%s\n%s", j1, j2)
		}

		// (c) After the upgrade is actually enabled, replaying the same batch
		// reproduces the previewed decisions one-to-one.
		if _, err := a.ApplyUpgrade(sound); err != nil {
			t.Fatalf("apply: %v", err)
		}
		for _, want := range pv2.Events {
			got, err := a.Submit(batch2[want.Index])
			logDecision(t, got)
			if want.Reason == market.CauseNone && err != nil {
				t.Fatalf("event %s: preview said pass, live failed: %v", want.EventID, err)
			}
			if got.Accepted != want.Accepted || got.Duplicate != want.Duplicate ||
				got.Reason != want.Reason || got.ContentFP != want.ContentFP {
				t.Fatalf("live decision for %s diverges from dry-run:\n preview=%+v\n live=%+v",
					want.EventID, want, got)
			}
		}
	})

	t.Run("sealed_window_dryrun_matches_live", func(t *testing.T) {
		// Dry-run clones the CURRENT state: a sealed window stays sealed in
		// the shadow, so sealing-aware previews match post-apply reality.
		a := newGrayAgg(t, WithWindowSize(10))
		if d, _ := a.Submit(v1Of(mkEvent("s1", "BTC", 1, 1, 1, true))); d.Reason != "" {
			t.Fatalf("setup submit: %s", d.Reason)
		}
		a.AdvanceWatermark(10)
		batch := []market.Event{
			v2Of(mkEvent("late1", "BTC", 2, 1, 1, true), "", 0),
			v1Of(mkEvent("fresh1", "BTC", 11, 2, 1, true)),
		}
		pv := a.DryRunUpgrade(v3WidenScore(), batch)
		logPreview(t, pv)
		if !pv.UpgradeOK {
			t.Fatalf("upgrade must be ok: %s", pv.Detail)
		}
		if pv.Events[0].Reason != market.CauseWindowSealed ||
			!pv.Events[1].Accepted {
			t.Fatalf("shadow must replay against current state: %+v", pv.Events)
		}
		if a.Watermark() != 10 {
			t.Fatal("dry-run must not alter online watermark")
		}
	})
}

// ---- 7. Regression guard: old scenarios under a gray-registered registry --

// TestGrayDecisionDeterminismReplay runs one mixed script twice on
// independent instances and compares the full decision multiset, covering
// out-of-order, lateness, watermark progression, conflicts and sealing under
// the new canonical dedup.
func TestGrayDecisionDeterminismReplay(t *testing.T) {
	script := func(a *Aggregator) []market.Decision {
		var ds []market.Decision
		sub := func(e market.Event) market.Decision {
			d, _ := a.Submit(e)
			ds = append(ds, d)
			return d
		}
		sub(v1Of(mkEvent("r1", "BTC", 1001, 10, 2, true)))
		sub(v2Of(mkEvent("r2", "ETH", 1005, 5, 1, false), "X", 0))
		sub(v1Of(mkEvent("r3", "BTC", 1003, 3, 4, true)))         // out of order
		sub(v2Of(mkEvent("r1", "BTC", 1001, 10, 2, true), "", 0)) // cross-version dup
		a.AdvanceWatermark(1005)
		sub(v1Of(mkEvent("r4", "ETH", 1001, 7, 3, true)))        // late into open window
		a.AdvanceWatermark(1010)                                 // seal [1000,1010)
		sub(v2Of(mkEvent("r5", "BTC", 1002, 1, 1, true), "", 0)) // window sealed
		conf := v2Of(mkEvent("r2", "ETH", 1005, 5, 1, false), "Y", 0)
		sub(conf) // real conflict on the new field
		return ds
	}
	key := func(ds []market.Decision) []string {
		ks := make([]string, 0, len(ds))
		for _, d := range ds {
			ks = append(ks, fmt.Sprintf("%s|acc=%t|dup=%t|reason=%s|fp=%s",
				d.EventID, d.Accepted, d.Duplicate, d.Reason, d.ContentFP))
		}
		sort.Strings(ks)
		return ks
	}
	a1 := newGrayAgg(t, WithWindowSize(10), WithAllowedLateness(5))
	a2 := newGrayAgg(t, WithWindowSize(10), WithAllowedLateness(5))
	ds1 := script(a1)
	ds2 := script(a2)
	k1, k2 := key(ds1), key(ds2)
	for _, d := range ds2 {
		logDecision(t, d)
	}
	if fmt.Sprint(k1) != fmt.Sprint(k2) {
		t.Fatalf("mixed gray script not reproducible:\n%v\n%v", k1, k2)
	}
	win := a1.SealedWindows()
	logWindows(t, "gray replay", win)
	var st *market.WindowStats
	for i := range win {
		if win[i].Window.Start == 1000 {
			st = &win[i]
		}
	}
	if st == nil || st.EventCount != 4 || st.Volume != 2+1+4+3 {
		t.Fatalf("sealed [1000,1010) must count r1,r2,r3,r4(late) once each: %+v", st)
	}
}
