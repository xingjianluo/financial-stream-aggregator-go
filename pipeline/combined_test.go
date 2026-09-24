package pipeline

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

func v2Event(id, symbol string, ts, price, qty int64, trade bool, venue string) market.Event {
	e := mkEvent(id, symbol, ts, price, qty, trade)
	e.SchemaVer = 2
	e.Fields = map[string]any{"venue": venue}
	return e
}

// runCombined builds a FRESH registry (v1 + additive v2) and aggregator and
// executes one fixed scenario. Running it twice must produce identical sorted
// decision multisets and identical sealed-window statistics.
func runCombined(t *testing.T, label string) ([]market.Decision, []market.WindowStats) {
	t.Helper()
	reg := NewRegistry()
	if _, err := reg.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Register(v2SchemaAddVenue()); err != nil {
		t.Fatal(err)
	}
	a := New(reg, WithWindowSize(10), WithAllowedLateness(5))

	var mu sync.Mutex
	var decisions []market.Decision
	record := func(d market.Decision) {
		mu.Lock()
		decisions = append(decisions, d)
		mu.Unlock()
	}

	// Phase 1: concurrent writers, all on-time, unique ids, t in [0,10).
	const nWriters, nPer = 8, 25
	var wg sync.WaitGroup
	for w := 0; w < nWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < nPer; i++ {
				id := fmt.Sprintf("c-w%d-%d", w, i)
				e := mkEvent(id, []string{"BTC", "ETH"}[w%2], 3, 1, 1, true)
				d, err := a.Submit(e)
				if err != nil {
					t.Errorf("[%s] concurrent submit %s: %v", label, id, err)
					return
				}
				record(d)
			}
		}(w)
	}

	// Phase 2 (serial, explainable): out-of-order events, additive-schema
	// event and an event with an unknown schema version.
	serial := []market.Event{
		mkEvent("a1", "BTC", 1001, 10, 2, true),
		mkEvent("a2", "BTC", 1005, 12, 1, false),
		mkEvent("a3", "ETH", 1003, 30, 4, true), // out of order: 1003 after 1005
		v2Event("b1", "BTC", 1007, 9, 1, true, "NSDQ"),
	}
	for _, e := range serial {
		d, err := a.Submit(e)
		logDecision(t, d)
		if err != nil {
			t.Errorf("[%s] serial submit %s: %v", label, e.EventID, err)
		}
		record(d)
	}
	unknown := mkEvent("bad-ver", "BTC", 1008, 1, 1, true)
	unknown.SchemaVer = 42
	d, err := a.Submit(unknown)
	logDecision(t, d)
	if err == nil || d.Reason != market.CauseSchemaUnknownVersion {
		t.Fatalf("[%s] unknown schema version must be rejected: %+v", label, d)
	}
	record(d)
	wg.Wait()

	// Phase 3: watermark to 1005 keeps [1000,1010) open. A late event at
	// t=1001 (4 behind wm, within grace 5) into the open window is counted and
	// side-output. Then an identical retry must be an idempotent duplicate.
	if d, _ := a.AdvanceWatermark(1005); d.Reason != "" {
		t.Fatalf("[%s] wm 1005: %s", label, d.Reason)
	}
	lateOpen := mkEvent("late-open", "ETH", 1001, 7, 3, true)
	d, err = a.Submit(lateOpen)
	logDecision(t, d)
	if err != nil || !d.Accepted {
		t.Fatalf("[%s] late-into-open must be counted: %+v err=%v", label, d, err)
	}
	record(d)
	if side := a.SideOutput(); len(side) != 1 || side[0].EventID != "late-open" {
		t.Fatalf("[%s] side output mismatch: %+v", label, side)
	}
	d, _ = a.Submit(lateOpen)
	logDecision(t, d)
	if !d.Duplicate || !d.Accepted {
		t.Fatalf("[%s] late event retry must be idempotent duplicate: %+v", label, d)
	}
	record(d)

	// Phase 4: seal [1000,1010); a later-arriving event for it must NOT change
	// the result and must be reported with a distinct cause.
	_, sealedNow := a.AdvanceWatermark(1010)
	logWindows(t, label+" wm=1010", sealedNow)
	sealedLate := mkEvent("sealed-late", "BTC", 1002, 5, 9, true)
	d, err = a.Submit(sealedLate)
	logDecision(t, d)
	if err == nil || d.Reason != market.CauseWindowSealed {
		t.Fatalf("[%s] sealed-window event must be CauseWindowSealed: %+v", label, d)
	}
	record(d)

	// Phase 5: same id with conflicting content -> explicit, distinct cause.
	conflict := mkEvent("a1", "BTC", 1001, 999, 2, true) // price differs
	d, err = a.Submit(conflict)
	logDecision(t, d)
	if err == nil || d.Reason != market.CauseConflict {
		t.Fatalf("[%s] conflict must be CauseConflict: %+v", label, d)
	}
	record(d)

	// Watermark regression is itself a distinguishable judgement.
	d, _ = a.AdvanceWatermark(900)
	if d.Reason != market.CauseWatermarkRegression {
		t.Fatalf("[%s] watermark regression: %+v", label, d)
	}
	t.Logf("[%s] watermark regression 1010->900 rejected: %s", label, d.Reason)

	windows := a.SealedWindows()
	logWindows(t, label+" final", windows)
	return decisions, windows
}

// TestCombinedScenario is the required combination test. It runs the whole
// mixed scenario twice on independent registries/aggregators and asserts that
// every decision (acceptance + reject cause) and every sealed statistic
// reproduces exactly.
func TestCombinedScenario(t *testing.T) {
	d1, w1 := runCombined(t, "run-1")
	d2, w2 := runCombined(t, "run-2")

	key := func(d market.Decision) string {
		return fmt.Sprintf("%s|accepted=%t|reason=%s|duplicate=%t|fp=%s",
			d.EventID, d.Accepted, d.Reason, d.Duplicate, d.ContentFP)
	}
	keys1 := make([]string, len(d1))
	keys2 := make([]string, len(d2))
	for i := range d1 {
		keys1[i] = key(d1[i])
	}
	for i := range d2 {
		keys2[i] = key(d2[i])
	}
	sort.Strings(keys1)
	sort.Strings(keys2)
	if fmt.Sprint(keys1) != fmt.Sprint(keys2) {
		t.Fatalf("decision multisets differ:\n%v\n%v", keys1, keys2)
	}
	if fmt.Sprint(w1) != fmt.Sprint(w2) {
		t.Fatalf("sealed windows differ:\n%+v\n%+v", w1, w2)
	}

	// Sanity-check the actual numbers in the sealed [1000,1010) window.
	var target *market.WindowStats
	for i := range w1 {
		if w1[i].Window.Start == 1000 {
			target = &w1[i]
		}
	}
	if target == nil {
		t.Fatal("window [1000,1010) missing")
	}
	// serial: a1,a2,a3,b1 (4) + late-open (1); concurrent events are at t=3
	// so they belong to [0,10) which never seals.
	if target.EventCount != 5 {
		t.Fatalf("[1000,1010) event count = %d, want 5", target.EventCount)
	}
	if target.Volume != 2+1+4+1+3 {
		t.Fatalf("[1000,1010) volume = %d, want 11", target.Volume)
	}
	t.Logf("[replay] %d decisions, %d sealed windows reproduced identically; "+
		"[1000,1010) events=%d volume=%d", len(keys1), len(w1),
		target.EventCount, target.Volume)
}

// TestReplayHistoricalSchema confirms old events keep their original verdict
// after the schema evolves, and that an incompatible evolution is refused.
func TestReplayHistoricalSchema(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	old := mkEvent("h1", "BTC", 1, 1, 1, true)
	if c, _ := r.Validate(old); c != market.CauseNone {
		t.Fatalf("old payload rejected before evolution: %s", c)
	}
	if _, err := r.Register(v2SchemaAddVenue()); err != nil {
		t.Fatal(err)
	}
	if c, detail := r.Validate(old); c != market.CauseNone {
		t.Fatalf("historical v1 payload must validate after v2: %s %s", c, detail)
	}
	badV3 := Schema{Version: 3, Fields: CoreSchemaV1.Fields} // removes venue
	rep, err := r.Register(badV3)
	if err == nil || rep.Reason != market.CauseSchemaRemovingField {
		t.Fatalf("incompatible evolution must be refused: %+v err=%v", rep, err)
	}
	t.Logf("[replay] incompatible evolution refused: reason=%s field=%s",
		rep.Reason, rep.Field)
}
