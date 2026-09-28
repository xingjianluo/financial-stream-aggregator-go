package pipeline

import (
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// ---- gray-upgrade test fixtures --------------------------------------------

// v2Gray adds "venue" with a safe default, and "fee_bps" with default 0.
func v2GraySchema() Schema {
	return Schema{Version: 2, Fields: append(append([]FieldSpec{},
		CoreSchemaV1.Fields...),
		FieldSpec{Name: "venue", Type: market.TypeString, HasDefault: true, Default: "UNKNOWN"},
		FieldSpec{Name: "fee_bps", Type: market.TypeInt64, HasDefault: true, Default: int64(0)},
	)}
}

// v3Widen widens quantity int64 -> float64 (the single permitted widening).
func v3WidenSchema() Schema {
	v2 := v2GraySchema()
	fields := make([]FieldSpec, 0, len(v2.Fields))
	for _, f := range v2.Fields {
		if f.Name == FieldQuantity {
			f.Type = market.TypeFloat64
		}
		fields = append(fields, f)
	}
	return Schema{Version: 3, Fields: fields}
}

func newGrayAgg(t *testing.T, opts ...Option) (*Registry, *Aggregator) {
	t.Helper()
	reg := NewRegistry()
	if _, err := reg.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.CommitUpgrade(v2GraySchema()); err != nil {
		t.Fatal(err)
	}
	return reg, New(reg, opts...)
}

func v1Event(id string, ts, price, qty int64) market.Event {
	return market.Event{EventID: id, Symbol: "BTC", EventTime: ts, SchemaVer: 1,
		Price: price, Quantity: qty, IsTrade: true}
}

func v2EventWith(id string, ts, price, qty int64, venue string, fee int64) market.Event {
	return market.Event{EventID: id, Symbol: "BTC", EventTime: ts, SchemaVer: 2,
		Price: price, Quantity: qty, IsTrade: true,
		Fields: map[string]any{"venue": venue, "fee_bps": fee}}
}

func logGrayDecision(t *testing.T, label string, d market.Decision) {
	t.Helper()
	win := "<none>"
	if d.Window != nil {
		win = fmt.Sprintf("[%d,%d)", d.Window.Start, d.Window.End)
	}
	t.Logf("[%s] event_id=%-8s ver=%d matched_ver=%d event_time=%d window=%-12s "+
		"accepted=%-5v dup=%-5v cross=%-5v reason=%q defaulted=%v detail=%s",
		label, d.EventID, d.SchemaVer, d.MatchedVer, d.EventTime, win,
		d.Accepted, d.Duplicate, d.CrossVersion, string(d.Reason), d.Defaulted, d.Detail)
}

// TestCrossVersionDuplicate covers the core gray rule: the same logical event
// delivered by the old and the new producer counts once regardless of arrival
// order, because the new fields only take their declared defaults.
func TestCrossVersionDuplicate(t *testing.T) {
	t.Run("old_then_new", func(t *testing.T) {
		_, a := newGrayAgg(t, WithWindowSize(10))
		old := v1Event("x1", 1, 100, 5)
		// New producer sends the same logical event with defaults matching:
		// venue=UNKNOWN (the declared default), fee_bps=0 (default).
		new := v2EventWith("x1", 1, 100, 5, "UNKNOWN", 0)

		d1, err := a.Submit(old)
		if err != nil {
			t.Fatal(err)
		}
		logGrayDecision(t, "old-first", d1)
		d2, err := a.Submit(new)
		if err != nil {
			t.Fatalf("cross-version duplicate must be accepted, got %v: %+v", err, d2)
		}
		logGrayDecision(t, "old-first", d2)
		if !d2.Duplicate || !d2.CrossVersion || d2.MatchedVer != 1 || !d2.Accepted {
			t.Fatalf("want cross-version duplicate replaying acceptance, got %+v", d2)
		}
		if len(d2.Defaulted) == 0 {
			t.Fatalf("defaulted field names must be reported, got %+v", d2)
		}

		_, sealed := a.AdvanceWatermark(10)
		logWindows(t, "old-first wm=10", sealed)
		if sealed[0].EventCount != 1 || sealed[0].Volume != 5 {
			t.Fatalf("logical event must count once: %+v", sealed[0])
		}
	})

	t.Run("new_then_old", func(t *testing.T) {
		_, a := newGrayAgg(t, WithWindowSize(10))
		new := v2EventWith("x2", 1, 100, 5, "UNKNOWN", 0)
		old := v1Event("x2", 1, 100, 5)

		d1, err := a.Submit(new)
		if err != nil {
			t.Fatal(err)
		}
		logGrayDecision(t, "new-first", d1)
		d2, err := a.Submit(old)
		if err != nil {
			t.Fatalf("old after new must also dedup: %v %+v", err, d2)
		}
		logGrayDecision(t, "new-first", d2)
		if !d2.Duplicate || !d2.CrossVersion || d2.MatchedVer != 2 {
			t.Fatalf("want cross-version duplicate matched_ver=2, got %+v", d2)
		}
		_, sealed := a.AdvanceWatermark(10)
		if sealed[0].EventCount != 1 {
			t.Fatalf("must count once regardless of arrival order: %+v", sealed[0])
		}
	})

	t.Run("arrival_order_independent", func(t *testing.T) {
		// The verdict must be reproducible: whichever version arrives first,
		// the second is a cross-version duplicate and stats are identical.
		stats := func(newFirst bool) market.WindowStats {
			_, a := newGrayAgg(t, WithWindowSize(10))
			old := v1Event("x3", 2, 11, 3)
			new := v2EventWith("x3", 2, 11, 3, "UNKNOWN", 0)
			if newFirst {
				a.Submit(new)
				a.Submit(old)
			} else {
				a.Submit(old)
				a.Submit(new)
			}
			_, s := a.AdvanceWatermark(10)
			return s[0]
		}
		s1, s2 := stats(false), stats(true)
		if fmt.Sprint(s1) != fmt.Sprint(s2) {
			t.Fatalf("stats depend on arrival order: %+v vs %+v", s1, s2)
		}
		if s1.EventCount != 1 {
			t.Fatalf("want exactly 1 count, got %d", s1.EventCount)
		}
	})
}

// TestCrossVersionRealConflict: default-equivalent re-deliveries dedup, but a
// substantive difference in price, quantity or a new field's value is rejected
// with cross_version_conflict, distinguishable from a same-version conflict.
func TestCrossVersionRealConflict(t *testing.T) {
	cases := []struct {
		name  string
		mut   func(e market.Event) market.Event
		which string
	}{
		{"price_differs", func(e market.Event) market.Event { e.Price = 999; return e }, "price"},
		{"quantity_differs", func(e market.Event) market.Event { e.Quantity = 77; return e }, "quantity"},
		{"new_field_value_differs", func(e market.Event) market.Event {
			e.Fields = map[string]any{"venue": "NYSE", "fee_bps": int64(0)}
			return e
		}, "venue"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, a := newGrayAgg(t, WithWindowSize(10))
			old := v1Event("c1", 1, 100, 5)
			new := tc.mut(v2EventWith("c1", 1, 100, 5, "UNKNOWN", 0))
			d1, _ := a.Submit(old)
			d2, err := a.Submit(new)
			logGrayDecision(t, tc.name, d2)
			if err == nil || d2.Reason != market.CauseCrossVersionConflict {
				t.Fatalf("%s: want cross_version_conflict, got %+v err=%v", tc.which, d2, err)
			}
			if re, ok := AsRejected(err); !ok || re.Cause != market.CauseCrossVersionConflict {
				t.Fatalf("error must carry cross_version_conflict: %v", err)
			}
			if d2.ContentFP == d1.ContentFP {
				t.Fatal("raw fingerprints must differ for conflicting content")
			}
			_, sealed := a.AdvanceWatermark(10)
			if sealed[0].EventCount != 1 {
				t.Fatalf("conflicting redelivery must not count: %+v", sealed[0])
			}

			// Same-version disagreement remains a plain conflict: the two
			// causes must stay distinguishable.
			regSame := NewRegistry()
			if _, err := regSame.Register(v1Schema()); err != nil {
				t.Fatal(err)
			}
			a2 := New(regSame, WithWindowSize(10))
			same := v1Event("c2", 1, 100, 5)
			sameMut := same
			sameMut.Price = 1
			a2.Submit(same)
			d3, err := a2.Submit(sameMut)
			logGrayDecision(t, tc.name+"-samever", d3)
			if err == nil || d3.Reason != market.CauseConflict {
				t.Fatalf("same-version mismatch must remain CauseConflict: %+v", d3)
			}
		})
	}
}

// TestNumericWideningEquivalence: int64->float64 widening with the SAME
// magnitude is a cross-version duplicate; a different magnitude conflicts.
func TestNumericWideningEquivalence(t *testing.T) {
	reg := NewRegistry()
	if _, err := reg.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.CommitUpgrade(v2GraySchema()); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.CommitUpgrade(v3WidenSchema()); err != nil {
		t.Fatal(err)
	}
	a := New(reg, WithWindowSize(10))

	old := v1Event("w1", 1, 100, 5) // quantity int64 5
	d1, err := a.Submit(old)
	if err != nil {
		t.Fatal(err)
	}
	// Under v3, quantity widened to float64 with equal magnitude 5.0.
	widened := market.Event{EventID: "w1", Symbol: "BTC", EventTime: 1, SchemaVer: 3,
		Price: 100, IsTrade: true,
		Fields: map[string]any{"quantity": float64(5), "venue": "UNKNOWN", "fee_bps": int64(0)}}
	d2, err := a.Submit(widened)
	logGrayDecision(t, "widen-equal", d2)
	if err != nil || !d2.Duplicate || !d2.CrossVersion {
		t.Fatalf("equal-magnitude widening must dedup: %+v err=%v", d2, err)
	}

	changed := widened
	changed.Fields = map[string]any{"quantity": float64(5.5), "venue": "UNKNOWN", "fee_bps": int64(0)}
	d3, err := a.Submit(changed)
	logGrayDecision(t, "widen-diff", d3)
	if err == nil || d3.Reason != market.CauseCrossVersionConflict {
		t.Fatalf("changed magnitude across widening must conflict: %+v", d3)
	}
	_ = d1
}

// TestGrayConcurrentProducers runs the two gray producers concurrently: the
// old one emits v1 ids and the new one emits the SAME ids under v2 (defaults
// matching). However goroutines race, each logical id must be counted once.
func TestGrayConcurrentProducers(t *testing.T) {
	_, a := newGrayAgg(t, WithWindowSize(1_000_000))
	const n = 200
	var wg sync.WaitGroup
	errs := make(chan error, 2*n)
	dups := make(chan market.Decision, 2*n)

	produce := func(makeEv func(i int) market.Event) {
		defer wg.Done()
		for i := 0; i < n; i++ {
			d, err := a.Submit(makeEv(i))
			if err != nil {
				errs <- err
				continue
			}
			if d.Duplicate {
				dups <- d
			}
		}
	}
	wg.Add(2)
	go produce(func(i int) market.Event { return v1Event(fmt.Sprintf("g%03d", i), int64(i), 10, 1) })
	go produce(func(i int) market.Event {
		return v2EventWith(fmt.Sprintf("g%03d", i), int64(i), 10, 1, "UNKNOWN", 0)
	})
	wg.Wait()
	close(errs)
	close(dups)
	for err := range errs {
		t.Fatalf("cross-version duplicate is not an error: %v", err)
	}
	var cross, same int
	for d := range dups {
		if d.CrossVersion {
			cross++
		} else {
			same++
		}
		if !d.Accepted {
			t.Fatalf("dedup must replay acceptance: %+v", d)
		}
	}
	if cross+same != n {
		t.Fatalf("want exactly %d duplicate deliveries (one per logical id), got %d", n, cross+same)
	}
	if cross != n {
		t.Fatalf("all duplicates here are cross-version, got cross=%d same=%d", cross, same)
	}
	_, sealed := a.AdvanceWatermark(1_000_000)
	logWindows(t, "gray-concurrent", sealed)
	var total int64
	for _, w := range sealed {
		total += w.EventCount
	}
	if total != n {
		t.Fatalf("each logical event counts once under concurrent gray producers: got %d want %d", total, n)
	}
	t.Logf("[gray-concurrent] %d logical ids, %d cross-version duplicates, counted=%d", n, cross, total)
}

// TestCrossVersionDuplicateAfterSeal: once a window is sealed, a cross-version
// re-delivery of an already-accepted logical event replays the FIRST verdict
// (acceptance) instead of recounting; it is still recognized as a
// cross-version duplicate. A genuinely different redelivery conflicts. This
// mirrors the same-version rule and stays reproducible regardless of timing.
func TestCrossVersionDuplicateAfterSeal(t *testing.T) {
	_, a := newGrayAgg(t, WithWindowSize(10))
	first := v1Event("s1", 1, 100, 2)
	d1, err := a.Submit(first)
	if err != nil {
		t.Fatal(err)
	}
	logGrayDecision(t, "seal", d1)
	_, sealed := a.AdvanceWatermark(10)
	logWindows(t, "seal wm=10", sealed)

	// New producer redelivers the SAME logical event (defaults equivalent)
	// after the window was sealed: replay the original acceptance, count once.
	newVer := v2EventWith("s1", 1, 100, 2, "UNKNOWN", 0)
	d2, err := a.Submit(newVer)
	logGrayDecision(t, "seal-redeliver", d2)
	if err != nil || !d2.Accepted {
		t.Fatalf("post-seal cross-version redelivery replays first acceptance: %+v err=%v", d2, err)
	}
	if !d2.Duplicate || !d2.CrossVersion || d2.MatchedVer != 1 {
		t.Fatalf("it must be recognized as a cross-version duplicate: %+v", d2)
	}
	if again := a.SealedWindows(); len(again) != 1 || again[0].EventCount != 1 || again[0].Volume != 2 {
		t.Fatalf("sealed stats immutable, duplicate does not recount: %+v", again)
	}
	t.Logf("[seal] replay first verdict across versions at watermark=%d event_time=%d: accepted=%v counted_once=true",
		d2.Watermark, d2.EventTime, d2.Accepted)

	// A genuinely conflicting redelivery after sealing: dedup precedes the
	// window classification, and the conflict remains distinguishable.
	conflict := v2EventWith("s1", 1, 999, 2, "UNKNOWN", 0)
	d3, err := a.Submit(conflict)
	logGrayDecision(t, "seal-conflict", d3)
	if err == nil || d3.Reason != market.CauseCrossVersionConflict {
		t.Fatalf("post-seal real conflict must be cross_version_conflict: %+v", d3)
	}
	if again := a.SealedWindows(); again[0].EventCount != 1 {
		t.Fatalf("conflict must not change sealed stats: %+v", again)
	}
}
