package pipeline

import (
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

func newTestAgg(t *testing.T, opts ...Option) *Aggregator {
	t.Helper()
	reg := NewRegistry()
	if _, err := reg.Register(v1Schema()); err != nil {
		t.Fatal(err)
	}
	return New(reg, opts...)
}

func mkEvent(id, symbol string, ts, price, qty int64, trade bool) market.Event {
	return market.Event{
		EventID: id, Symbol: symbol, EventTime: ts, SchemaVer: 1,
		Price: price, Quantity: qty, IsTrade: trade,
	}
}

// TestBasicTumblingWindows verifies ordered ingestion and deterministic sealed
// window statistics.
func TestBasicTumblingWindows(t *testing.T) {
	a := newTestAgg(t, WithWindowSize(10))

	events := []market.Event{
		mkEvent("e1", "BTC", 1, 100, 2, true),
		mkEvent("e2", "BTC", 5, 110, 1, false),
		mkEvent("e3", "ETH", 9, 50, 4, true),
		mkEvent("e4", "BTC", 12, 120, 3, true),
	}
	for _, e := range events {
		d, err := a.Submit(e)
		if err != nil {
			t.Fatalf("submit %s: %v", e.EventID, err)
		}
		logDecision(t, d)
	}

	d, sealed := a.AdvanceWatermark(20)
	if d.Reason != "" {
		t.Fatalf("advance wm: %s", d.Reason)
	}
	logWindows(t, "wm=20", sealed)

	if len(sealed) != 2 {
		t.Fatalf("want 2 sealed windows, got %d", len(sealed))
	}
	w0 := sealed[0]
	if w0.Window != (market.Window{Start: 0, End: 10}) {
		t.Fatalf("window0 bounds wrong: %+v", w0.Window)
	}
	if w0.EventCount != 3 || w0.Volume != 7 || w0.PriceSum != 260 {
		t.Fatalf("window0 stats wrong: %+v", w0)
	}
	if len(w0.BySymbol) != 2 || w0.BySymbol[0].Symbol != "BTC" {
		t.Fatalf("by-symbol breakdown must be symbol-sorted: %+v", w0.BySymbol)
	}
	if w0.BySymbol[0].TradeCount != 1 || w0.BySymbol[0].QuoteCount != 1 {
		t.Fatalf("trade/quote split wrong: %+v", w0.BySymbol[0])
	}
	if w0.BySymbol[1].Symbol != "ETH" || w0.BySymbol[1].Volume != 4 {
		t.Fatalf("eth breakdown wrong: %+v", w0.BySymbol[1])
	}
	w1 := sealed[1]
	if w1.Window != (market.Window{Start: 10, End: 20}) || w1.EventCount != 1 {
		t.Fatalf("window1 wrong: %+v", w1)
	}
}

// TestOutOfOrder shows events arriving in reverse event-time order are placed
// into the correct open windows.
func TestOutOfOrder(t *testing.T) {
	a := newTestAgg(t, WithWindowSize(10))
	order := []int64{29, 21, 15, 3, 1}
	for i, ts := range order {
		e := mkEvent(fmt.Sprintf("o%d", i), "BTC", ts, 1, 1, true)
		d, err := a.Submit(e)
		logDecision(t, d)
		if err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	_, sealed := a.AdvanceWatermark(30)
	logWindows(t, "wm=30", sealed)
	if len(sealed) != 3 {
		t.Fatalf("want 3 sealed windows, got %d", len(sealed))
	}
	counts := []int64{2, 1, 2} // [0,10): t=3,1 [10,20): t=15 [20,30): t=29,21
	for i, w := range sealed {
		if w.EventCount != counts[i] {
			t.Fatalf("window %d count=%d want %d", i, w.EventCount, counts[i])
		}
	}
}

// TestDuplicateAndConflict covers idempotent re-delivery and explicit
// same-id/different-content rejection.
func TestDuplicateAndConflict(t *testing.T) {
	a := newTestAgg(t, WithWindowSize(10))

	e := mkEvent("dup", "BTC", 2, 100, 5, true)
	d1, err := a.Submit(e)
	if err != nil {
		t.Fatal(err)
	}
	logDecision(t, d1)

	// Identical redelivery: same verdict, no double counting.
	d2, err := a.Submit(e)
	if err != nil || d2.Duplicate != true || d2.Accepted != true {
		t.Fatalf("duplicate must be accepted as idempotent: %+v err=%v", d2, err)
	}
	logDecision(t, d2)
	if d2.Accepted != d1.Accepted || d2.Reason != d1.Reason ||
		d2.ContentFP != d1.ContentFP || !d2.Duplicate {
		t.Fatalf("duplicate must replay the original verdict with Duplicate=true: %+v vs %+v", d2, d1)
	}

	// Same id, conflicting content: rejected, distinguishable cause.
	conflict := e
	conflict.Price = 999
	d3, err := a.Submit(conflict)
	if err == nil || d3.Reason != market.CauseConflict {
		t.Fatalf("conflict must be CauseConflict: %+v err=%v", d3, err)
	}
	logDecision(t, d3)
	re, ok := AsRejected(err)
	if !ok || re.Cause != market.CauseConflict {
		t.Fatalf("error must carry CauseConflict, got %v", err)
	}
	if d3.ContentFP == d1.ContentFP {
		t.Fatal("conflicting content must hash to a different fingerprint")
	}

	// Seal: exactly one event was counted.
	_, sealed := a.AdvanceWatermark(10)
	logWindows(t, "wm=10", sealed)
	if len(sealed) != 1 || sealed[0].EventCount != 1 || sealed[0].Volume != 5 {
		t.Fatalf("conflict/duplicate must not change stats: %+v", sealed)
	}
}

// TestLateEventHandling exercises all three late-event distinctions:
// late-into-open-window within lateness (counted + side output), late beyond
// the allowed lateness (dropped), and late into an already-sealed window.
func TestLateEventHandling(t *testing.T) {
	t.Run("within_lateness_open_window_side_output", func(t *testing.T) {
		a := newTestAgg(t, WithWindowSize(10), WithAllowedLateness(5),
			WithLatePolicy(market.PolicyDrop))

		d, err := a.Submit(mkEvent("late-open", "BTC", 6, 1, 2, true))
		logDecision(t, d)
		if err != nil {
			t.Fatal(err)
		}
		// Watermark passes the window end but stays within the grace period;
		// [0,10) seals only once wm >= 10, so at wm=9 the window is open and
		// the late event at t=6 is still admissible.
		if d, _ := a.AdvanceWatermark(9); d.Reason != "" {
			t.Fatalf("wm 9: %s", d.Reason)
		}
		late := mkEvent("late-open-2", "ETH", 6, 1, 3, true)
		d, err = a.Submit(late)
		logDecision(t, d)
		if err != nil || !d.Accepted {
			t.Fatalf("late event at t=6 wm=9 (grace=5) must be counted: %+v err=%v", d, err)
		}
		if side := a.SideOutput(); len(side) != 1 || side[0].EventID != "late-open-2" {
			t.Fatalf("side output must contain the late event: %+v", side)
		}
		_, sealed := a.AdvanceWatermark(10)
		logWindows(t, "wm=10", sealed)
		if sealed[0].EventCount != 2 || sealed[0].Volume != 5 {
			t.Fatalf("late-but-open event must be aggregated: %+v", sealed[0])
		}
	})

	t.Run("beyond_lateness_dropped", func(t *testing.T) {
		a := newTestAgg(t, WithWindowSize(100), WithAllowedLateness(5),
			WithLatePolicy(market.PolicyDrop))
		// Keep the target window open: wm=50 < window end 100.
		if _, _ = a.AdvanceWatermark(50); true {
		}
		e := mkEvent("too-late", "BTC", 10, 1, 1, true) // 40 behind wm
		d, err := a.Submit(e)
		logDecision(t, d)
		if err == nil || d.Reason != market.CauseLateDropped {
			t.Fatalf("event beyond lateness must be CauseLateDropped: %+v err=%v", d, err)
		}
	})

	t.Run("sealed_window_immutable", func(t *testing.T) {
		a := newTestAgg(t, WithWindowSize(10))
		d, _ := a.Submit(mkEvent("first", "BTC", 1, 1, 1, true))
		logDecision(t, d)
		_, sealed := a.AdvanceWatermark(10) // seals [0,10)
		logWindows(t, "wm=10", sealed)

		after := mkEvent("after-seal", "BTC", 2, 1, 1, true)
		d, err := a.Submit(after)
		logDecision(t, d)
		if err == nil || d.Reason != market.CauseWindowSealed {
			t.Fatalf("event in sealed window must be CauseWindowSealed: %+v err=%v", d, err)
		}
		// The sealed result must be byte-identical afterwards.
		again := a.SealedWindows()
		if len(again) != 1 || again[0].EventCount != 1 || again[0].Volume != 1 {
			t.Fatalf("sealed window must be immutable: %+v", again)
		}
	})

	t.Run("reject_policy", func(t *testing.T) {
		a := newTestAgg(t, WithWindowSize(100), WithAllowedLateness(1000),
			WithLatePolicy(market.PolicyReject))
		if _, _ = a.AdvanceWatermark(50); true {
		}
		d, err := a.Submit(mkEvent("rej", "BTC", 10, 1, 1, true))
		logDecision(t, d)
		if err == nil || d.Reason != market.CauseLateDropped {
			t.Fatalf("PolicyReject must reject every late event: %+v", d)
		}
	})
}

// TestWatermarkMonotonic verifies the watermark only advances.
func TestWatermarkMonotonic(t *testing.T) {
	a := newTestAgg(t)
	if _, _ = a.AdvanceWatermark(100); a.Watermark() != 100 {
		t.Fatal("watermark should be 100")
	}
	d, sealed := a.AdvanceWatermark(50)
	if d.Reason != market.CauseWatermarkRegression || len(sealed) != 0 {
		t.Fatalf("regression must be rejected with no sealing: %+v", d)
	}
	t.Logf("[wm] regression to 50 rejected reason=%s detail=%s", d.Reason, d.Detail)
	d, _ = a.AdvanceWatermark(100) // equal, still non-advancing
	if d.Reason != market.CauseWatermarkRegression {
		t.Fatalf("equal watermark must also be rejected: %+v", d)
	}
	if a.Watermark() != 100 {
		t.Fatal("watermark changed despite rejected call")
	}
}

// TestSealedOnce ensures a window is sealed at most once even across multiple
// watermark advances.
func TestSealedOnce(t *testing.T) {
	a := newTestAgg(t, WithWindowSize(10))
	d, _ := a.Submit(mkEvent("s1", "BTC", 1, 1, 1, true))
	logDecision(t, d)
	_, s1 := a.AdvanceWatermark(10)
	_, s2 := a.AdvanceWatermark(20)
	if len(s1) != 1 || len(s2) != 0 {
		t.Fatalf("window must seal exactly once: %d then %d", len(s1), len(s2))
	}
	if len(a.SealedWindows()) != 1 {
		t.Fatal("sealed history must keep the window")
	}
}

// TestConcurrentWriters verifies serializable, loss-free ingestion under many
// concurrent writers.
func TestConcurrentWriters(t *testing.T) {
	a := newTestAgg(t, WithWindowSize(1_000_000))
	const writers = 16
	const per = 200
	var wg sync.WaitGroup
	errs := make(chan error, writers*per)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := fmt.Sprintf("w%d-e%d", w, i)
				e := mkEvent(id, fmt.Sprintf("S%d", w%4), int64(w*per+i),
					int64(i), 1, i%2 == 0)
				if _, err := a.Submit(e); err != nil {
					errs <- err
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent submit error: %v", err)
	}
	_, sealed := a.AdvanceWatermark(1_000_000)
	logWindows(t, "concurrent", sealed)
	var total int64
	var vol int64
	for _, w := range sealed {
		total += w.EventCount
		vol += w.Volume
	}
	if total != writers*per {
		t.Fatalf("lost updates: counted %d want %d", total, writers*per)
	}
	if vol != writers*per {
		t.Fatalf("lost volume: %d want %d", vol, writers*per)
	}
}
