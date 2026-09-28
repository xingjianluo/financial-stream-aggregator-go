// Package pipeline implements the concurrency-safe local market-data
// aggregation engine: idempotent incremental ingestion, event-time tumbling
// windows with a monotone watermark, explicit late-event handling and
// schema-versioned compatibility checking.
package pipeline

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// Option configures an Aggregator.
type Option func(*config)

type config struct {
	windowSizeMs  int64
	allowedLateMs int64
	latePolicy    market.LatePolicy
}

// WithWindowSize sets the tumbling-window length in event-time milliseconds.
func WithWindowSize(ms int64) Option {
	return func(c *config) {
		if ms > 0 {
			c.windowSizeMs = ms
		}
	}
}

// WithAllowedLateness sets how far behind the watermark an event may be and
// still be counted into its not-yet-sealed window (in milliseconds).
func WithAllowedLateness(ms int64) Option {
	return func(c *config) {
		if ms >= 0 {
			c.allowedLateMs = ms
		}
	}
}

// WithLatePolicy selects the late-event policy.
func WithLatePolicy(p market.LatePolicy) Option {
	return func(c *config) { c.latePolicy = p }
}

// openWindow accumulates one in-flight window.
type openWindow struct {
	id     market.Window
	count  int64
	volume int64
	price  int64
	bySym  map[string]*market.SymbolCount
}

// seenEvent remembers one accepted/known event id for idempotent dedup.
type seenEvent struct {
	fp      string
	window  market.Window
	late    bool
	decided market.Decision
	// first holds the first physical delivery, so a later delivery under a
	// different schema version can be reconciled against it (default filling
	// + numeric widening) rather than against a raw versioned fingerprint.
	first market.Event
}

// Aggregator is a concurrency-safe local streaming aggregator. All state
// transitions are serialized under one mutex; the resulting state depends only
// on the (event, watermark) input stream, so a replayed stream reproduces the
// same decisions and sealed windows.
type Aggregator struct {
	mu        sync.Mutex
	reg       *Registry
	cfg       config
	watermark int64

	open   map[int64]*openWindow // keyed by window start
	sealed []market.WindowStats
	seen   map[string]seenEvent
	side   []market.Event
}

// New constructs an aggregator over the given registry. Defaults: 10s windows,
// zero allowed lateness, late events side-output for audit.
func New(reg *Registry, opts ...Option) *Aggregator {
	c := config{
		windowSizeMs:  10_000,
		allowedLateMs: 0,
		latePolicy:    market.PolicyDrop,
	}
	for _, o := range opts {
		o(&c)
	}
	return &Aggregator{
		reg:  reg,
		cfg:  c,
		open: make(map[int64]*openWindow),
		seen: make(map[string]seenEvent),
	}
}

// WindowOf maps an event timestamp to its aligned tumbling window [start,end).
func (a *Aggregator) WindowOf(ts int64) market.Window {
	size := a.cfg.windowSizeMs
	start := (ts / size) * size
	return market.Window{Start: start, End: start + size}
}

// Watermark returns the current watermark (event-time milliseconds).
func (a *Aggregator) Watermark() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.watermark
}

// Submit applies one incremental event and returns its reproducible Decision.
//
// Evaluation order (the first failing rule wins, which is what makes the
// outcome explainable):
//  1. schema validation against the event's declared version;
//  2. idempotency: a known id with identical content is a duplicate, with
//     conflicting content a conflict (never silently overwritten);
//  3. event-time/window classification against the current watermark.
func (a *Aggregator) Submit(e market.Event) (market.Decision, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	e = cloneEvent(e) // internal state must not alias caller-owned maps
	fp := ContentFingerprint(e)
	win := a.WindowOf(e.EventTime)
	d := market.Decision{
		EventID:   e.EventID,
		EventTime: e.EventTime,
		Window:    windowPtr(win),
		ContentFP: fp,
		Watermark: a.watermark,
		SchemaVer: e.SchemaVer,
	}

	// 1. Structural validation against the event's own schema version.
	if cause, detail := a.reg.Validate(e); cause != market.CauseNone {
		d.Reason = cause
		d.Detail = detail
		return d, &RejectedError{Cause: cause, Detail: detail}
	}

	// 2. Idempotent dedup / explicit conflict, gray-upgrade aware.
	//    - identical physical content (any version): idempotent duplicate;
	//    - different version but semantically identical once new optional
	//      fields take their declared defaults and int64->float64 widening
	//      keeps the same numeric magnitude: cross-version duplicate, still
	//      counted once and replaying the original verdict;
	//    - different version with a real difference in price/quantity/a new
	//      field: cross_version_conflict, a distinguishable rejection;
	//    - same version, different content: conflict.
	if prev, ok := a.seen[e.EventID]; ok {
		switch a.classifyRepeated(prev, e, fp, &d) {
		case repeatedSame:
			replay := prev.decided
			replay.Duplicate = true
			replay.Watermark = a.watermark
			replay.Window = windowPtr(prev.window)
			replay.SchemaVer = e.SchemaVer
			return replay, nil
		case repeatedCrossVersion:
			replay := prev.decided
			replay.Duplicate = true
			replay.CrossVersion = true
			replay.MatchedVer = prev.first.SchemaVer
			replay.Watermark = a.watermark
			replay.Window = windowPtr(prev.window)
			replay.SchemaVer = e.SchemaVer
			replay.Defaulted = d.Defaulted
			return replay, nil
		case repeatedConflict:
			return d, &RejectedError{Cause: d.Reason, Detail: d.Detail}
		}
	}

	// 3. Event-time classification.
	if e.EventTime < a.watermark {
		// The event is late relative to the watermark.
		if win.End <= a.watermark {
			// Its window is already sealed: results are immutable.
			d.Reason = market.CauseWindowSealed
			d.Detail = "window is sealed by watermark; late event rejected"
			a.remember(d, win, false, e)
			return d, &RejectedError{Cause: market.CauseWindowSealed, Detail: d.Detail}
		}
		if a.watermark-e.EventTime > a.cfg.allowedLateMs {
			d.Reason = market.CauseLateDropped
			d.Detail = "lateness exceeds allowed lateness"
			a.remember(d, win, false, e)
			return d, &RejectedError{Cause: market.CauseLateDropped, Detail: d.Detail}
		}
		if a.cfg.latePolicy == market.PolicyReject {
			d.Reason = market.CauseLateDropped
			d.Detail = "late event rejected by policy"
			a.remember(d, win, false, e)
			return d, &RejectedError{Cause: market.CauseLateDropped, Detail: d.Detail}
		}
		// PolicyDrop + window still open + within allowed lateness: count it
		// and side-output an audit copy; the late admission is recorded.
		a.addToWindow(win, e)
		a.side = append(a.side, e)
		d.Accepted = true
		d.Detail = "late but window still open; counted and side-output"
		a.remember(d, win, true, e)
		return d, nil
	}

	// On-time (possibly out-of-order within open windows): simply count it.
	a.addToWindow(win, e)
	d.Accepted = true
	a.remember(d, win, false, e)
	return d, nil
}

// remember records the first verdict for an event id so retried delivery can
// reproduce it. Must be called with a.mu held.
func (a *Aggregator) remember(d market.Decision, win market.Window, late bool, e market.Event) {
	a.seen[d.EventID] = seenEvent{
		fp:      d.ContentFP,
		window:  win,
		late:    late,
		decided: d,
		first:   e,
	}
}

// repeatedKind is the outcome of comparing a re-delivered id with its first
// delivery under the gray-upgrade reconciliation rules.
type repeatedKind int

const (
	repeatedSame repeatedKind = iota
	repeatedCrossVersion
	repeatedConflict
)

// classifyRepeated decides whether a repeated delivery is the same logical
// event, a cross-version-compatible re-delivery, or a real conflict. It fills
// d.Duplicate/d.Reason/d.Detail/d.Defaulted as appropriate.
func (a *Aggregator) classifyRepeated(prev seenEvent, e market.Event, fp string, d *market.Decision) repeatedKind {
	if prev.fp == fp {
		// Same version, byte-identical content: the original idempotent path.
		d.Duplicate = true
		return repeatedSame
	}
	if e.SchemaVer == prev.first.SchemaVer {
		// Same version, different content: never silently overwrite.
		d.Duplicate = true
		d.Reason = market.CauseConflict
		d.Detail = "event_id " + e.EventID +
			" already seen with different content under schema v" +
			strconv.Itoa(e.SchemaVer)
		return repeatedConflict
	}

	// Cross-version: canonicalize both sides onto the registry's newest
	// schema. Default filling and the int64->float64 widening are transparent
	// there; any remaining difference is a genuine value conflict.
	fpNew, defNew, okNew := a.reg.SemanticFingerprint(e)
	fpOld, defOld, okOld := a.reg.SemanticFingerprint(prev.first)
	if !okNew || !okOld {
		d.Duplicate = true
		d.Reason = market.CauseCrossVersionConflict
		d.Detail = fmt.Sprintf("event_id %s v%d vs v%d cannot be reconciled: "+
			"a field lacks a safe default under the newest schema",
			e.EventID, prev.first.SchemaVer, e.SchemaVer)
		return repeatedConflict
	}
	if fpNew == fpOld {
		d.Duplicate = true
		d.Defaulted = mergeDefaulted(defOld, defNew)
		d.Detail = fmt.Sprintf("cross-version duplicate: v%d equivalent to v%d "+
			"after default filling and numeric widening",
			e.SchemaVer, prev.first.SchemaVer)
		return repeatedCrossVersion
	}
	d.Duplicate = true
	d.Reason = market.CauseCrossVersionConflict
	d.Detail = fmt.Sprintf("event_id %s v%d vs v%d differs in price/quantity/new-field "+
		"values after defaults are applied", e.EventID,
		prev.first.SchemaVer, e.SchemaVer)
	return repeatedConflict
}

// mergeDefaulted unions the defaulted field names reported for the two
// deliveries, deduplicated and sorted, for a stable audit trail.
func mergeDefaulted(a, b []string) []string {
	set := make(map[string]struct{}, len(a)+len(b))
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		set[s] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// addToWindow folds one accepted event into its open window.
func (a *Aggregator) addToWindow(win market.Window, e market.Event) {
	w, ok := a.open[win.Start]
	if !ok {
		w = &openWindow{id: win, bySym: make(map[string]*market.SymbolCount)}
		a.open[win.Start] = w
	}
	w.count++
	w.volume += e.Quantity
	w.price += e.Price
	sc := w.bySym[e.Symbol]
	if sc == nil {
		sc = &market.SymbolCount{Symbol: e.Symbol}
		w.bySym[e.Symbol] = sc
	}
	sc.Count++
	sc.Volume += e.Quantity
	if e.IsTrade {
		sc.TradeCount++
	} else {
		sc.QuoteCount++
	}
}

// AdvanceWatermark monotonically advances the watermark to ts. Every window
// with End <= ts is sealed exactly once and returned in window-start order;
// sealed windows are immutable and are removed from the active set.
//
// A non-advancing call (ts <= current watermark) changes nothing and reports
// the rejection in the returned Decision; the watermark never regresses.
func (a *Aggregator) AdvanceWatermark(ts int64) (market.Decision, []market.WindowStats) {
	a.mu.Lock()
	defer a.mu.Unlock()

	d := market.Decision{Watermark: a.watermark}
	if ts <= a.watermark {
		d.Reason = market.CauseWatermarkRegression
		d.Detail = "watermark must advance monotonically"
		return d, nil
	}
	a.watermark = ts
	d.Watermark = ts

	var sealed []market.WindowStats
	starts := make([]int64, 0, len(a.open))
	for start := range a.open {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	for _, start := range starts {
		w := a.open[start]
		if w.id.End <= ts {
			stats := snapshotWindow(w, ts)
			a.sealed = append(a.sealed, stats)
			sealed = append(sealed, stats)
			delete(a.open, start)
		}
	}
	return d, sealed
}

// snapshotWindow renders an open window as an immutable stats snapshot.
func snapshotWindow(w *openWindow, sealedAtWM int64) market.WindowStats {
	syms := make([]market.SymbolCount, 0, len(w.bySym))
	for _, sc := range w.bySym {
		syms = append(syms, *sc)
	}
	sort.Slice(syms, func(i, j int) bool { return syms[i].Symbol < syms[j].Symbol })
	return market.WindowStats{
		Window:     w.id,
		EventCount: w.count,
		Volume:     w.volume,
		PriceSum:   w.price,
		SealedAtWM: sealedAtWM,
		BySymbol:   syms,
	}
}

// SealedWindows returns finalized windows in window-start order.
func (a *Aggregator) SealedWindows() []market.WindowStats {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]market.WindowStats, len(a.sealed))
	copy(out, a.sealed)
	for i := range out {
		out[i].BySymbol = append([]market.SymbolCount(nil), out[i].BySymbol...)
	}
	return out
}

// SideOutput returns audit copies of late-but-into-open-window events.
func (a *Aggregator) SideOutput() []market.Event {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]market.Event, len(a.side))
	copy(out, a.side)
	return out
}

// cloneEvent deep-copies the caller-supplied map so later mutation by the
// caller cannot affect stored fingerprints, stats or side-output copies.
func cloneEvent(e market.Event) market.Event {
	if len(e.Fields) > 0 {
		f := make(map[string]any, len(e.Fields))
		for k, v := range e.Fields {
			f[k] = v
		}
		e.Fields = f
	}
	return e
}

func windowPtr(w market.Window) *market.Window {
	cp := w
	return &cp
}
