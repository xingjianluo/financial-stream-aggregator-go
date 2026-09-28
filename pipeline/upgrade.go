package pipeline

import (
	"fmt"
	"sort"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// EventPreview is the dry-run verdict for ONE historical/replayed event under
// the proposed upgrade.
type EventPreview struct {
	Index     int                `json:"index"`
	EventID   string             `json:"event_id"`
	EventTime int64              `json:"event_time"`
	SchemaVer int                `json:"schema_ver"`
	Window    *market.Window     `json:"window,omitempty"`
	Accepted  bool               `json:"accepted"`
	Duplicate bool               `json:"duplicate"`
	Reason    market.RejectCause `json:"reason"`
	ContentFP string             `json:"content_fp"`
	// Backfilled lists optional fields that would be filled with their
	// declared default when this event is read/rebuilt under the new schema.
	Backfilled []string `json:"backfilled,omitempty"`
	Watermark  int64    `json:"watermark"`
	Detail     string   `json:"detail,omitempty"`
}

// PreviewSummary is the aggregate conclusion of a dry-run over the event batch.
type PreviewSummary struct {
	Total      int                        `json:"total"`
	Accepted   int                        `json:"accepted"`
	Rejected   int                        `json:"rejected"`
	Duplicates int                        `json:"duplicates"`
	Conflicts  int                        `json:"conflicts"`
	ByReason   map[market.RejectCause]int `json:"by_reason,omitempty"`
}

// UpgradePreview is the read-only result of rehearsing a proposed schema
// upgrade. Two fundamentally different failure shapes must not be confused:
//
//   - UpgradeOK=false: the upgrade ITSELF is incompatible (see Reason, Field);
//     Events is empty because the new structure must not even be applied;
//   - UpgradeOK=true with Summary.Rejected>0: the upgrade is sound, but some
//     individual historical events fail the requirements (each carries its
//     own reason).
//
// Repeating the same dry-run with the same batch on the same online state
// yields an identical preview; nothing online changes during the rehearsal.
type UpgradePreview struct {
	UpgradeOK bool               `json:"upgrade_ok"`
	From      int                `json:"from"`
	To        int                `json:"to"`
	Reason    market.RejectCause `json:"reason,omitempty"`
	Field     string             `json:"field,omitempty"`
	Detail    string             `json:"detail,omitempty"`
	Events    []EventPreview     `json:"events,omitempty"`
	Summary   PreviewSummary     `json:"summary"`
}

// DryRunUpgrade rehearses registering schema s (under the gray-rollout rules)
// and replaying the given event batch, entirely on a private copy of the
// registry and of the aggregator's current state. Online state is not
// modified: no schema is registered, no watermark moves, no statistic
// changes.
//
// When the rehearsal is later followed by ApplyUpgrade(s) and the same batch
// is submitted without interleaving traffic, every Submit decision matches
// the corresponding EventPreview one-to-one.
func (a *Aggregator) DryRunUpgrade(s Schema, events []market.Event) UpgradePreview {
	// Compatibility is first probed on an independent registry clone so an
	// incompatible upgrade cannot touch the online registry.
	shadowReg := a.reg.Clone()
	rep, err := shadowReg.RegisterUpgrade(s)
	if err != nil || !rep.OK {
		reason := market.CauseSchemaEmpty
		var field, detail string
		if rep != nil {
			reason = rep.Reason
			field = rep.Field
			detail = rep.Detail
		}
		return UpgradePreview{UpgradeOK: false,
			From: s.Version - 1, To: s.Version,
			Reason: reason, Field: field, Detail: detail}
	}

	shadow := a.cloneState(shadowReg)
	out := UpgradePreview{UpgradeOK: true, From: rep.From, To: rep.To,
		Events: make([]EventPreview, 0, len(events)),
		Summary: PreviewSummary{Total: len(events),
			ByReason: map[market.RejectCause]int{}}}

	for i, e := range events {
		// Rebuild view under the prospective schema: this is what "reading the
		// event under the new version" would backfill, shown for audit.
		_, backfilled, bcause, bdetail := shadowReg.Canonicalize(e)
		d, submitErr := shadow.Submit(e)

		p := EventPreview{
			Index: i, EventID: d.EventID, EventTime: d.EventTime,
			SchemaVer: e.SchemaVer, Window: d.Window,
			Accepted: d.Accepted, Duplicate: d.Duplicate,
			Reason: d.Reason, ContentFP: d.ContentFP,
			Backfilled: backfilled, Watermark: d.Watermark, Detail: d.Detail,
		}
		switch {
		case p.Reason == market.CauseConflict:
			// A conflict still means the id was seen before; it must be
			// classified as a rejected conflict, not as an idempotent dup.
			out.Summary.Conflicts++
			out.Summary.Rejected++
			out.Summary.ByReason[p.Reason]++
		case p.Duplicate:
			out.Summary.Duplicates++
		case p.Accepted:
			out.Summary.Accepted++
		default:
			out.Summary.Rejected++
			if p.Reason != market.CauseNone {
				out.Summary.ByReason[p.Reason]++
			}
		}
		_ = submitErr
		if bcause != market.CauseNone {
			// A rebuild failure annotates the preview; structural Submit
			// validation independently reports the same cause.
			p.Detail = p.Detail + " [rebuild: " + string(bcause) + ": " + bdetail + "]"
		}
		out.Events = append(out.Events, p)
	}
	return out
}

// ApplyUpgrade registers schema s for real under the gray-rollout rules: new
// fields must carry a safe declared default, otherwise the upgrade is refused
// with schema_no_safe_default BEFORE any existing event can be affected.
//
// After a successful apply, submitting the batch rehearsed by DryRunUpgrade
// (without interleaving traffic) reproduces the dry-run decisions.
func (a *Aggregator) ApplyUpgrade(s Schema) (*CompatReport, error) {
	return a.reg.RegisterUpgrade(s)
}

// cloneState builds an independent aggregator over reg with a deep copy of
// the current window/watermark/dedup state. Used only by the read-only
// dry-run; all locks are released before the shadow is replayed.
func (a *Aggregator) cloneState(reg *Registry) *Aggregator {
	a.mu.Lock()
	defer a.mu.Unlock()

	shadow := &Aggregator{
		reg:       reg,
		cfg:       a.cfg,
		watermark: a.watermark,
		open:      make(map[int64]*openWindow, len(a.open)),
		sealed:    make([]market.WindowStats, len(a.sealed)),
		seen:      make(map[string]seenEvent, len(a.seen)),
		side:      make([]market.Event, len(a.side)),
	}
	for start, w := range a.open {
		nw := &openWindow{id: w.id, count: w.count, volume: w.volume,
			price: w.price, bySym: make(map[string]*market.SymbolCount, len(w.bySym))}
		for sym, sc := range w.bySym {
			cp := *sc
			nw.bySym[sym] = &cp
		}
		shadow.open[start] = nw
	}
	copy(shadow.sealed, a.sealed)
	for i := range shadow.sealed {
		shadow.sealed[i].BySymbol = append([]market.SymbolCount(nil), a.sealed[i].BySymbol...)
	}
	for id, se := range a.seen {
		cp := se
		if se.decided.Window != nil {
			w := *se.decided.Window
			cp.decided.Window = &w
		}
		shadow.seen[id] = cp
	}
	for i, e := range a.side {
		shadow.side[i] = cloneEvent(e)
	}
	return shadow
}

// String renders a one-line summary for test logs.
func (p UpgradePreview) String() string {
	reasons := make([]string, 0, len(p.Summary.ByReason))
	for c, n := range p.Summary.ByReason {
		reasons = append(reasons, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(reasons)
	if !p.UpgradeOK {
		return fmt.Sprintf("upgrade REJECTED reason=%s field=%s detail=%s",
			p.Reason, p.Field, p.Detail)
	}
	return fmt.Sprintf("upgrade ok v%d->v%d events=%d accepted=%d duplicates=%d rejected=%d conflicts=%d reasons=%v",
		p.From, p.To, p.Summary.Total, p.Summary.Accepted, p.Summary.Duplicates,
		p.Summary.Rejected, p.Summary.Conflicts, reasons)
}
