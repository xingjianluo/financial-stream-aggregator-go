package pipeline

import (
	"fmt"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// TrialOutcome classifies a single event in a dry run.
type TrialOutcome string

const (
	// TrialAccepted: the event is accepted as a FIRST (non-duplicate)
	// delivery and counted into its window. Missing optional fields are
	// listed in Defaulted when the event had to be projected to the newest
	// schema for cross-version reconciliation.
	TrialAccepted TrialOutcome = "accepted"
	// TrialDuplicate: the event is a re-delivery of a logical id the
	// pipeline has already seen (within the batch or in the online ledger).
	// It is NOT counted again. The REPLAYED first verdict is exposed
	// verbatim (Accepted/Reason), so it covers:
	//   - same-content idempotent duplicate (replays acceptance),
	//   - cross-version-equivalent re-delivery (Duplicate+CrossVersion,
	//     replays acceptance, MatchedVer/Defaulted give the audit basis),
	//   - a duplicate of an id whose FIRST verdict was a rejection
	//     (replays that rejection with its original cause, e.g. an event
	//     whose window was sealed before the id was ever delivered).
	TrialDuplicate TrialOutcome = "duplicate"
	// TrialDataRejected: a FIRST delivery that the pipeline explicitly
	// rejects under the current online state. Reason distinguishes every
	// case: window_sealed / late_dropped / conflict /
	// cross_version_conflict / schema_*.
	TrialDataRejected TrialOutcome = "data_rejected"
	// TrialSkippedIncompatible: the candidate evolution is itself
	// incompatible, so per-event processing is not meaningful.
	TrialSkippedIncompatible TrialOutcome = "skipped_plan_incompatible"
)

// TrialItem is the per-event verdict of a dry run. It mirrors the live
// market.Decision fields one to one, which is what makes the assertion
// "trial item i == online Submit of event i from the same starting state"
// checkable field by field.
type TrialItem struct {
	Index     int            `json:"index"`
	EventID   string         `json:"event_id"`
	SchemaVer int            `json:"schema_ver"`
	EventTime int64          `json:"event_time"`
	Window    *market.Window `json:"window,omitempty"`
	// Accepted is the verdict (including a replayed duplicate verdict):
	// true for an accepted first delivery or a duplicate replaying
	// acceptance; false for a first-delivery rejection or a duplicate that
	// replays a rejection.
	Accepted  bool `json:"accepted"`
	Duplicate bool `json:"duplicate"`
	// Counted reports whether THIS delivery increments window statistics.
	// Only a non-duplicate accepted first delivery counts; duplicates and
	// rejections never do.
	Counted      bool               `json:"counted"`
	CrossVersion bool               `json:"cross_version,omitempty"`
	MatchedVer   int                `json:"matched_ver,omitempty"`
	Outcome      TrialOutcome       `json:"outcome"`
	Reason       market.RejectCause `json:"reason,omitempty"`
	Detail       string             `json:"detail,omitempty"`
	Defaulted    []string           `json:"defaulted,omitempty"`
	Watermark    int64              `json:"watermark"`
	ContentFP    string             `json:"content_fp,omitempty"`
}

// TrialSummary aggregates a dry run. PlanCompatible distinguishes "the
// evolution itself is incompatible" from "the evolution is fine but some
// data is rejected". Accepted counts first deliveries that are counted;
// Duplicated counts re-deliveries that are not counted again (whether they
// replay acceptance or a rejection); DataRejected counts explicitly
// rejected first deliveries.
type TrialSummary struct {
	PlanCompatible bool `json:"plan_compatible"`
	Total          int  `json:"total"`
	Accepted       int  `json:"accepted"`
	Duplicated     int  `json:"duplicated"`
	DataRejected   int  `json:"data_rejected"`
}

// TrialReport is the full read-only result of a dry-run upgrade.
type TrialReport struct {
	FromVer    int                `json:"from_ver"`
	ToVer      int                `json:"to_ver"`
	Compatible bool               `json:"compatible"`
	Reason     market.RejectCause `json:"reason,omitempty"`
	Field      string             `json:"field,omitempty"`
	Detail     string             `json:"detail,omitempty"`
	// OnlineWatermark is the live watermark the trial was taken against.
	// Every per-item judgement uses this (and the matching sealed/open
	// window set), so a later trial after a watermark advance may
	// legitimately change verdicts; for the SAME online state repeated
	// trials are identical.
	OnlineWatermark int64        `json:"online_watermark"`
	Items           []TrialItem  `json:"items"`
	Summary         TrialSummary `json:"summary"`
}

// AllPassed is the enable gate: the plan is compatible AND every item would
// be processed without an explicit rejection. A duplicate replaying a
// rejection counts as a failure; duplicates replaying acceptance do not.
func (r *TrialReport) AllPassed() bool {
	if r == nil || !r.Compatible {
		return false
	}
	for _, it := range r.Items {
		if !it.Accepted {
			return false
		}
	}
	return true
}

// UpgradePlanner evaluates and (later) commits a candidate schema evolution
// with gray-upgrade safety rules. TrialUpgrade is read-only: it never mutates
// the registry, the aggregator or any online state (watermark, sealed/open
// windows, seen-event ledger, side output).
//
// A trial is taken against the aggregator's CURRENT online state: sealed
// windows, allowed lateness and policy, the watermark and every already-seen
// logical id participate exactly as they do in Aggregator.Submit. Replaying
// the same batch against the same state yields the same item-by-item
// verdicts, and committing the candidate and submitting the same batch
// online reproduces those verdicts one to one.
type UpgradePlanner struct {
	reg *Registry
	agg *Aggregator
}

// NewUpgradePlanner builds a planner over the live registry/aggregator whose
// current state the dry run will be judged against.
func NewUpgradePlanner(reg *Registry, agg *Aggregator) *UpgradePlanner {
	return &UpgradePlanner{reg: reg, agg: agg}
}

// TrialUpgrade simulates enabling the candidate evolution and then
// processing batch against the aggregator's CURRENT online state, WITHOUT
// changing any online state. The shadow aggregator runs the IDENTICAL
// Aggregator.Submit pipeline, so for the same starting state and batch the
// per-item verdicts are byte-for-byte what CommitUpgrade + Submit would
// produce online.
//
// Three judgement levels are kept apart:
//   - plan incompatible (Compatible=false): the evolution itself is illegal
//     (removal, narrowing, a new field without a safe default, ...). Nothing
//     can be enabled; every item is skipped_plan_incompatible; neither the
//     registry nor the aggregator is touched;
//   - per-item data rejected: the evolution is fine, but processing THIS
//     event from the current state explicitly rejects it (window_sealed,
//     late_dropped, conflict, cross_version_conflict, schema_*);
//   - per-item duplicate: a re-delivery of an already-seen logical id (in
//     the online ledger or earlier in the same batch). It is never counted
//     again and its replayed first verdict is exposed, so first-count,
//     do-not-recount and explicit-reject stay distinguishable item by item.
//
// Repeated trials with the same inputs and the same online state return
// identical reports; the trial never advances the watermark, seals a window,
// records a seen id or changes statistics or the side output.
func (p *UpgradePlanner) TrialUpgrade(candidate Schema, batch []market.Event) (*TrialReport, error) {
	p.reg.mu.RLock()
	fromVer := p.reg.latestVersionLocked()
	var prev Schema
	if fromVer > 0 {
		prev = p.reg.schemas[fromVer]
	}
	schemas := p.reg.snapshotLocked()
	p.reg.mu.RUnlock()

	rep := &TrialReport{FromVer: fromVer, ToVer: candidate.Version,
		OnlineWatermark: p.agg.Watermark()}
	items := make([]TrialItem, 0, len(batch))

	// 1. Plan-level checks, identical to what CommitUpgrade enforces.
	if r, cause := evaluatePlan(schemas, prev, fromVer, candidate); cause != market.CauseNone {
		rep.Compatible = false
		rep.Reason = cause
		rep.Field = r.Field
		rep.Detail = r.Detail
		for i, e := range batch {
			items = append(items, TrialItem{
				Index: i, EventID: e.EventID, SchemaVer: e.SchemaVer,
				EventTime: e.EventTime, Watermark: rep.OnlineWatermark,
				Outcome: TrialSkippedIncompatible,
				Detail:  "skipped: candidate evolution is incompatible",
			})
		}
		rep.Items = items
		rep.Summary = TrialSummary{PlanCompatible: false, Total: len(batch)}
		return rep, nil
	}

	// 2. The plan is safe. Detach a shadow aggregator over a detached
	//    registry with the candidate committed, and replay the batch through
	//    the real Submit pipeline. The shadow accumulates in-batch dedup
	//    state (a logical id appearing twice within the batch is first
	//    counted, then a duplicate), exactly mirroring live processing.
	rep.Compatible = true
	shadow := p.agg.shadowClone(candidate, true)
	summary := TrialSummary{PlanCompatible: true, Total: len(batch)}
	for i, raw := range batch {
		e := cloneEvent(raw)
		d, err := shadow.Submit(e)
		item := decisionToItem(i, d)
		// The same boundary Submit uses separates the three cases the
		// caller-facing contract requires item by item:
		//   - a non-nil *RejectedError is an EXPLICIT rejection. This covers
		//     first deliveries refused by the current state (window_sealed,
		//     late_dropped, schema_*) AND same/cross-version conflicts (which
		//     carry Duplicate=true together with the error: the id is known
		//     but this delivery contradicts it);
		//   - a nil error with Duplicate=true is a benign re-delivery that
		//     replays the first verdict and is never counted (a duplicate of
		//     an id whose first verdict was a rejection is included: it
		//     replays that rejection, no error);
		//   - a nil error otherwise is the counted first delivery.
		switch {
		case err != nil:
			item.Outcome = TrialDataRejected
			summary.DataRejected++
		case d.Duplicate:
			item.Outcome = TrialDuplicate
			summary.Duplicated++
		default:
			item.Outcome = TrialAccepted
			summary.Accepted++
		}
		items = append(items, item)
	}
	rep.Items = items
	rep.Summary = summary
	return rep, nil
}

// decisionToItem maps a shadow Submit verdict onto a trial item, copying
// every field the live/trial consistency assertion compares.
func decisionToItem(index int, d market.Decision) TrialItem {
	item := TrialItem{
		Index:        index,
		EventID:      d.EventID,
		SchemaVer:    d.SchemaVer,
		EventTime:    d.EventTime,
		Window:       d.Window,
		Accepted:     d.Accepted,
		Duplicate:    d.Duplicate,
		CrossVersion: d.CrossVersion,
		MatchedVer:   d.MatchedVer,
		Reason:       d.Reason,
		Detail:       d.Detail,
		Defaulted:    d.Defaulted,
		Watermark:    d.Watermark,
		ContentFP:    d.ContentFP,
	}
	// Only a first, accepted delivery increments window statistics.
	item.Counted = d.Accepted && !d.Duplicate
	return item
}

// ProjectEvent rebuilds e as if read under target: fields absent from e but
// declared by target with a safe default are filled with that default. It
// returns the projected event, the sorted list of defaulted field names, and
// a distinguishable failure cause when projection is impossible. The input is
// never mutated, so repeated rebuilds are stable and replayable.
func ProjectEvent(e market.Event, target Schema) (market.Event, []string, market.RejectCause, string) {
	return projectEventTo(e, target)
}

// evaluatePlan mirrors the structural and gray-safety checks of
// Registry.CommitUpgrade without writing anything. An already-committed
// identical candidate (re-trial after commit) is compatible, matching
// CommitUpgrade's idempotency.
func evaluatePlan(schemas map[int]Schema, prev Schema, fromVer int, candidate Schema) (*CompatReport, market.RejectCause) {
	emptyReg := NewRegistry()
	if rep, _ := emptyReg.validateCandidateLocked(candidate); rep != nil && !rep.OK {
		return rep, rep.Reason
	}
	if existing, ok := schemas[candidate.Version]; ok {
		if schemaEqual(existing, candidate) {
			return &CompatReport{OK: true, From: candidate.Version, To: candidate.Version,
				Detail: "identical schema already registered"}, market.CauseNone
		}
		return &CompatReport{OK: false, From: candidate.Version, To: candidate.Version,
				Reason: market.CauseSchemaDuplicateVersion,
				Detail: "schema version already registered with a different body"},
			market.CauseSchemaDuplicateVersion
	}
	if fromVer == 0 {
		if candidate.Version != 1 {
			return &CompatReport{OK: false, From: 0, To: candidate.Version,
					Reason: market.CauseSchemaVersionGap,
					Detail: "first schema must be version 1"},
				market.CauseSchemaVersionGap
		}
		return &CompatReport{OK: true, From: 0, To: 1}, market.CauseNone
	}
	if candidate.Version != fromVer+1 {
		cause := market.CauseSchemaVersionGap
		if candidate.Version < fromVer {
			cause = market.CauseSchemaVersionRegressed
		}
		return &CompatReport{OK: false, From: fromVer, To: candidate.Version,
				Reason: cause, Detail: fmt.Sprintf(
					"expected next version %d, got %d", fromVer+1, candidate.Version)},
			cause
	}
	if rep, err := checkEvolution(prev, candidate); err != nil {
		return rep, rep.Reason
	}
	if rep, err := checkGraySafety(prev, candidate); err != nil {
		return rep, rep.Reason
	}
	return &CompatReport{OK: true, From: fromVer, To: candidate.Version}, market.CauseNone
}
