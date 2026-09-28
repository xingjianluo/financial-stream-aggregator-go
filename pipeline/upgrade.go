package pipeline

import (
	"fmt"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// TrialOutcome classifies a single historical event under a dry-run upgrade.
type TrialOutcome string

const (
	// TrialAccepted: the event is compatible with the candidate schema; any
	// missing optional field was filled from its declared default.
	TrialAccepted TrialOutcome = "accepted"
	// TrialDataRejected: the upgrade itself is fine, but THIS historical
	// event does not satisfy the candidate structure (per-data failure).
	TrialDataRejected TrialOutcome = "data_rejected"
	// TrialSkippedIncompatible: the candidate evolution is itself
	// incompatible, so per-event projection is not meaningful.
	TrialSkippedIncompatible TrialOutcome = "skipped_plan_incompatible"
)

// TrialItem is the per-event verdict of a dry-run upgrade.
type TrialItem struct {
	Index     int                `json:"index"`
	EventID   string             `json:"event_id"`
	SchemaVer int                `json:"schema_ver"`
	EventTime int64              `json:"event_time"`
	OK        bool               `json:"ok"`
	Outcome   TrialOutcome       `json:"outcome"`
	Reason    market.RejectCause `json:"reason,omitempty"`
	Detail    string             `json:"detail,omitempty"`
	Defaulted []string           `json:"defaulted,omitempty"`
}

// TrialSummary aggregates a dry run. PlanCompatible distinguishes "the
// evolution itself is incompatible" from "the evolution is fine but some
// historical data does not satisfy it".
type TrialSummary struct {
	PlanCompatible bool `json:"plan_compatible"`
	Total          int  `json:"total"`
	Accepted       int  `json:"accepted"`
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
	Items      []TrialItem        `json:"items"`
	Summary    TrialSummary       `json:"summary"`
}

// UpgradePlanner evaluates and (later) commits a candidate schema evolution
// with gray-upgrade safety rules. TrialUpgrade is read-only: it never mutates
// the registry, the aggregator or any online state.
type UpgradePlanner struct {
	reg *Registry
}

// NewUpgradePlanner builds a planner over the given registry.
func NewUpgradePlanner(reg *Registry) *UpgradePlanner {
	return &UpgradePlanner{reg: reg}
}

// TrialUpgrade simulates the candidate evolution against a historical/replay
// batch WITHOUT changing any online state. For the same registry, candidate
// and batch it returns byte-identical reports.
//
// Two failure levels are kept apart:
//   - plan incompatible (Compatible=false): the evolution itself is illegal
//     (removal, narrowing, a new field without a safe default, ...). Nothing
//     can be enabled; every item is marked skipped_plan_incompatible;
//   - data rejected (per item): the evolution is fine, but an individual
//     historical event cannot be read/rebuilt under the candidate schema.
func (p *UpgradePlanner) TrialUpgrade(candidate Schema, batch []market.Event) (*TrialReport, error) {
	p.reg.mu.RLock()
	fromVer := p.reg.latestVersionLocked()
	var prev Schema
	if fromVer > 0 {
		prev = p.reg.schemas[fromVer]
	}
	p.reg.mu.RUnlock()

	rep := &TrialReport{FromVer: fromVer, ToVer: candidate.Version}
	items := make([]TrialItem, 0, len(batch))

	// 1. Plan-level checks, identical to what CommitUpgrade enforces.
	if r, cause := evaluatePlan(prev, fromVer, candidate); cause != market.CauseNone {
		rep.Compatible = false
		rep.Reason = cause
		rep.Field = r.Field
		rep.Detail = r.Detail
		for i, e := range batch {
			items = append(items, TrialItem{
				Index: i, EventID: e.EventID, SchemaVer: e.SchemaVer,
				EventTime: e.EventTime, Outcome: TrialSkippedIncompatible,
				Detail: "skipped: candidate evolution is incompatible",
			})
		}
		rep.Items = items
		rep.Summary = TrialSummary{PlanCompatible: false, Total: len(batch)}
		return rep, nil
	}

	// 2. The plan is safe. Replay every event under the candidate version:
	//    validate the payload and apply default filling.
	rep.Compatible = true
	var accepted, rejected int
	for i, raw := range batch {
		e := cloneEvent(raw)
		item := TrialItem{Index: i, EventID: e.EventID, SchemaVer: e.SchemaVer,
			EventTime: e.EventTime}
		if cause, detail := validateAgainst(e, candidate); cause != market.CauseNone {
			item.OK = false
			item.Outcome = TrialDataRejected
			item.Reason = cause
			item.Detail = detail
			rejected++
			items = append(items, item)
			continue
		}
		_, defaulted, cause, detail := projectEventTo(e, candidate)
		if cause != market.CauseNone {
			item.OK = false
			item.Outcome = TrialDataRejected
			item.Reason = cause
			item.Detail = detail
			rejected++
			items = append(items, item)
			continue
		}
		item.OK = true
		item.Outcome = TrialAccepted
		item.Defaulted = defaulted
		if len(defaulted) > 0 {
			item.Detail = fmt.Sprintf("accepted; fields filled from defaults: %v", defaulted)
		} else {
			item.Detail = "accepted; no default filling needed"
		}
		accepted++
		items = append(items, item)
	}
	rep.Items = items
	rep.Summary = TrialSummary{PlanCompatible: true, Total: len(batch),
		Accepted: accepted, DataRejected: rejected}
	return rep, nil
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
// Registry.CommitUpgrade without writing anything.
func evaluatePlan(prev Schema, fromVer int, candidate Schema) (*CompatReport, market.RejectCause) {
	emptyReg := NewRegistry()
	if rep, _ := emptyReg.validateCandidateLocked(candidate); rep != nil && !rep.OK {
		return rep, rep.Reason
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
		if candidate.Version <= fromVer {
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
