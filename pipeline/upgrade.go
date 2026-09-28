package pipeline

import (
	"fmt"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// TrialOutcome classifies a single historical event under a dry-run upgrade.
type TrialOutcome string

const (
	// TrialAccepted: a FIRST delivery that is accepted and counted under the
	// current online state (on time, or late-but-into-an-open-window admitted
	// by policy).
	TrialAccepted TrialOutcome = "accepted"
	// TrialDuplicate: the logical event id was already delivered (online or
	// earlier in this batch). It is NOT counted again; the FIRST verdict is
	// replayed (accepted or rejected for the original reason), and
	// Duplicate/CrossVersion/MatchedVer carry the dedup basis exactly like
	// online processing.
	TrialDuplicate TrialOutcome = "duplicate"
	// TrialDataRejected: the upgrade itself is fine, but THIS event is
	// rejected under the current online state — sealed window, lateness,
	// conflict, structural mismatch, ... (Reason carries the per-data cause).
	TrialDataRejected TrialOutcome = "data_rejected"
	// TrialSkippedIncompatible: the candidate evolution is itself
	// incompatible, so per-event projection is not meaningful.
	TrialSkippedIncompatible TrialOutcome = "skipped_plan_incompatible"
)

// TrialItem is the per-event verdict of a dry-run upgrade. The decision fields
// mirror market.Decision one-to-one, so an item can be compared directly with
// the Decision produced by the later online Submit of the same event.
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

	// Decision basis, identical to what Aggregator.Submit returns online:
	Duplicate    bool          `json:"duplicate,omitempty"`
	CrossVersion bool          `json:"cross_version,omitempty"`
	MatchedVer   int           `json:"matched_ver,omitempty"`
	Window       market.Window `json:"window"`
	Watermark    int64         `json:"watermark,omitempty"`
}

// TrialSummary aggregates a dry run. PlanCompatible distinguishes "the
// evolution itself is incompatible" from "the evolution is fine but some
// historical data is rejected/duplicated under the current state".
type TrialSummary struct {
	PlanCompatible bool `json:"plan_compatible"`
	Total          int  `json:"total"`
	// Accepted counts FIRST deliveries that are counted; a duplicate is never
	// counted here.
	Accepted     int `json:"accepted"`
	Duplicated   int `json:"duplicated"`
	DataRejected int `json:"data_rejected"`
}

// TrialReport is the full read-only result of a dry-run upgrade. When the
// planner was bound to a live aggregator, OnlineBound is true and the
// Online* fields record the exact state snapshot the verdicts were computed
// against.
type TrialReport struct {
	FromVer    int                `json:"from_ver"`
	ToVer      int                `json:"to_ver"`
	Compatible bool               `json:"compatible"`
	Reason     market.RejectCause `json:"reason,omitempty"`
	Field      string             `json:"field,omitempty"`
	Detail     string             `json:"detail,omitempty"`
	Items      []TrialItem        `json:"items"`
	Summary    TrialSummary       `json:"summary"`

	OnlineBound     bool  `json:"online_bound"`
	OnlineWatermark int64 `json:"online_watermark,omitempty"`
	OnlineSeen      int   `json:"online_seen,omitempty"`
}

// UpgradePlanner evaluates and (later) commits a candidate schema evolution
// with gray-upgrade safety rules. TrialUpgrade is read-only: it never mutates
// the registry, the aggregator or any online state.
//
// A planner created with NewUpgradePlanner judges a batch against an EMPTY
// online state (structural dry run only). Bind it to the live aggregator with
// NewOnlineUpgradePlanner to make the trial consider the current watermark,
// sealed windows and already-seen events — this is the mode whose per-event
// conclusions are guaranteed to match the later online processing.
type UpgradePlanner struct {
	reg *Registry
	agg *Aggregator // optional live state to judge against
}

// NewUpgradePlanner builds a registry-only planner. Its trials consider no
// online state (watermark 0, no seen events); use it for pure structural
// previews.
func NewUpgradePlanner(reg *Registry) *UpgradePlanner {
	return &UpgradePlanner{reg: reg}
}

// NewOnlineUpgradePlanner builds a planner whose trials judge every event
// against a SNAPSHOT of the live aggregator's decision state: current
// watermark (hence sealed-window / lateness classification) and the set of
// already-seen logical events (hence duplicate / conflict / cross-version
// reconciliation). The trial stays read-only; the live state is never changed.
func NewOnlineUpgradePlanner(agg *Aggregator) *UpgradePlanner {
	return &UpgradePlanner{reg: agg.reg, agg: agg}
}

// TrialUpgrade simulates the candidate evolution against a historical/replay
// batch WITHOUT changing any online state. For the same registry, live state
// and batch it returns identical reports.
//
// Every plan-compatible item is judged by the SAME decision code the online
// Aggregator.Submit uses, replayed on a throwaway shadow aggregator seeded
// with a deep copy of the online state. Consequently the per-item outcome,
// reject reason and duplicate markers match the later online processing of
// the same batch against the same state item by item.
//
// Two failure levels are kept apart:
//   - plan incompatible (Compatible=false): the evolution itself is illegal
//     (removal, narrowing, a new field without a safe default, ...). Nothing
//     can be enabled; every item is marked skipped_plan_incompatible;
//   - data rejected (per item): the evolution is fine, but an individual
//     event is rejected under the current state (window sealed, too late,
//     conflict, bad payload, ...).
func (p *UpgradePlanner) TrialUpgrade(candidate Schema, batch []market.Event) (*TrialReport, error) {
	// Read plan inputs under the registry lock only; never hold it while
	// taking the aggregator lock.
	p.reg.mu.RLock()
	fromVer := p.reg.latestVersionLocked()
	var prev Schema
	if fromVer > 0 {
		prev = p.reg.schemas[fromVer]
	}
	p.reg.mu.RUnlock()

	rep := &TrialReport{FromVer: fromVer, ToVer: candidate.Version}
	items := make([]TrialItem, 0, len(batch))

	// Snapshot the decision-relevant online state (if bound) BEFORE any
	// per-item work. The snapshot is deep, so the trial cannot perturb the
	// watermark, seen events, windows or statistics.
	var state shadowState
	if p.agg != nil {
		state = p.agg.snapshotState()
		rep.OnlineBound = true
		rep.OnlineWatermark = state.Watermark
		rep.OnlineSeen = len(state.Seen)
	} else {
		state = shadowState{
			Seen:   make(map[string]seenEvent),
			Config: config{windowSizeMs: 10_000, allowedLateMs: 0, latePolicy: market.PolicyDrop},
		}
	}

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
				Watermark: state.Watermark,
				Detail:    "skipped: candidate evolution is incompatible",
			})
		}
		rep.Items = items
		rep.Summary = TrialSummary{PlanCompatible: false, Total: len(batch)}
		return rep, nil
	}

	// 2. The plan is safe. Build an isolated registry (history replay + the
	// candidate enabled under gray rules) and a shadow aggregator seeded with
	// the copied online state, then drive the SAME Submit decision pipeline
	// the live aggregator uses.
	p.reg.mu.RLock()
	shadowReg := p.reg.buildShadowRegistryLocked(candidate)
	p.reg.mu.RUnlock()
	shadow := newShadowAggregator(state, shadowReg)

	rep.Compatible = true
	var accepted, duplicated, rejected int
	for i, raw := range batch {
		d, _ := shadow.submitLocked(cloneEvent(raw))
		var win market.Window
		if d.Window != nil {
			win = *d.Window
		}
		item := TrialItem{
			Index: i, EventID: d.EventID, SchemaVer: d.SchemaVer,
			EventTime: d.EventTime, OK: d.Accepted, Reason: d.Reason,
			Detail: d.Detail, Duplicate: d.Duplicate,
			CrossVersion: d.CrossVersion, MatchedVer: d.MatchedVer,
			Window: win, Watermark: d.Watermark,
		}
		switch {
		case !d.Accepted:
			// An explicit rejection always wins: a same-id delivery whose
			// content conflicts also carries Duplicate=true online, yet it is
			// a rejection (data_rejected), not a "not counted again" replay.
			item.Outcome = TrialDataRejected
			rejected++
		case d.Duplicate:
			// Accepted repeat of an already-seen logical event: replay the
			// first verdict, never count again.
			item.Outcome = TrialDuplicate
			item.Defaulted = append([]string(nil), d.Defaulted...)
			duplicated++
		default:
			item.Outcome = TrialAccepted
			accepted++
			// Audit-only enrichment: which candidate-schema defaults the read
			// side would apply to this (possibly old-version) payload. This
			// does NOT change the verdict, which stays identical to online.
			if _, defaulted, cause, _ := projectEventTo(raw, candidate); cause == market.CauseNone {
				item.Defaulted = defaulted
			}
		}
		items = append(items, item)
	}
	rep.Items = items
	rep.Summary = TrialSummary{PlanCompatible: true, Total: len(batch),
		Accepted: accepted, Duplicated: duplicated, DataRejected: rejected}
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
