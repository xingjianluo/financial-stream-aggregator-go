package aggregator

import (
	"sort"
	"sync"
	"time"
)

// LatePolicy 是窗口封死后迟到事件的处理策略。
type LatePolicy int

const (
	// LateDrop 丢弃迟到事件，但会计数并在 Counters() 中可查。
	LateDrop LatePolicy = iota
	// LateSideOutput 主统计不计入，事件副本旁路留存，可事后审计解释。
	LateSideOutput
)

// Config 是聚合管线配置。
type Config struct {
	// WindowSize 滚动窗口长度，必须为正。
	WindowSize time.Duration
	// AllowedLateness 宽限时长：当 wm - AllowedLateness >= windowEnd 时窗口封死。
	// 必须 >= 0；为 0 表示水位一过窗口末端立即封死。
	AllowedLateness time.Duration
	// LatePolicy 迟到事件处理策略。
	LatePolicy LatePolicy
}

// Decision 是一次提交的判定结果，包含可解释、可复现的判定上下文。
type Decision struct {
	EventID     string
	Reason      ReasonCode
	Accepted    bool
	EventTime   time.Time
	Watermark   time.Time
	WindowStart time.Time
	WindowEnd   time.Time
	Detail      string
}

// Aggregator 是线程安全的本地流式聚合管线。
type Aggregator struct {
	mu       sync.Mutex
	cfg      Config
	registry *Registry
	assigner *windowAssigner
	wm       *Watermark

	open    map[time.Time]*window // 尚未封死的窗口，key 为窗口起点
	sealed  map[time.Time]bool    // 已封死窗口的起点集合
	results []WindowResult        // 已封窗结果，按起点升序

	// known 记录每个"已见过"的合法事件 ID 对应的规范化内容哈希。
	// 无论事件最终被接受还是迟到丢弃，ID 都会登记，保证重复投递幂等、
	// 同 ID 内容冲突可被识别。
	known map[string]string

	side     []Event // LateSideOutput 策略下旁路留存的迟到事件
	counters map[ReasonCode]int
}

// New 创建管线并按顺序注册给定的 schema 版本。
func New(cfg Config, schemas ...Schema) (*Aggregator, error) {
	if cfg.WindowSize <= 0 {
		return nil, &SchemaIncompatibleError{Detail: "WindowSize must be positive"}
	}
	if cfg.AllowedLateness < 0 {
		return nil, &SchemaIncompatibleError{Detail: "AllowedLateness must be >= 0"}
	}
	reg := NewRegistry()
	for _, s := range schemas {
		if err := reg.Register(s); err != nil {
			return nil, err
		}
	}
	return &Aggregator{
		cfg:      cfg,
		registry: reg,
		assigner: newWindowAssigner(cfg.WindowSize),
		wm:       NewWatermark(time.Time{}),
		open:     map[time.Time]*window{},
		sealed:   map[time.Time]bool{},
		known:    map[string]string{},
		counters: map[ReasonCode]int{},
	}, nil
}

// Submit 并发安全地提交一条增量事件。整个判定过程在锁内完成，
// 因此同一事件无论被多少 goroutine 同时投递，结论唯一。
func (a *Aggregator) Submit(ev Event) Decision {
	a.mu.Lock()
	defer a.mu.Unlock()

	d := Decision{
		EventID:   ev.ID,
		EventTime: ev.EventTime.UTC(),
		Watermark: a.wm.Value().UTC(),
	}

	// 1) schema 校验与 payload 规范化。
	normalized, price, qty, err := a.registry.normalizeEvent(ev)
	if err != nil {
		if _, ok := err.(*SchemaIncompatibleError); ok {
			d.Reason = ReasonUnknownSchema
		} else {
			d.Reason = ReasonInvalidPayload
		}
		d.Detail = err.Error()
		a.counters[d.Reason]++
		return d
	}
	if ev.ID == "" {
		d.Reason = ReasonInvalidPayload
		d.Detail = "event id must not be empty"
		a.counters[d.Reason]++
		return d
	}

	hash, err := contentHash(ev.SchemaVersion, ev.EventTime, normalized)
	if err != nil {
		d.Reason = ReasonInvalidPayload
		d.Detail = err.Error()
		a.counters[d.Reason]++
		return d
	}

	// 先确定窗口归属（仅用于解释上下文，迟到与否在去重之后判定）。
	start, end := a.assigner.assign(ev.EventTime.UTC())
	d.WindowStart, d.WindowEnd = start, end

	// 2) 幂等去重 / 冲突识别：身份先于时间判定，保证结论稳定可解释。
	if existing, seen := a.known[ev.ID]; seen {
		if existing == hash {
			d.Reason = ReasonDuplicate
			d.Accepted = false
			d.Detail = "identical content already seen; idempotently ignored"
			a.counters[d.Reason]++
			return d
		}
		d.Reason = ReasonConflict
		d.Detail = (&ConflictError{EventID: ev.ID, ExistingHash: existing, GotHash: hash}).Error()
		a.counters[d.Reason]++
		return d
	}

	// 登记身份，保证迟到事件的重复投递同样幂等。
	a.known[ev.ID] = hash

	// 3) 水位/封窗判定：窗口末端已越过 (水位 - 宽限) 即为迟到。
	cutoff := a.wm.Value().Add(-a.cfg.AllowedLateness)
	if !a.wm.Value().IsZero() && !end.After(cutoff) {
		switch a.cfg.LatePolicy {
		case LateSideOutput:
			d.Reason = ReasonLateSideOutput
			a.side = append(a.side, cloneEvent(ev, normalized))
		default:
			d.Reason = ReasonLateDropped
		}
		d.Detail = (&LateEventError{
			EventID:   ev.ID,
			EventTime: d.EventTime.Format(time.RFC3339Nano),
			Watermark: d.Watermark.Format(time.RFC3339Nano),
			WindowEnd: end.Format(time.RFC3339Nano),
		}).Error()
		a.sealed[start] = true
		a.counters[d.Reason]++
		return d
	}

	// 4) 接受，计入对应窗口。
	w := a.open[start]
	if w == nil {
		w = &window{start: start, end: end}
		a.open[start] = w
	}
	w.add(windowEntry{
		eventTime: ev.EventTime.UTC(),
		id:        ev.ID,
		price:     price,
		qty:       qty,
	})
	d.Reason = ReasonAccepted
	d.Accepted = true
	a.counters[d.Reason]++
	return d
}

// AdvanceWatermark 按事件时间推进水位，并封死所有
// end <= 新水位 - AllowedLateness 的窗口。返回推进后的水位与本次新封窗结果。
// 水位回拨返回 WatermarkRegressionError，且不改变任何状态。
func (a *Aggregator) AdvanceWatermark(t time.Time) (time.Time, []WindowResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, err := a.wm.Advance(t.UTC()); err != nil {
		a.counters[ReasonWatermarkRegress]++
		return a.wm.Value().UTC(), nil, err
	}
	cutoff := a.wm.Value().Add(-a.cfg.AllowedLateness)

	var ready []time.Time
	for start := range a.open {
		if !a.open[start].end.After(cutoff) {
			ready = append(ready, start)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].Before(ready[j]) })

	fresh := make([]WindowResult, 0, len(ready))
	for _, start := range ready {
		res := a.open[start].seal()
		fresh = append(fresh, res)
		a.results = append(a.results, res)
		a.sealed[start] = true
		delete(a.open, start)
	}
	return a.wm.Value().UTC(), fresh, nil
}

// Results 返回已封窗结果的副本（按窗口起点升序）。
func (a *Aggregator) Results() []WindowResult {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]WindowResult, len(a.results))
	copy(out, a.results)
	return out
}

// SideOutput 返回被旁路留存的迟到事件副本（按到达顺序）。
func (a *Aggregator) SideOutput() []Event {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]Event, len(a.side))
	copy(out, a.side)
	return out
}

// Counters 返回各判定原因的计数快照。
func (a *Aggregator) Counters() map[ReasonCode]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[ReasonCode]int, len(a.counters))
	for k, v := range a.counters {
		out[k] = v
	}
	return out
}

// Watermark 返回当前水位。
func (a *Aggregator) Watermark() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.wm.Value().UTC()
}

func cloneEvent(ev Event, normalized map[string]any) Event {
	cp := make(map[string]any, len(normalized))
	for k, v := range normalized {
		cp[k] = v
	}
	return Event{
		ID:            ev.ID,
		SchemaVersion: ev.SchemaVersion,
		EventTime:     ev.EventTime.UTC(),
		Payload:       cp,
	}
}
