package aggregator

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

const testLayout = "2006-01-02 15:04:05.000"

// mkTime 构造测试用事件时间（UTC），锚定固定日期，只精确到毫秒避免日志噪音。
func mkTime(clock string) time.Time {
	t, err := time.ParseInLocation(testLayout, "2026-09-24 "+clock, time.UTC)
	if err != nil {
		panic(err)
	}
	return t
}

func mkEvent(id string, schemaV int, clock string, price float64, qty int64, extra map[string]any) Event {
	p := map[string]any{"price": price, "qty": qty}
	for k, v := range extra {
		p[k] = v
	}
	return Event{ID: id, SchemaVersion: schemaV, EventTime: mkTime(clock), Payload: p}
}

func testSchemaV1() Schema {
	return Schema{Name: "quote", Version: 1, Fields: []FieldDef{
		{Name: "price", Type: FieldFloat64},
		{Name: "qty", Type: FieldInt64},
	}}
}

func logDecision(t *testing.T, tag string, d Decision) {
	t.Helper()
	t.Logf("[%s] event=%-4s eventTime=%s watermark=%s window=[%s,%s) reason=%-17s accepted=%-5v detail=%s",
		tag,
		d.EventID,
		fmtTime(d.EventTime),
		fmtTime(d.Watermark),
		fmtTime(d.WindowStart),
		fmtTime(d.WindowEnd),
		d.Reason,
		d.Accepted,
		d.Detail,
	)
}

func fmtTime(ts time.Time) string {
	if ts.IsZero() {
		return "00:00:00.000"
	}
	return ts.UTC().Format(testLayout)
}

func logResults(t *testing.T, tag string, wm time.Time, rs []WindowResult) {
	t.Helper()
	t.Logf("[%s] watermark=%s 封窗 %d 个:", tag, fmtTime(wm), len(rs))
	for _, r := range rs {
		t.Logf("    window=[%s,%s) count=%d sum=%.2f min=%.2f max=%.2f qty=%d first=%s@%.2f last=%s@%.2f",
			fmtTime(r.Start), fmtTime(r.End), r.EventCount, r.SumPrice, r.MinPrice, r.MaxPrice, r.SumQty,
			r.FirstEventID, r.FirstPrice, r.LastEventID, r.LastPrice)
	}
}

func approxEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

// TestOutOfOrderWindowDeterministic：乱序提交，封窗统计必须确定且可复现，
// first/last 由 (事件时间, ID) 决定，与到达顺序无关。
func TestOutOfOrderWindowDeterministic(t *testing.T) {
	build := func() *Aggregator {
		a, err := New(Config{WindowSize: 10 * time.Second}, testSchemaV1())
		if err != nil {
			t.Fatal(err)
		}
		// 故意乱序：同一 10s 窗口内事件不按时间到达。
		events := []Event{
			mkEvent("e3", 1, "00:00:08.000", 103, 3, nil),
			mkEvent("e1", 1, "00:00:01.000", 101, 1, nil),
			mkEvent("e2", 1, "00:00:05.000", 102, 2, nil),
			mkEvent("e5", 1, "00:00:15.000", 201, 5, nil), // 下一窗口
		}
		for _, ev := range events {
			logDecision(t, "乱序", a.Submit(ev))
		}
		wm, fresh, err := a.AdvanceWatermark(mkTime("00:00:20.000"))
		if err != nil {
			t.Fatal(err)
		}
		logResults(t, "乱序", wm, fresh)
		return a
	}

	a := build()
	rs := a.Results()
	if len(rs) != 2 {
		t.Fatalf("期望 2 个封窗，实际 %d", len(rs))
	}
	r0 := rs[0]
	if r0.EventCount != 3 ||
		!approxEqual(r0.SumPrice, 306) ||
		!approxEqual(r0.MinPrice, 101) ||
		!approxEqual(r0.MaxPrice, 103) ||
		r0.SumQty != 6 ||
		r0.FirstEventID != "e1" ||
		r0.LastEventID != "e3" {
		t.Fatalf("窗口0统计不符合预期: %+v", r0)
	}
	r1 := rs[1]
	if r1.EventCount != 1 || r1.FirstEventID != "e5" || r1.LastEventID != "e5" {
		t.Fatalf("窗口1统计不符合预期: %+v", r1)
	}

	// 同序重放一次，结果必须完全一致。
	a2 := build()
	rs2 := a2.Results()
	if len(rs2) != len(rs) {
		t.Fatalf("重放窗口数不一致: %d vs %d", len(rs2), len(rs))
	}
	for i := range rs {
		if !sameResult(rs[i], rs2[i]) {
			t.Fatalf("重放结果不一致:\n%+v\n%+v", rs[i], rs2[i])
		}
	}
	t.Logf("重放结论一致：%d 个窗口统计完全相同", len(rs))
}

func sameResult(a, b WindowResult) bool {
	return a.Start.Equal(b.Start) && a.End.Equal(b.End) &&
		a.EventCount == b.EventCount &&
		approxEqual(a.SumPrice, b.SumPrice) &&
		approxEqual(a.MinPrice, b.MinPrice) &&
		approxEqual(a.MaxPrice, b.MaxPrice) &&
		a.SumQty == b.SumQty &&
		approxEqual(a.FirstPrice, b.FirstPrice) &&
		approxEqual(a.LastPrice, b.LastPrice) &&
		a.FirstEventID == b.FirstEventID &&
		a.LastEventID == b.LastEventID
}

// TestDeduplicationAndConflict：重复投递幂等；同 ID 内容冲突必须被明确拒绝，
// 且两种失败原因可区分；原始统计不得被重复计数或被覆盖。
func TestDeduplicationAndConflict(t *testing.T) {
	a, err := New(Config{WindowSize: 10 * time.Second}, testSchemaV1())
	if err != nil {
		t.Fatal(err)
	}

	original := mkEvent("e1", 1, "00:00:01.000", 100, 10, nil)
	logDecision(t, "去重", a.Submit(original))

	// 完全相同的重投（包括 map 构造顺序不同）：幂等忽略。
	dup := Event{
		ID:            "e1",
		SchemaVersion: 1,
		EventTime:     mkTime("00:00:01.000"),
		Payload:       map[string]any{"qty": int64(10), "price": 100.0}, // key 顺序不同
	}
	d := a.Submit(dup)
	logDecision(t, "去重", d)
	if d.Reason != ReasonDuplicate {
		t.Fatalf("期望 DUPLICATE_IGNORED，实际 %s", d.Reason)
	}

	// 同 ID 但价格不同：冲突拒绝，且不得覆盖原值。
	conflict := mkEvent("e1", 1, "00:00:01.000", 999, 10, nil)
	d = a.Submit(conflict)
	logDecision(t, "去重", d)
	if d.Reason != ReasonConflict {
		t.Fatalf("期望 CONFLICT_REJECTED，实际 %s", d.Reason)
	}

	// 同 ID 但事件时间不同：同样是冲突（规范化内容包含事件时间）。
	conflictTime := mkEvent("e1", 1, "00:00:02.000", 100, 10, nil)
	d = a.Submit(conflictTime)
	logDecision(t, "去重", d)
	if d.Reason != ReasonConflict {
		t.Fatalf("时间戳冲突期望 CONFLICT_REJECTED，实际 %s", d.Reason)
	}

	// 冲突之后再次原样投递：仍然幂等（首次版本始终是权威）。
	d = a.Submit(dup)
	logDecision(t, "去重", d)
	if d.Reason != ReasonDuplicate {
		t.Fatalf("冲突后重投期望 DUPLICATE_IGNORED，实际 %s", d.Reason)
	}

	wm, fresh, _ := a.AdvanceWatermark(mkTime("00:00:10.000"))
	logResults(t, "去重", wm, fresh)

	if len(fresh) != 1 {
		t.Fatalf("期望 1 个封窗，实际 %d", len(fresh))
	}
	r := fresh[0]
	if r.EventCount != 1 || !approxEqual(r.SumPrice, 100) || r.SumQty != 10 {
		t.Fatalf("统计被重复计数或覆盖: %+v", r)
	}
	c := a.Counters()
	if c[ReasonAccepted] != 1 || c[ReasonDuplicate] != 2 || c[ReasonConflict] != 2 {
		t.Fatalf("计数不符合预期: %v", c)
	}
	t.Logf("计数快照: accepted=%d duplicate=%d conflict=%d",
		c[ReasonAccepted], c[ReasonDuplicate], c[ReasonConflict])
}

// TestLateEventsAndWatermark：水位单调、封窗不可改写、迟到事件按策略处理、
// 迟到事件的重复投递仍然幂等、重复/冲突先于迟到判定。
func TestLateEventsAndWatermark(t *testing.T) {
	for _, policy := range []LatePolicy{LateDrop, LateSideOutput} {
		name := "Drop"
		lateReason := ReasonLateDropped
		if policy == LateSideOutput {
			name = "SideOutput"
			lateReason = ReasonLateSideOutput
		}
		t.Run(name, func(t *testing.T) {
			// AllowedLateness=2s：wm 推进到 :12 时，[00:00:00,00:00:10) 才封死。
			a, err := New(Config{
				WindowSize:      10 * time.Second,
				AllowedLateness: 2 * time.Second,
				LatePolicy:      policy,
			}, testSchemaV1())
			if err != nil {
				t.Fatal(err)
			}

			logDecision(t, name, a.Submit(mkEvent("e1", 1, "00:00:01.000", 100, 1, nil)))
			logDecision(t, name, a.Submit(mkEvent("e2", 1, "00:00:09.000", 110, 1, nil)))

			// wm=:11，cutoff=:09，窗口末端 :10 > :09，尚未封死。
			if wm, fresh, err := a.AdvanceWatermark(mkTime("00:00:11.000")); err != nil || len(fresh) != 0 {
				t.Fatalf("宽限期内不应封窗: wm=%s fresh=%d err=%v", wm, len(fresh), err)
			} else {
				t.Logf("[%s] watermark=%s 宽限期内窗口仍开放", name, fmtTime(wm))
			}

			// wm=:12，cutoff=:10，窗口封死。
			wm, fresh, err := a.AdvanceWatermark(mkTime("00:00:12.000"))
			if err != nil {
				t.Fatal(err)
			}
			logResults(t, name, wm, fresh)
			if len(fresh) != 1 || fresh[0].EventCount != 2 {
				t.Fatalf("封窗结果异常: %+v", fresh)
			}

			// 迟到事件进入已封窗口。
			late := mkEvent("late1", 1, "00:00:02.000", 500, 9, nil)
			d := a.Submit(late)
			logDecision(t, name, d)
			if d.Reason != lateReason {
				t.Fatalf("期望 %s，实际 %s", lateReason, d.Reason)
			}
			if d.WindowEnd != mkTime("00:00:10.000") {
				t.Fatalf("迟到判定窗口末端异常: %s", fmtTime(d.WindowEnd))
			}

			// 同一迟到事件再投一次：仍是幂等重复，而不是第二次迟到。
			d = a.Submit(late)
			logDecision(t, name, d)
			if d.Reason != ReasonDuplicate {
				t.Fatalf("迟到事件重投期望 DUPLICATE_IGNORED，实际 %s", d.Reason)
			}

			// 同 ID 冲突内容：明确拒绝为冲突，而不是迟到。
			d = a.Submit(mkEvent("late1", 1, "00:00:02.000", 501, 9, nil))
			logDecision(t, name, d)
			if d.Reason != ReasonConflict {
				t.Fatalf("期望 CONFLICT_REJECTED，实际 %s", d.Reason)
			}

			// 已封窗结果不得被改写。
			rs := a.Results()
			if len(rs) != 1 || rs[0].EventCount != 2 || !approxEqual(rs[0].SumPrice, 210) {
				t.Fatalf("封窗结果被迟到数据改写: %+v", rs)
			}

			// 旁路策略：事件留存；丢弃策略：不留存。
			side := a.SideOutput()
			if policy == LateSideOutput {
				if len(side) != 1 || side[0].ID != "late1" {
					t.Fatalf("期望旁路 1 条，实际 %d", len(side))
				}
				t.Logf("[%s] 旁路留存迟到事件 id=%s eventTime=%s", name, side[0].ID, fmtTime(side[0].EventTime))
			} else if len(side) != 0 {
				t.Fatalf("Drop 策略不应有旁路事件，实际 %d", len(side))
			}

			// 水位回拨被拒绝，状态不变。
			if _, _, err := a.AdvanceWatermark(mkTime("00:00:11.000")); err == nil {
				t.Fatal("水位回拨应被拒绝")
			} else {
				t.Logf("[%s] %s", name, err)
			}
			if a.Watermark() != mkTime("00:00:12.000") {
				t.Fatalf("回拨后水位不应变化: %s", fmtTime(a.Watermark()))
			}
			// 同值推进允许但无变化。
			wm2, fresh2, err := a.AdvanceWatermark(mkTime("00:00:12.000"))
			if err != nil || len(fresh2) != 0 || wm2 != wm {
				t.Fatalf("同值推进应无变化: wm=%s fresh=%d err=%v", fmtTime(wm2), len(fresh2), err)
			}
		})
	}
}

// TestConcurrentWriters：多个 goroutine 并发提交（含乱序、重复），
// 最终统计必须与事件集合本身对应：唯一计数、窗口结果确定。
func TestConcurrentWriters(t *testing.T) {
	a, err := New(Config{WindowSize: 10 * time.Second}, testSchemaV1())
	if err != nil {
		t.Fatal(err)
	}

	// 20 个唯一事件 + 每个事件额外重复投递 2 次，分给 6 个 goroutine。
	var unique []Event
	prices := []float64{}
	var qtySum int64
	for i := 0; i < 20; i++ {
		sec := i % 30 // 跨 3 个窗口
		id := fmtID(i)
		ev := mkEvent(id, 1, fmt.Sprintf("00:00:%02d.500", sec), float64(100+i), int64(i+1), nil)
		unique = append(unique, ev)
		prices = append(prices, float64(100+i))
		qtySum += int64(i + 1)
	}

	var jobs []Event
	for rep := 0; rep < 3; rep++ {
		// 每轮打乱顺序，模拟乱序到达。
		jobs = append(jobs, shuffledCopy(unique, rep)...)
	}

	decisions := make([]Decision, len(jobs))
	chunk := (len(jobs) + 5) / 6
	done := make(chan struct{}, 6)
	for g := 0; g < 6; g++ {
		lo := g * chunk
		hi := lo + chunk
		if hi > len(jobs) {
			hi = len(jobs)
		}
		go func(lo, hi int) {
			for i := lo; i < hi; i++ {
				decisions[i] = a.Submit(jobs[i])
			}
			done <- struct{}{}
		}(lo, hi)
	}
	for g := 0; g < 6; g++ {
		<-done
	}

	accepted, duplicate := 0, 0
	for _, d := range decisions {
		switch d.Reason {
		case ReasonAccepted:
			accepted++
		case ReasonDuplicate:
			duplicate++
		default:
			t.Fatalf("出现非预期判定: %s (%s)", d.Reason, d.Detail)
		}
	}
	if accepted != 20 || duplicate != 40 {
		t.Fatalf("并发计数异常: accepted=%d duplicate=%d", accepted, duplicate)
	}
	t.Logf("[并发] 提交=%d accepted=%d duplicate=%d，无冲突", len(jobs), accepted, duplicate)

	wm, fresh, _ := a.AdvanceWatermark(mkTime("00:00:40.500"))
	logResults(t, "并发", wm, fresh)

	rs := a.Results()
	if len(rs) != 2 {
		t.Fatalf("期望 2 个封窗，实际 %d", len(rs))
	}
	totalCount := 0
	for _, r := range rs {
		totalCount += r.EventCount
	}
	if totalCount != 20 {
		t.Fatalf("唯一事件总数应为 20，实际 %d", totalCount)
	}
	// 窗口归属与顺序：sec 0-9 -> w0 (10个), 10-19 -> w1 (10个)
	if rs[0].EventCount != 10 || rs[1].EventCount != 10 {
		t.Fatalf("窗口归属异常: %d %d", rs[0].EventCount, rs[1].EventCount)
	}
	// 校验 w0 的 first/last（按事件时间，同时间再按 ID）。
	if rs[0].FirstEventID != "e00" || rs[0].LastEventID != "e09" {
		t.Fatalf("w0 first/last 异常: %s %s", rs[0].FirstEventID, rs[0].LastEventID)
	}
	var sumPrice float64
	var sumQty int64
	for _, r := range rs {
		sumPrice += r.SumPrice
		sumQty += r.SumQty
	}
	var expectSum float64
	for _, p := range prices {
		expectSum += p
	}
	if !approxEqual(sumPrice, expectSum) || sumQty != qtySum {
		t.Fatalf("总量异常: sumPrice=%f (want %f) sumQty=%d (want %d)", sumPrice, expectSum, sumQty, qtySum)
	}
	t.Logf("[并发] 总量 sumPrice=%.2f sumQty=%d，与事件集合一致", sumPrice, sumQty)
}

// TestSchemaEvolutionAndReplay：组合场景 —— 新增 optional 字段后，
// 旧版本事件与新版本事件混着重放，结论必须与首次处理一致；
// 不兼容演进在构造期就被拒绝。
func TestSchemaEvolutionAndReplay(t *testing.T) {
	v1 := Schema{Name: "quote", Version: 1, Fields: []FieldDef{
		{Name: "price", Type: FieldFloat64},
		{Name: "qty", Type: FieldInt64},
	}}
	v2 := Schema{Name: "quote", Version: 2, Fields: []FieldDef{
		{Name: "price", Type: FieldFloat64},
		{Name: "qty", Type: FieldInt64},
		{Name: "venue", Type: FieldString, Optional: true},
	}}

	build := func() (*Aggregator, []Event) {
		a, err := New(Config{WindowSize: 10 * time.Second}, v1, v2)
		if err != nil {
			t.Fatal(err)
		}
		events := []Event{
			mkEvent("old1", 1, "00:00:01.000", 100, 1, nil),
			mkEvent("new1", 2, "00:00:04.000", 102, 2, map[string]any{"venue": "SSE"}),
			mkEvent("old2", 1, "00:00:08.000", 99, 1, nil),
		}
		// 新版本事件缺失 optional 字段也合法。
		events = append(events, mkEvent("new2", 2, "00:00:12.000", 105, 1, nil))
		for _, ev := range events {
			logDecision(t, "演进", a.Submit(ev))
		}
		wm, fresh, err := a.AdvanceWatermark(mkTime("00:00:20.000"))
		if err != nil {
			t.Fatal(err)
		}
		logResults(t, "演进", wm, fresh)
		return a, events
	}

	a1, events := build()

	// 不兼容演进（v2 -> v3 把 qty 收窄类型 + 删除 price 等）必须被拒绝。
	bad := Schema{Name: "quote", Version: 3, Fields: []FieldDef{
		{Name: "price", Type: FieldString}, // float64 -> string 不允许
		{Name: "qty", Type: FieldInt64},
		{Name: "venue", Type: FieldString, Optional: true},
	}}
	if err := mustRegistry(t).Register(bad); err == nil {
		t.Fatal("不兼容 schema 变更必须被拒绝")
	} else {
		t.Logf("符合预期地拒绝不兼容演进: %v", err)
	}

	// 用完全相同的事件流重放到一个全新实例：结论逐字节一致。
	a2, _ := build()
	r1, r2 := a1.Results(), a2.Results()
	if len(r1) != len(r2) {
		t.Fatalf("重放窗口数不一致")
	}
	for i := range r1 {
		if !sameResult(r1[i], r2[i]) {
			t.Fatalf("结构演进下重放结论不一致:\n%+v\n%+v", r1[i], r2[i])
		}
	}
	if c1, c2 := a1.Counters(), a2.Counters(); c1[ReasonAccepted] != c2[ReasonAccepted] ||
		c1[ReasonAccepted] != len(events) {
		t.Fatalf("重放计数不一致: %v vs %v", c1, c2)
	}
	t.Logf("[演进] %d 条混合版本事件重放结论一致（旧版无 optional 字段亦可处理）", len(events))
}

func mustRegistry(t *testing.T) *Registry {
	t.Helper()
	r := NewRegistry()
	if err := r.Register(Schema{Name: "quote", Version: 1, Fields: []FieldDef{
		{Name: "price", Type: FieldFloat64},
		{Name: "qty", Type: FieldInt64},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(Schema{Name: "quote", Version: 2, Fields: []FieldDef{
		{Name: "price", Type: FieldFloat64},
		{Name: "qty", Type: FieldInt64},
		{Name: "venue", Type: FieldString, Optional: true},
	}}); err != nil {
		t.Fatal(err)
	}
	return r
}

// TestCombinedChaos：组合 —— 并发写入 + 乱序 + 重复 + 迟到 + 冲突，
// 所有判定原因必须可区分、计数闭环、封窗结果确定。
func TestCombinedChaos(t *testing.T) {
	a, err := New(Config{WindowSize: 10 * time.Second, AllowedLateness: time.Second}, testSchemaV1())
	if err != nil {
		t.Fatal(err)
	}

	good := []Event{
		mkEvent("g1", 1, "00:00:01.000", 10, 1, nil),
		mkEvent("g2", 1, "00:00:05.000", 20, 2, nil),
		mkEvent("g3", 1, "00:00:12.000", 30, 3, nil),
	}
	// 先正常接受 g1/g2，再推进水位封死第一窗（wm=:11, cutoff=:10）。
	for _, ev := range good[:2] {
		logDecision(t, "混沌", a.Submit(ev))
	}
	wm, fresh, _ := a.AdvanceWatermark(mkTime("00:00:11.000"))
	logResults(t, "混沌", wm, fresh)

	type job struct {
		ev      Event
		want    ReasonCode
		comment string
	}
	jobs := []job{
		{good[0], ReasonDuplicate, "g1 原样重投"},
		{good[2], ReasonAccepted, "g3 进入第二窗"},
		{mkEvent("g1", 1, "00:00:01.000", 11, 1, nil), ReasonConflict, "g1 改价冲突"},
		{mkEvent("L1", 1, "00:00:02.000", 99, 1, nil), ReasonLateDropped, "L1 迟到进第一窗"},
		{mkEvent("L1", 1, "00:00:02.000", 99, 1, nil), ReasonDuplicate, "L1 迟到事件重投"},
		{mkEvent("g2", 1, "00:00:05.000", 20, 2, nil), ReasonDuplicate, "g2 乱序重投"},
		{mkEvent("g4", 1, "00:00:19.000", 40, 4, nil), ReasonAccepted, "g4 仍在第二窗"},
	}

	errCh := make(chan string, len(jobs))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, j := range jobs { // 单 goroutine 乱序投放即足够制造交错；再用并发提交 g2 制造竞争
			d := a.Submit(j.ev)
			logDecision(t, "混沌", d)
			if d.Reason != j.want {
				errCh <- fmt.Sprintf("%s: 期望 %s 实际 %s", j.comment, j.want, d.Reason)
				return
			}
		}
	}()
	// 并发风暴：多个 goroutine 同时重复提交 g2 与 g3，accepted 数绝不增加。
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				d := a.Submit(good[(g+i)%2])
				if d.Reason != ReasonDuplicate {
					errCh <- fmt.Sprintf("并发重投期望 DUPLICATE，实际 %s", d.Reason)
					return
				}
			}
		}(g)
	}
	<-done
	wg.Wait()
	close(errCh)
	for msg := range errCh {
		t.Fatal(msg)
	}

	wm, fresh, _ = a.AdvanceWatermark(mkTime("00:00:21.000"))
	logResults(t, "混沌", wm, fresh)

	rs := a.Results()
	if len(rs) != 2 {
		t.Fatalf("期望 2 窗，实际 %d", len(rs))
	}
	if rs[0].EventCount != 2 || !approxEqual(rs[0].SumPrice, 30) {
		t.Fatalf("第一窗被改写: %+v", rs[0])
	}
	if rs[1].EventCount != 2 || !approxEqual(rs[1].SumPrice, 70) {
		t.Fatalf("第二窗统计异常: %+v", rs[1])
	}
	c := a.Counters()
	t.Logf("[混沌] 判定计数: accepted=%d duplicate=%d conflict=%d lateDropped=%d regress=%d",
		c[ReasonAccepted], c[ReasonDuplicate], c[ReasonConflict], c[ReasonLateDropped], c[ReasonWatermarkRegress])
	if c[ReasonAccepted] != 4 || c[ReasonConflict] != 1 || c[ReasonLateDropped] != 1 {
		t.Fatalf("计数闭环异常: %v", c)
	}
	if c[ReasonDuplicate] < 3 {
		t.Fatalf("重复计数异常: %d", c[ReasonDuplicate])
	}
}
