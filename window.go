package aggregator

import (
	"sort"
	"time"
)

// WindowResult 是一个已封窗 [Start, End) 的可复现统计结果。
// FirstPrice/LastPrice 按 (事件时间, 事件ID) 排序确定，与到达顺序、
// 并发调度无关，因此任意重放都得到相同结论。
type WindowResult struct {
	Start        time.Time
	End          time.Time
	EventCount   int
	SumPrice     float64
	MinPrice     float64
	MaxPrice     float64
	SumQty       int64
	FirstPrice   float64
	LastPrice    float64
	FirstEventID string
	LastEventID  string
}

type windowEntry struct {
	eventTime time.Time
	id        string
	price     float64
	qty       int64
}

type window struct {
	start   time.Time
	end     time.Time
	entries []windowEntry
}

type windowAssigner struct {
	size time.Duration
}

func newWindowAssigner(size time.Duration) *windowAssigner {
	return &windowAssigner{size: size}
}

// assign 返回事件时间 t 所属的对齐滚动窗口 [start, end)。
// 以 Unix 时间为基准对齐，避免时区/截断实现差异。
func (a *windowAssigner) assign(t time.Time) (start, end time.Time) {
	u := t.UnixNano()
	size := int64(a.size)
	boundary := u - mod64(u, size)
	if u < 0 && u%size != 0 {
		// Go 的 % 对负数返回负余数；这里行情时间均在 epoch 之后，做一次防御性修正。
		boundary -= size
	}
	start = time.Unix(0, boundary).UTC()
	end = start.Add(a.size)
	return start, end
}

func mod64(a, b int64) int64 { return a % b }

func (w *window) add(e windowEntry) {
	w.entries = append(w.entries, e)
}

// seal 封窗并产出确定性统计。聚合器保证不会对同一窗口封两次。
func (w *window) seal() WindowResult {
	res := WindowResult{Start: w.start, End: w.end}
	if len(w.entries) == 0 {
		return res
	}
	// 稳定排序：事件时间为主序，事件 ID 为次序，消除并发到达顺序的影响。
	sorted := make([]windowEntry, len(w.entries))
	copy(sorted, w.entries)
	sort.SliceStable(sorted, func(i, j int) bool {
		if !sorted[i].eventTime.Equal(sorted[j].eventTime) {
			return sorted[i].eventTime.Before(sorted[j].eventTime)
		}
		return sorted[i].id < sorted[j].id
	})

	res.EventCount = len(sorted)
	res.SumPrice = sorted[0].price
	res.MinPrice = sorted[0].price
	res.MaxPrice = sorted[0].price
	res.SumQty = sorted[0].qty
	res.FirstPrice = sorted[0].price
	res.FirstEventID = sorted[0].id
	res.LastPrice = sorted[len(sorted)-1].price
	res.LastEventID = sorted[len(sorted)-1].id
	for i := 1; i < len(sorted); i++ {
		p := sorted[i].price
		res.SumPrice += p
		if p < res.MinPrice {
			res.MinPrice = p
		}
		if p > res.MaxPrice {
			res.MaxPrice = p
		}
		res.SumQty += sorted[i].qty
	}
	return res
}
