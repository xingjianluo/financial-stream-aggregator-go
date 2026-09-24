package aggregator

import "time"

// Watermark 是单调不减的事件时间水位。语义：
// 水位 wm 断言"事件时间 < wm 的事件原则上都已到达"（以调用方外部处理为准），
// 因此所有 end <= wm - AllowedLateness 的窗口可以封死。
// 水位只能前进或原地不动，回拨必须被明确拒绝。
type Watermark struct {
	ts time.Time
}

func NewWatermark(start time.Time) *Watermark {
	if start.IsZero() {
		start = time.Time{}
	}
	return &Watermark{ts: start}
}

// Advance 尝试把水位推进到 t。
// 返回值 advanced 表示水位是否真的发生变化。
// t < 当前水位时返回 WatermarkRegressionError，且状态不变。
func (w *Watermark) Advance(t time.Time) (advanced bool, err error) {
	if t.Before(w.ts) {
		return false, &WatermarkRegressionError{
			From: w.ts.UTC().Format(time.RFC3339Nano),
			To:   t.UTC().Format(time.RFC3339Nano),
		}
	}
	if t.Equal(w.ts) {
		return false, nil
	}
	w.ts = t
	return true, nil
}

func (w *Watermark) Value() time.Time { return w.ts }
