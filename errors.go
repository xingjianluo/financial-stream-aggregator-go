package aggregator

import "fmt"

// ReasonCode 是每次提交/推进的可区分判定依据，用于日志与统计。
type ReasonCode string

const (
	ReasonAccepted         ReasonCode = "ACCEPTED"             // 事件被接受并计入窗口
	ReasonDuplicate        ReasonCode = "DUPLICATE_IGNORED"    // 同 ID 且内容一致：幂等忽略
	ReasonConflict         ReasonCode = "CONFLICT_REJECTED"    // 同 ID 但内容冲突：明确拒绝
	ReasonLateDropped      ReasonCode = "LATE_DROPPED"         // 迟到事件：按 LateDrop 策略丢弃
	ReasonLateSideOutput   ReasonCode = "LATE_SIDE_OUTPUT"     // 迟到事件：按 LateSideOutput 旁路
	ReasonSchemaIncompat   ReasonCode = "SCHEMA_INCOMPATIBLE"  // schema 版本不兼容
	ReasonUnknownSchema    ReasonCode = "UNKNOWN_SCHEMA"       // 提交了未注册的 schema 版本
	ReasonInvalidPayload   ReasonCode = "INVALID_PAYLOAD"      // payload 不符合 schema
	ReasonWatermarkRegress ReasonCode = "WATERMARK_REGRESSION" // 水位回拨被拒绝
)

// DuplicateError 表示同一逻辑事件（ID 相同且规范化内容一致）被重复投递。
// 重复投递是幂等的：不算错误计数进业务统计，调用方可据此区分"未做事"。
type DuplicateError struct{ EventID string }

func (e *DuplicateError) Error() string {
	return fmt.Sprintf("duplicate event ignored: id=%q", e.EventID)
}

// ConflictError 表示同一 ID 的事件内容（事件时间/schema/负载）与首次投递不一致。
// 系统绝不静默覆盖后到版本，而是明确拒绝。
type ConflictError struct {
	EventID      string
	ExistingHash string
	GotHash      string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("conflicting event rejected: id=%q existing=%s got=%s",
		e.EventID, e.ExistingHash, e.GotHash)
}

// LateEventError 表示事件到达时其所属窗口 [start,end) 已被水位封死。
type LateEventError struct {
	EventID   string
	EventTime string
	Watermark string
	WindowEnd string
}

func (e *LateEventError) Error() string {
	return fmt.Sprintf("late event: id=%q eventTime=%s watermark=%s windowEnd=%s",
		e.EventID, e.EventTime, e.Watermark, e.WindowEnd)
}

// SchemaIncompatibleError 表示新版本 schema 与上一版本存在不兼容变更。
type SchemaIncompatibleError struct {
	Name   string
	From   int
	To     int
	Detail string
}

func (e *SchemaIncompatibleError) Error() string {
	return fmt.Sprintf("incompatible schema %s v%d->v%d: %s", e.Name, e.From, e.To, e.Detail)
}

// WatermarkRegressionError 表示试图把水位向更早的时间拨。
type WatermarkRegressionError struct {
	From string
	To   string
}

func (e *WatermarkRegressionError) Error() string {
	return fmt.Sprintf("watermark regression rejected: from=%s to=%s", e.From, e.To)
}
