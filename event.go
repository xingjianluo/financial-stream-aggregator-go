package aggregator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// FieldType 是结构字段支持的标量类型。
type FieldType string

const (
	FieldInt64   FieldType = "int64"
	FieldFloat64 FieldType = "float64"
	FieldString  FieldType = "string"
	FieldBool    FieldType = "bool"
)

// FieldDef 描述一个字段。
type FieldDef struct {
	Name     string
	Type     FieldType
	Optional bool // 仅用于 payload 校验：optional 字段允许缺失
}

// Schema 描述某一版本的事件结构。
type Schema struct {
	Name    string // 逻辑结构名（如 quote）
	Version int    // 单调递增的版本号
	Fields  []FieldDef
}

// Event 是一条增量行情事件。Payload 中的值必须符合其注册 schema。
// 事件身份：ID（逻辑唯一）；幂等判定：ID + 规范化内容哈希。
type Event struct {
	ID            string
	SchemaVersion int
	EventTime     time.Time
	Payload       map[string]any
}

// canonicalContent 生成与输入顺序无关、时区无关的规范化字节序列。
// 组成：schema 版本、UTC 事件时间（纳秒精度）、按 key 排序后的 payload。
// 规范化后的 payload 中数值只能是 int64 或 float64（由 Registry 保证），
// 这样 1 与 1.0 的差别在规范化阶段就被显式确定，而不是泄漏到哈希里。
func canonicalContent(schemaVersion int, eventTime time.Time, payload map[string]any) ([]byte, error) {
	// encoding/json 对 map[string]any 按 key 字典序输出，保证确定性。
	pb, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("canonical payload: %w", err)
	}
	return []byte(fmt.Sprintf("v=%d|t=%s|p=%s",
		schemaVersion,
		eventTime.UTC().Truncate(time.Nanosecond).Format(time.RFC3339Nano),
		string(pb))), nil
}

// contentHash 返回规范化内容的 sha256 十六进制摘要。
func contentHash(schemaVersion int, eventTime time.Time, payload map[string]any) (string, error) {
	b, err := canonicalContent(schemaVersion, eventTime, payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
