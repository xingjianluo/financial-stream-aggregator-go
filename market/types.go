// Package market defines the domain types of the local market-data stream
// aggregation pipeline: logical events, tumbling windows and watermarks.
package market

// TypeKind enumerates the scalar field types understood by the schema registry.
type TypeKind string

const (
	TypeInt64   TypeKind = "int64"
	TypeFloat64 TypeKind = "float64"
	TypeString  TypeKind = "string"
	TypeBool    TypeKind = "bool"
)

// RejectCause is the *distinguishable* reason an event or a schema change was
// rejected. Every unsuccessful Submit carries one of these values.
type RejectCause string

const (
	CauseNone RejectCause = "" // accepted path

	// Schema / structural compatibility causes.
	CauseSchemaUnknownVersion    RejectCause = "schema_unknown_version"
	CauseSchemaMissingField      RejectCause = "schema_missing_field"
	CauseSchemaExtraField        RejectCause = "schema_extra_field"
	CauseSchemaFieldTypeMismatch RejectCause = "schema_field_type_mismatch"
	CauseSchemaTypeIncompatible  RejectCause = "schema_type_incompatible"
	CauseSchemaRemovingField     RejectCause = "schema_removing_field"
	CauseSchemaDuplicateVersion  RejectCause = "schema_duplicate_version"
	CauseSchemaVersionGap        RejectCause = "schema_version_gap"
	CauseSchemaVersionRegressed  RejectCause = "schema_version_regressed"
	CauseSchemaEmpty             RejectCause = "schema_empty"
	// Gray-upgrade-specific structural causes.
	CauseSchemaAddedFieldNoDefault RejectCause = "schema_added_field_without_default"
	CauseSchemaDefaultTypeMismatch RejectCause = "schema_default_type_mismatch"

	// Deduplication causes.
	CauseDuplicate RejectCause = "duplicate"
	CauseConflict  RejectCause = "conflict"
	// CauseCrossVersionConflict means the same logical id arrived from the two
	// gray-upgrade producers under different schema versions, and even after
	// default filling and numeric-widening normalization the payloads disagree
	// in price/quantity/new-field values. It is deliberately distinct from
	// CauseConflict (same-version disagreement) and from a cross-version
	// duplicate (a compatible re-delivery, Duplicate=true).
	CauseCrossVersionConflict RejectCause = "cross_version_conflict"

	// Lateness / window lifecycle causes.
	CauseWindowSealed RejectCause = "window_sealed"
	CauseLateDropped  RejectCause = "late_dropped"

	// Watermark lifecycle causes.
	CauseWatermarkRegression RejectCause = "watermark_regression_rejected"
)

// LatePolicy decides what happens to a late-but-into-open-window event:
//   - PolicyDrop  : the event is counted (it belongs to an open window), and a
//     copy is side-output for audit. On windows that are sealed it is rejected.
//   - PolicyReject: every late event is rejected, regardless of window state.
type LatePolicy string

const (
	PolicyDrop   LatePolicy = "drop_side_output"
	PolicyReject LatePolicy = "reject"
)

// Event is a logical incremental market-data event identified by EventID.
//
// Price and Quantity are expressed in raw integer units (e.g. 1 price unit =
// 1e-6 of the quoted currency); keeping them integer makes aggregation exact.
// Extra optional fields introduced by later schema versions live in Fields.
type Event struct {
	EventID   string         `json:"event_id"`
	Symbol    string         `json:"symbol"`
	EventTime int64          `json:"event_time"` // milliseconds since epoch
	SchemaVer int            `json:"schema_ver"`
	Price     int64          `json:"price"`
	Quantity  int64          `json:"quantity"`
	IsTrade   bool           `json:"is_trade"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// Window identifies a [start, end) tumbling window aligned at start.
type Window struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// SymbolCount is one symbol's contribution to a window's statistics.
type SymbolCount struct {
	Symbol     string `json:"symbol"`
	Count      int64  `json:"count"`
	Volume     int64  `json:"volume"`
	TradeCount int64  `json:"trade_count"`
	QuoteCount int64  `json:"quote_count"`
}

// WindowStats is the immutable snapshot of one sealed tumbling window.
type WindowStats struct {
	Window     Window        `json:"window"`
	EventCount int64         `json:"event_count"`
	Volume     int64         `json:"volume"` // sum of quantity across events
	PriceSum   int64         `json:"price_sum"`
	SealedAtWM int64         `json:"sealed_at_wm"`
	BySymbol   []SymbolCount `json:"by_symbol"` // sorted by Symbol for determinism
}

// Decision is the reproducible verdict for a single Submit call.
type Decision struct {
	EventID   string      `json:"event_id"`
	EventTime int64       `json:"event_time"`
	Window    *Window     `json:"window,omitempty"`
	Accepted  bool        `json:"accepted"`
	Reason    RejectCause `json:"reason"`
	Watermark int64       `json:"watermark"`
	Duplicate bool        `json:"duplicate"`
	ContentFP string      `json:"content_fp"`
	Detail    string      `json:"detail,omitempty"`

	// SchemaVer is the schema version declared by THIS delivery.
	SchemaVer int `json:"schema_ver,omitempty"`
	// CrossVersion marks a duplicate that arrived under a different schema
	// version than the first delivery of the same logical id.
	CrossVersion bool `json:"cross_version,omitempty"`
	// MatchedVer is the first delivery's schema version for a cross-version
	// duplicate; zero otherwise.
	MatchedVer int `json:"matched_ver,omitempty"`
	// Defaulted lists optional fields that were filled from their declared
	// defaults when the two gray producers were reconciled (audit trail).
	Defaulted []string `json:"defaulted,omitempty"`
}
