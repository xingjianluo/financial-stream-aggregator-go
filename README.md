# 本地行情流式聚合管线（Go）

一个无外部依赖、可嵌入、线程安全的本地流式聚合管线：多个并发写入者按**事件时间**
提交增量行情，输出**可复现**的滚动窗口统计；在乱序、重复、迟到数据以及结构演进下，
每一条事件都有唯一、可区分、可解释的判定结论。

## 快速开始

```go
v1 := aggregator.Schema{Name: "quote", Version: 1, Fields: []aggregator.FieldDef{
    {Name: "price", Type: aggregator.FieldFloat64}, // 必填
    {Name: "qty", Type: aggregator.FieldInt64},     // 必填
}}
agg, _ := aggregator.New(aggregator.Config{
    WindowSize:      10 * time.Second, // 滚动窗口长度
    AllowedLateness: 2 * time.Second,  // 封窗宽限
    LatePolicy:      aggregator.LateSideOutput,
}, v1)

d := agg.Submit(aggregator.Event{
    ID: "q-001", SchemaVersion: 1,
    EventTime: ts,
    Payload:   map[string]any{"price": 101.5, "qty": int64(3)},
})
// d.Reason: ACCEPTED / DUPLICATE_IGNORED / CONFLICT_REJECTED /
//           LATE_DROPPED / LATE_SIDE_OUTPUT / UNKNOWN_SCHEMA / INVALID_PAYLOAD

wm, sealed, err := agg.AdvanceWatermark(ts.Add(12 * time.Second))
_ = agg.Results()     // 已封窗结果（按窗口起点升序）
_ = agg.SideOutput()  // 被旁路留存的迟到事件
_ = agg.Counters()    // 各判定原因的计数
```

## 判定原因（ReasonCode）

| ReasonCode | 含义 | 是否计入窗口 |
|---|---|---|
| `ACCEPTED` | 通过校验，计入其所属窗口 | 是 |
| `DUPLICATE_IGNORED` | 同 ID 且规范化内容完全一致，幂等忽略（重复投递） | 否 |
| `CONFLICT_REJECTED` | 同 ID 但内容/事件时间/schema 版本不一致，**明确拒绝、不覆盖** | 否 |
| `LATE_DROPPED` | 到达时窗口已封死，按 `LateDrop` 策略丢弃 | 否（计数） |
| `LATE_SIDE_OUTPUT` | 到达时窗口已封死，主统计不计，事件副本旁路留存 | 否（留存） |
| `UNKNOWN_SCHEMA` | 提交了未注册的 schema 版本 | 否 |
| `INVALID_PAYLOAD` | payload 缺必填字段、类型不符或含未知字段 | 否 |
| `WATERMARK_REGRESSION` | 试图把水位向更早的时间拨，状态不变 | — |

提交时的判定顺序（先到先得，结论稳定）：

1. **结构校验**：schema 版本存在性 + payload 字段/类型校验（未知字段也拒绝）；
2. **身份判定**：按 `ID` 查规范化内容哈希：哈希相同 → 重复；哈希不同 → 冲突；
3. **时间判定**：窗口已封死 → 迟到（按策略处理）；
4. 否则接受并计入窗口。

身份登记先于迟到处理：因此**迟到事件的重复投递仍判为重复，迟到事件的篡改仍判为冲突**，
不会出现"先迟到、后重复"的漂移结论。

## 窗口与水位语义

- 滚动窗口按 Unix 时间对齐，左闭右开：`[start, start+WindowSize)`。
- 统计指标：`count / sum(price) / min / max / sum(qty) / first / last`。
  `first` 与 `last` 由 **(事件时间, 事件 ID)** 全序决定，与线程调度和到达顺序无关，
  因此任意并发交错、任意重放顺序都产出完全相同的结果。
- 水位（watermark）**单调不减**：`AdvanceWatermark(t)` 仅在 `t > 当前水位` 时前进；
  `t == 当前水位` 为无变化的空操作；`t < 当前水位` 返回 `WatermarkRegressionError`
  且状态完全不变。
- 封窗条件：

  ```
  cutoff = watermark - AllowedLateness
  窗口封死  <=>  windowEnd <= cutoff
  ```

  即 `AllowedLateness=0` 时水位一过窗口末端立即封死；配置正数宽限可容忍一定乱序。
- **封窗不可变**：窗口封死后，`Results()` 中的统计永远不会被后续事件修改；
  迟到事件进入 `LATE_DROPPED`（仅计数）或 `LATE_SIDE_OUTPUT`（旁路副本，可审计）。

## 幂等去重与冲突

- 事件身份是 `ID`；内容由 `schema 版本 | UTC 事件时间(纳秒) | 按 key 排序的 payload`
  规范化后做 SHA-256 摘要（见 `canonicalContent`），与 JSON 字段书写顺序无关。
- 同一 ID 首次出现的内容即**权威版本**：
  - 后续内容哈希相同 → 幂等忽略，统计不重复计数；
  - 哈希不同 → 返回 `ConflictError`（附带 existing/got 两个哈希），**绝不静默覆盖**。
- 事件时间是内容的一部分：同 ID 改时间戳同样构成冲突。
- 数字在规范化阶段归一为 `int64`/`float64`，避免 `1` 与 `1.0` 的表示歧义。

## 结构（schema）演进兼容范围

版本号必须从 **v1 开始严格 +1**（禁止跳号/重号）。相邻版本仅允许以下变更：

- **新增字段**：必须声明为 `optional`（旧数据没有该字段仍可重放）；
- **类型拓宽**：仅允许 `int64 -> float64`（旧整数可精确表示；提交时 int 值也自动拓宽）；
- **可选性放宽**：`required -> optional`。

以下变更一律以 `SchemaIncompatibleError` 拒绝，且错误信息指出具体字段与原因：

- 删除字段 / 字段改名（表现为先删后增）；
- 新增 `required` 字段；
- `optional -> required`（收严）；
- 任何类型收窄（如 `float64 -> int64`）或其它类型互转（如 `string -> bool`）。

兼容性是**逐相邻版本**校验的，v3 只需对 v2 兼容；v1 事件按 v1 结构永久可解析，
因此新旧版本事件可以混合提交，历史数据重放与首次处理结论一致。

## 可复现性说明

- 统计仅依赖事件时间与事件 ID 排序，不使用墙上时钟；
- 规范化内容与窗口对齐都是确定性的；
- 全部状态变更在单把互斥锁内完成，`go test -race` 无告警；
- 测试包含"全新实例同流重放，逐字段对比结果"的断言（见
  `TestOutOfOrderWindowDeterministic`、`TestSchemaEvolutionAndReplay`）。

## 本地验证

```bash
go build ./...
go vet ./...
gofmt -l .      # 应无输出
go test -v ./...        # 详细日志：窗口、水位、事件时间与每条判定依据
go test -race ./...     # 竞态检测
```

或直接 `make test`。测试日志中每条事件打印 `eventTime / watermark /
window=[start,end) / reason / detail`，封窗时打印完整统计，计数闭环通过
`Counters()` 快照断言。
