# 本地行情流式聚合管线（Go）

多个并发写入者按**事件时间**（event time）提交增量行情，管线输出**可复现**的
滚动窗口（tumbling window）统计，并在乱序、重复、迟到数据以及结构演进下保持
结果唯一、判定可解释。

- 包：
  - `market`：领域类型（事件、窗口、窗口统计、判定结果、可区分拒绝原因）。
  - `pipeline`：聚合器（`Aggregator`）、结构注册表（`Registry`）、内容指纹。
- 仅依赖 Go 标准库；Go ≥ 1.25。

## 快速验证

```bash
go build ./...
go test ./...                 # 常规测试命令，全部用例本地可运行
go test -race ./...           # 并发安全检查
go test -v ./pipeline/        # 查看每个判定的窗口/水位/事件时间/判定依据日志
```

## 1. 滚动窗口与水位语义

- **滚动窗口**：窗口按事件时间对齐，长度由 `WithWindowSize(ms)` 配置（默认
  10000ms）。事件时间 `t` 归入窗口
  `[floor(t/size)*size, floor(t/size)*size + size)`，**半开区间**。窗口只在收到
  第一个属于它的事件时惰性创建；从未出现数据的窗口不会输出。
- **水位（watermark）**：由 `AdvanceWatermark(ts)` 显式推进，含义是“系统不再期待
  事件时间早于 `ts` 的数据”（注意：迟到事件仍按第 3 节策略单独判定）。
  - 水位**只能单调前进**：`ts <= 当前水位` 的推进被拒绝，返回判定原因
    `watermark_regression_rejected`，状态不发生任何变化。
- **封窗**：推进水位时，所有满足 `window.End <= 新水位` 的窗口被**恰好一次**
  封存，返回封存快照（按窗口起点排序）。封存结果：
  - 不可变：后续到达该窗口的事件一律以 `window_sealed` 拒绝，统计**永不改写**；
  - 可查询：`SealedWindows()` 返回全部历史封存窗口的深拷贝快照。
- 每个封存窗口输出：事件数、总量（quantity 求和）、价格和（price 求和，均为整数
  定点单位，聚合精确无浮点误差）、封存水位 `sealed_at_wm`，以及按 `symbol`
  **字典序排序**的明细分项（确定性输出的一部分）。

## 2. 幂等去重与冲突拒绝

事件以 `EventID` 作为逻辑身份。管线对每个已见 ID 保存其**内容指纹**
（SHA-256 截断，内容含 schema 版本、symbol、事件时间、价格、数量、买卖方向与
全部扩展字段，**不含 EventID 本身**）：

- **重复投递**（相同 ID、相同指纹）：回放**首次判定结论**（接受/拒绝及原因一致），
  `Decision.Duplicate=true`，不重复计数。即使重复发生在水位推进之后，结论也与首次
  完全一致。
- **冲突投递**（相同 ID、不同指纹）：明确拒绝，原因 `conflict`，错误类型为
  `*pipeline.RejectedError{Cause: CauseConflict}`，**绝不静默覆盖**。
- 判定优先级（首个失败规则生效，保证可解释）：
  1. 结构校验（schema 版本/字段/类型）；
  2. 幂等去重 / 冲突；
  3. 事件时间与水位、迟到策略判定。

## 3. 乱序与迟到处理

- **乱序**：事件可按任意顺序到达。只要事件时间 `t >= 当前水位`，就计入对应窗口
  （窗口可能尚未封存），到达顺序不影响最终统计。
- **迟到**：`t < 当前水位` 时按以下顺序明确分类，三类判定互不混淆：

  | 情形 | 判定原因 | 是否计数 |
  |---|---|---|
  | 所属窗口已封存（`window.End <= watermark`） | `window_sealed` | 否（结果不可变） |
  | 窗口仍开，但迟到量 `watermark - t > AllowedLateMs` | `late_dropped` | 否 |
  | 窗口仍开且在允许迟到宽限内，策略为 `reject` | `late_dropped` | 否 |
  | 窗口仍开且在宽限内，策略为 `drop_side_output`（默认） | 接受，计入窗口 | 是，同时放入**侧输出**（`SideOutput()`）留痕审计 |

  - 允许迟到宽限：`WithAllowedLateness(ms)`，默认 `0`。
  - 策略：`WithLatePolicy(policy)`，`PolicyDrop`（默认，计数+侧输出）或
    `PolicyReject`（一律拒绝）。
  - 迟到但计入的事件在 `Decision.Detail` 中记录
    “late but window still open; counted and side-output”，判定依据可审计。

## 4. 结构（Schema）增量演进

注册表 `Registry` 保存全部历史版本。事件**始终按自身声明的 `SchemaVer`
校验**，因此历史数据在新版本注册后重放，结论与首次处理一致。

v1 为固定核心结构：`event_id:string, symbol:string, event_time:int64,
price:int64, quantity:int64, is_trade:bool`。演进规则（仅允许增量）：

- ✅ **新增可选字段**（旧事件不含新字段时仍按其旧版本校验）；
- ✅ **唯一允许的类型变更：`int64 -> float64`（拓宽）**；读取侧对 float64 字段
  兼容 int64 值；
- ❌ 删除或重命名任何字段 → `schema_removing_field`；
- ❌ 类型收窄（`float64 -> int64`）或跨类型变更 → `schema_type_incompatible`
  （报告中给出字段名、from/to 类型）；
- ❌ 版本号不连续（跳号）→ `schema_version_gap`；
- ❌ 注册比最新版本更低的新版本号 → `schema_version_regressed`；
- ❌ 用已有版本号注册**不同内容** → `schema_duplicate_version`（版本号代表不可变
  的历史结构）；
- ✅ 用相同版本号注册**相同内容** → 幂等成功（保证“注册历史重放”结论一致）。

事件级校验原因：`schema_unknown_version`、`schema_missing_field`、
`schema_extra_field`（携带未声明字段）、`schema_field_type_mismatch`、
`schema_empty`。每次 `Register` 返回 `*CompatReport`（含 from/to 版本、原因、
字段、类型、说明）；失败时同时返回 `*RejectedError`，可用 `AsRejected(err)`
提取程序化原因。

## 5. 可复现性与并发

- 全部状态转换在单一互斥锁下串行化；窗口统计为可交换聚合（计数/求和），输出按
  symbol 与窗口起点排序，因此结果只取决于（事件、水位推进）输入流，与 goroutine
  调度无关。`go test -race` 通过。
- 同一份输入脚本在全新注册表+聚合器上重放，得到逐事件一致的判定集合与逐窗口
  一致的统计（见 `TestCombinedScenario`，并发写入 × 乱序 × 重复 × 迟到 ×
  封窗后迟到 × 冲突 × 未知 schema 版本的组合）。
- `Submit` 入口对 `Fields` map 做防御性深拷贝，调用方提交后修改 map 不影响
  内部指纹、统计与侧输出。

## 6. 判定结果与日志

每次 `Submit` 返回 `market.Decision`：事件 ID、事件时间、所属窗口、接受与否、
拒绝原因（`RejectCause` 枚举）、当前水位、是否重复、内容指纹与文字说明。测试用
`-v` 运行时逐条打印形如：

```
[decision] event_id=late-open event_time=1001 window=[1000,1010) watermark=1005
           accepted=true duplicate=false reason="" detail=late but window ...
[run-1 wm=1010] sealed window [1000,1010) events=5 volume=11 sealed_at_wm=1010 symbols=2
```

## 7. 最小用法

```go
reg := pipeline.NewRegistry()
reg.Register(pipeline.CoreSchemaV1)

agg := pipeline.New(reg,
    pipeline.WithWindowSize(10_000),
    pipeline.WithAllowedLateness(1_000),
    pipeline.WithLatePolicy(market.PolicyDrop))

d, err := agg.Submit(market.Event{
    EventID: "e1", Symbol: "BTC", EventTime: 1001, SchemaVer: 1,
    Price: 100_000_000, Quantity: 2, IsTrade: true,
})
_ = d
_, sealed := agg.AdvanceWatermark(10_010) // 封存 [10000,10010)
```
