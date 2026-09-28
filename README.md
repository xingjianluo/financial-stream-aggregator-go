# 本地行情流式聚合管线（Go）

多个并发写入者按**事件时间**（event time）提交增量行情，管线输出**可复现**的
滚动窗口（tumbling window）统计，并在乱序、重复、迟到数据以及结构演进下保持
结果唯一、判定可解释。

- 包：
  - `market`：领域类型（事件、窗口、窗口统计、判定结果、可区分拒绝原因）。
  - `pipeline`：聚合器（`Aggregator`）、结构注册表（`Registry`）、内容指纹、
    灰度升级层（跨版本判重、默认值补齐、升级干跑）。
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

事件以 `EventID` 作为逻辑身份。管线对每个已见 ID 保存其**规范化逻辑指纹**
（`Registry.DedupFingerprint`，SHA-256 截断；指纹剔除 schema 版本标签、归一
`int64->float64` 拓宽、剔除取值等于声明默认值的可选字段；**不含 EventID
本身**。原始内容指纹 `ContentFingerprint` 仍保留，用于审计线格式）：

- **重复投递**（相同 ID、相同指纹，含新老生产者各投一次的**跨版本重复**）：回放
  **首次判定结论**（接受/拒绝及原因一致），`Decision.Duplicate=true`，不重复
  计数。即使重复发生在水位推进或封窗之后，结论也与首次完全一致；跨版本重复在
  `Detail` 中注明两个版本号。判重细节见第 5 节。
- **冲突投递**（相同 ID、不同规范化指纹：价格/数量或新字段实质取值不同）：明确
  拒绝，原因 `conflict`，错误类型为
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

- ✅ **新增字段**：老事件不含新字段时仍按其旧版本校验；若新字段带声明默认值
  （`FieldSpec.HasDefault`），它是可选字段，缺省按默认值补齐（见第 5 节）；
- ✅ **唯一允许的类型变更：`int64 -> float64`（拓宽）**；读取侧对 float64 字段
  兼容 int64 值，判重时把两种载体归一为同一数值；
- ❌ 删除或重命名任何字段 → `schema_removing_field`；
- ❌ 类型收窄（`float64 -> int64`）或跨类型变更 → `schema_type_incompatible`
  （报告中给出字段名、from/to 类型）；
- ❌ 版本号不连续（跳号）→ `schema_version_gap`；
- ❌ 注册比最新版本更低的新版本号 → `schema_version_regressed`；
- ❌ 用已有版本号注册**不同内容** → `schema_duplicate_version`（版本号代表不可变
  的历史结构）；
- ✅ 用相同版本号注册**相同内容** → 幂等成功（保证“注册历史重放”结论一致）。

> 灰度升级请走 `RegisterUpgrade` / `ApplyUpgrade`：它额外要求**每个新增字段都
> 有安全默认值**，否则在生效前以 `schema_no_safe_default` 拒绝（详见第 5 节）。
> 普通 `Register` 保留历史语义，允许新增无默认值的必填字段。

事件级校验原因：`schema_unknown_version`、`schema_missing_field`、
`schema_extra_field`（携带未声明字段）、`schema_field_type_mismatch`、
`schema_empty`。每次 `Register` 返回 `*CompatReport`（含 from/to 版本、原因、
字段、类型、说明）；失败时同时返回 `*RejectedError`，可用 `AsRejected(err)`
提取程序化原因。

## 5. 灰度升级：跨版本共存、判重与默认值补齐

灰度期间同一笔逻辑行情可能被**新老两套生产者各投一次**：老事件不带新增字段，
新事件带新增字段。这一层在原有管线之上工作，入口都在 `pipeline` 包。

### 5.1 可选字段与安全默认值

新版本新增字段时，用 `FieldSpec` 声明“可缺省 + 明确默认值”：

```go
FieldSpec{Name: "venue", Type: market.TypeString, HasDefault: true, Default: ""}
FieldSpec{Name: "score", Type: market.TypeInt64, HasDefault: true, Default: int64(0)}
```

- 老事件在新版本注册后**仍然按自己声明的旧版本校验**，绝不会因为缺少新字段而被
  当成结构不完整（缺字段必须是显式默认值字段）。
- 需要按新版本读取/重建老事件时，调 `Registry.Canonicalize(e)`：缺失的可选字段
  **按 schema 链上的声明默认值逐级补齐**，结果只取决于注册表与事件本身，稳定、
  可重放；同时返回被补齐的字段名清单供审计。
- 声明的默认值必须与字段类型一致，否则在注册时就以
  `schema_field_type_mismatch` 拒绝。

### 5.2 灰度升级注册：`RegisterUpgrade` 与 `ApplyUpgrade`

- `Registry.RegisterUpgrade(s)`（或聚合器上的 `agg.ApplyUpgrade(s)`）在原有
  增量演进规则之上追加一条**灰度规则**：新版本**新增的每个字段都必须带安全
  默认值**。
- 不满足时，升级在**生效之前**就被拒掉，原因是可区分的
  `schema_no_safe_default`（报告给出字段名），注册表不发生任何变化，绝不会
  “先放过去、运行中再报错”。
- 老的 `Register` 保持原语义（允许新增无默认值的必填字段，见第 4 节），灰度
  场景必须走 `RegisterUpgrade`。
- 接受的结构变更：新增带默认值的可选字段；`int64 -> float64` 拓宽。
- 拒绝的结构变更：新增字段无安全默认值（`schema_no_safe_default`）；删除/
  重命名字段；类型收窄或跨类型变更；版本跳号/回退/同号不同体。

### 5.3 跨版本判重：规范化逻辑指纹

判重不再比较线上原始字节，而是用 `Registry.DedupFingerprint(e)` 计算
**规范化逻辑指纹**，规则：

1. 去掉 `schema_ver` 标签——版本号不同本身绝不构成冲突；
2. 对最终声明为 `float64` 的数值字段，把 `int64` 载体归一为 `float64`——
   纯类型拓宽、数值不变（如 `7` 与 `7.0`）视为相同；
3. 携带值**等于声明默认值**的可选字段从指纹投影中剔除——新生产者显式发默认值
   与老生产者缺省该字段，描述的是同一逻辑事件。

于是：

- **跨版本重复**（同一 `EventID`、规范化指纹相同）：回放**首次判定结论**
  （接受/拒绝及原因一致），`Decision.Duplicate=true`，`Detail` 追加
  `cross-version duplicate (first seen as schema vX, this delivery is vY)`，
  统计**只计一次**。全部状态转换在单锁内串行化，两套生产者如何并发抢跑、
  到达顺序如何，结论一致、计数唯一；封窗之后再收到跨版本重投，同样回放首次
  结论（已计过就不计数、永不改封窗结果）。
- **真实冲突**（同一 `EventID`、规范化指纹不同：价格/数量不同，或新字段取了
  非默认值，或拓宽字段数值真的变了）：明确拒绝，原因仍是可区分的 `conflict`，
  与跨版本重复严格分开，绝不静默覆盖。
- 指纹对“未来注册的新版本”稳定：后续版本新增的默认值字段不会改变历史事件的
  指纹。

### 5.4 升级前干跑：`DryRunUpgrade`

```go
pv := agg.DryRunUpgrade(proposedSchema, batch) // 只读，不动任何在线状态
pv.UpgradeOK                                   // 升级本身是否兼容
pv.Reason / pv.Field                           // 不兼容时的原因与字段
pv.Events                                      // 逐条：接受/重复/拒绝 + 依据
pv.Summary                                     // 汇总：接受/重复/冲突/按原因计数
```

- 干跑在**注册表深拷贝**与**聚合器当前状态的深拷贝**上进行：水位、窗口、已见
  事件、封窗结果都不会被改动；同一批输入、同一在线状态重复干跑，结论逐字节
  一致（测试用 JSON 序列化比对验证）。
- 两种失败严格区分：
  - **升级本身不兼容**：`UpgradeOK=false`，`Reason=schema_no_safe_default`
    等（或移除字段/类型不兼容/版本规则原因），此时不逐条试事件，`Events` 为空；
  - **升级没问题、个别历史数据不满足要求**：`UpgradeOK=true`，
    `Summary.Rejected>0`，每条失败事件在 `Events` 中带自己的原因（如
    `schema_field_type_mismatch`）。
- 每条 `EventPreview` 都带：事件 ID、**来源 schema 版本**、事件时间、所属窗口、
  当时水位、接受/重复标记、原因、逻辑指纹，以及按新版本重建时**将补齐哪些
  默认值字段**（`Backfilled`）。
- **干跑通过后启用一致性**：随后 `ApplyUpgrade(proposedSchema)` 且无穿插流量，
  对同一批事件逐次 `Submit` 的判定（接受与否、重复、原因、指纹）与干跑逐条
  一致（见 `TestGrayDryRun`）。

## 6. 可复现性与并发

- 全部状态转换在单一互斥锁下串行化；窗口统计为可交换聚合（计数/求和），输出按
  symbol 与窗口起点排序，因此结果只取决于（事件、水位推进）输入流，与 goroutine
  调度无关。`go test -race` 通过。
- 同一份输入脚本在全新注册表+聚合器上重放，得到逐事件一致的判定集合与逐窗口
  一致的统计（见 `TestCombinedScenario`，并发写入 × 乱序 × 重复 × 迟到 ×
  封窗后迟到 × 冲突 × 未知 schema 版本的组合；以及
  `TestGrayDecisionDeterminismReplay`、`TestGrayMixedConcurrentWriters` 的
  灰度混合版本组合）。
- 干跑 `DryRunUpgrade` 在深拷贝状态上执行，天然不触碰在线状态；同批输入重复
  干跑结论一致，干跑通过后启用再跑同一批，判定逐条一致。
- `Submit` 入口对 `Fields` map 做防御性深拷贝，调用方提交后修改 map 不影响
  内部指纹、统计与侧输出。

## 7. 判定结果与日志

每次 `Submit` 返回 `market.Decision`：事件 ID、事件时间、所属窗口、接受与否、
拒绝原因（`RejectCause` 枚举）、当前水位、是否重复、规范化逻辑指纹与文字说明。
测试用 `-v` 运行时逐条打印形如：

```
[decision] event_id=late-open event_time=1001 window=[1000,1010) watermark=1005
           accepted=true duplicate=false reason="" detail=late but window ...
[run-1 wm=1010] sealed window [1000,1010) events=5 volume=11 sealed_at_wm=1010 symbols=2
```

灰度干跑 `DryRunUpgrade` 还会逐条打印事件的**来源 schema 版本**、事件时间、窗口、
水位、接受/重复标记、判定原因、将补齐的默认值字段与逻辑指纹，并打印汇总结论：

```
[dryrun] UPGRADE ITSELF REJECTED reason="schema_no_safe_default" field="venue" ...
[dryrun] upgrade compatible v2->v3 summary=upgrade ok ... accepted=2 duplicates=1 rejected=1
[dryrun] idx=0 event_id=d1 schema_ver=1 event_time=1 ... backfilled=[venue score] fp=81714a658f138515
```

```bash
go test -v -run TestGray ./pipeline/   # 只看灰度升级相关用例的逐条日志
```

## 8. 最小用法

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

灰度升级（新字段带安全默认值，先干跑再启用）：

```go
v2 := pipeline.Schema{Version: 2, Fields: []pipeline.FieldSpec{
    /* v1 字段... */
    {Name: "venue", Type: market.TypeString, HasDefault: true, Default: ""},
}}

// 1) 只读试算：历史事件逐条结论 + 汇总，不改动在线状态
pv := agg.DryRunUpgrade(v2, history)
if !pv.UpgradeOK { /* pv.Reason: 升级本身不兼容，如 schema_no_safe_default */ }

// 2) 干跑可接受后真正启用；之后重放同一批，Submit 结论与 pv.Events 逐条一致
if _, err := agg.ApplyUpgrade(v2); err != nil { /* ... */ }
```
