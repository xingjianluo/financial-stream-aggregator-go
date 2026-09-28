# 本地行情流式聚合管线（Go）

多个并发写入者按**事件时间**（event time）提交增量行情，管线输出**可复现**的
滚动窗口（tumbling window）统计，并在乱序、重复、迟到数据、结构演进以及
**灰度升级（新旧两套生产者并行）**下保持结果唯一、判定可解释。

- 包：
  - `market`：领域类型（事件、窗口、窗口统计、判定结果、可区分拒绝原因）。
  - `pipeline`：聚合器（`Aggregator`）、结构注册表（`Registry`）、内容指纹、
    灰度升级规划器（`UpgradePlanner`，含干跑试算）。
- 仅依赖 Go 标准库；Go ≥ 1.25。

## 快速验证

```bash
go build ./...
go test ./...                 # 常规测试命令，全部用例本地可运行
go test -race ./...           # 并发安全检查（含灰度双生产者并发混写）
go test -v ./pipeline/        # 查看每个判定的版本/窗口/水位/事件时间/判定依据日志
```

灰度升级相关用例可单独运行：

```bash
go test -v -run 'Gray|CrossVersion|Default|Upgrade|Trial|Widening' ./pipeline/
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

## 2. 幂等去重与冲突拒绝（含灰度期跨版本判重）

事件以 `EventID` 作为逻辑身份。管线对每个已见 ID 保存其首次投递的原始事件与
**内容指纹**（SHA-256 截断，内容含 schema 版本、symbol、事件时间、价格、数量、
买卖方向与全部扩展字段，**不含 EventID 本身**）：

- **重复投递**（相同 ID、相同指纹）：回放**首次判定结论**（接受/拒绝及原因一致），
  `Decision.Duplicate=true`，不重复计数。即使重复发生在水位推进之后，结论也与首次
  完全一致。
- **冲突投递**（同版本、相同 ID、不同指纹）：明确拒绝，原因 `conflict`，错误类型为
  `*pipeline.RejectedError{Cause: CauseConflict}`，**绝不静默覆盖**。
- **灰度期跨版本重复**（新旧两套生产者各投一次，见第 8 节）：把两次投递都投影到
  注册表最新版本——新增的可选字段取声明默认值、唯一允许的拓宽 `int64 -> float64`
  在数值量级相同时视为等价——若语义指纹一致，则计一次：
  `Decision.Duplicate=true`、`Decision.CrossVersion=true`、`MatchedVer` 给出首次
  投递版本，`Defaulted` 列出补齐的字段；回放首次判定，**与到达顺序、并发抢占无关**。
- **灰度期真实冲突**：跨版本投影后价格、数量或新增字段取值仍有实质差异，拒绝原因
  `cross_version_conflict`，与同版本 `conflict` 和跨版本重复三者**彼此可区分**。
- 判定优先级（首个失败规则生效，保证可解释）：
  1. 结构校验（schema 版本/字段/类型）；
  2. 幂等去重 / 冲突 / 跨版本调和；
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

- ✅ **新增可选字段（带声明默认值）**：`FieldSpec{HasDefault: true, Default: ...}`；
  旧事件不含新字段时仍按其旧版本校验，读取/重建时由读取侧按默认值稳定补齐（第 8 节）；
- ✅ **新增字段（仅结构演进，`Register`）**：不带默认值的纯加字段在结构层面仍允许，
  但**不能作为灰度升级启用**——灰度启用必须走 `CommitUpgrade` 并带默认值；
- ✅ **唯一允许的类型变更：`int64 -> float64`（拓宽）**；读取侧对 float64 字段
  兼容 int64 值，拓宽后数值量级相同的同一事件判为等价重复；
- ❌ 删除或重命名任何字段 → `schema_removing_field`；
- ❌ 类型收窄（`float64 -> int64`）或跨类型变更 → `schema_type_incompatible`
  （报告中给出字段名、from/to 类型）；
- ❌ 声明的默认值与字段类型不符 → `schema_default_type_mismatch`（注册即拒，
  绝不拖到运行期）；
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

每次 `Submit` 返回 `market.Decision`：事件 ID、本次投递与首次匹配的 schema 版本
（`SchemaVer`/`MatchedVer`）、事件时间、所属窗口、接受与否、拒绝原因
（`RejectCause` 枚举）、当前水位、是否重复、是否跨版本重复（`CrossVersion`）、
补齐的默认字段（`Defaulted`）、内容指纹与文字说明。测试用 `-v` 运行时逐条打印形如：

```
[decision] event_id=late-open ver=1 matched_ver=0 event_time=1001 window=[1000,1010) watermark=1005
           accepted=true duplicate=false cross=false reason="" defaulted=[] detail=late but window ...
[gray] event_id=x1 ver=2 matched_ver=1 ... cross=true reason="" defaulted=[fee_bps venue] ...
[trial] v1->v2 compatible=true ... #0 outcome=accepted defaulted=[fee_bps venue]
[run-1 wm=1010] sealed window [1000,1010) events=5 volume=11 sealed_at_wm=1010 symbols=2
```

## 7. 灰度升级：跨版本判重、默认值补齐与干跑试算

灰度期一部分生产者按老版本（v1）发送，另一部分按新版本（如 v2）发送。同一笔
行情很可能被两套生产者各投一次。下列三条能力保证结果唯一、判定可复现。

### 7.1 跨版本怎么判重

1. 事件先按**自身声明版本**校验（老事件永不因新字段而结构失败）。
2. 相同 `EventID` 再次到达时：
   - 内容指纹完全相同 → 普通重复（任何版本）；
   - 版本不同：把两次投递都**投影到注册表最新 schema**——新版本新增但老版本缺失的
     可选字段取其**声明默认值**；对 `int64 -> float64` 拓宽字段，把整数与等值浮点
     归一为同一数值表示——再比较**语义指纹**（不含 schema 版本号）。
   - 语义一致 → 跨版本重复（`Duplicate=true, CrossVersion=true`），只计一次并回放
     首次结论；`MatchedVer`/`Defaulted` 给出对账依据。
   - 价格、数量或新增字段取值有实质差异 → `cross_version_conflict`，明确拒绝，
     与同版本 `conflict`、跨版本重复三者可区分。
3. 全部调和在聚合器单锁内完成；无论两个生产者 goroutine 如何抢、谁先到，同一逻辑
   事件只计一次，结论与到达顺序无关。封窗后的跨版本重投同样回放首次结论、不重计。

### 7.2 可选字段默认值怎么补齐

`FieldSpec` 新增 `HasDefault bool` 与 `Default any`：

- 校验时，带安全默认值的字段允许缺失，老事件不会被当作结构不完整；
- `pipeline.ProjectEvent(event, targetSchema)` 按目标版本重建事件，缺失字段填入
  声明默认值，返回按名排序的 `defaulted` 字段清单；输入不被修改，重复重建结果一致；
- 判重的语义投影与 `ProjectEvent` 使用同一套补齐规则。

### 7.3 哪些结构变更可接受、哪些会被拒绝

灰度启用走 `reg.CommitUpgrade(candidate)`（而不是纯结构演进用的 `Register`）：

- ✅ 新增字段且每个新字段都带**类型正确的安全默认值**；
- ✅ 既有数值字段 `int64 -> float64` 拓宽；
- ❌ 新增字段**没有安全默认值** → `schema_added_field_without_default`：升级在生效
  前被拒，注册表仍停留在旧版本，已存在事件不会失效；
- ❌ 默认值类型与字段不符 → `schema_default_type_mismatch`；
- ❌ 删字段/重命名、类型收窄或跨类型变更、跳号/版本回退/同号异内容：沿用第 4 节
  各自原因，与上面的灰度原因互不混淆。

### 7.4 升级前干跑试算

`planner := pipeline.NewUpgradePlanner(reg)`，
`rep, _ := planner.TrialUpgrade(candidate, batch)` 在**只读**状态下预演：

- `rep.Compatible=false`：**这次升级本身不兼容**（如新字段无默认值），每条事件标记
  `skipped_plan_incompatible`，汇总 `PlanCompatible=false`；注册表/在线状态不变；
- `rep.Compatible=true` 但个别条目 `Outcome=data_rejected`：**升级没问题，只是该条
  历史数据不满足要求**（带独立原因），与方案级不兼容严格区分；
- 通过的条目给出 `accepted` 与补齐字段清单；同一批输入重复试跑结论逐字节一致；
- 试跑通过后 `CommitUpgrade` 再对同一批事件在线处理，逐条“接受/拒绝 + 原因”结论
  与试跑一致（见 `TestTrialCommitConsistency`）。

### 7.5 最小灰度用法

```go
v2 := pipeline.Schema{Version: 2, Fields: append(
    append([]pipeline.FieldSpec{}, pipeline.CoreSchemaV1.Fields...),
    pipeline.FieldSpec{Name: "venue", Type: market.TypeString,
        HasDefault: true, Default: "UNKNOWN"})}

planner := pipeline.NewUpgradePlanner(reg)
if rep, _ := planner.TrialUpgrade(v2, history); rep.Compatible {
    reg.CommitUpgrade(v2) // 干跑通过后才启用
}
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
