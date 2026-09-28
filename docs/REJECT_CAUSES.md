# 判定原因（RejectCause）速查

所有失败判定都携带 `market.RejectCause` 枚举值（同时出现在 `Decision.Reason`
与 `*pipeline.RejectedError.Cause`，可用 `pipeline.AsRejected(err)` 提取），
各值彼此可区分，不依赖错误文本解析。

## 事件提交（Aggregator.Submit）

| 原因 | 触发条件 | 是否计数 |
|---|---|---|
| `duplicate`（由 `Decision.Duplicate=true` 表示，接受） | 相同 ID、相同内容指纹的重复投递，回放首次判定 | 否（已计过） |
| 跨版本重复（`Duplicate=true` + `CrossVersion=true`，接受） | 灰度期同一逻辑事件以不同 schema 版本到达；投影到最新版本并补齐默认值、归一数值拓宽后语义指纹一致；`MatchedVer`/`Defaulted` 可审计 | 否（已计过） |
| `conflict` | 相同 ID、**同版本**、不同内容指纹；绝不静默覆盖 | 否 |
| `cross_version_conflict` | 相同 ID、**跨版本**，补齐默认值/归一拓宽后价格、数量或新增字段取值仍有实质差异；与 `conflict` 和跨版本重复三者可区分 | 否 |
| `window_sealed` | 事件所属窗口已被水位封存，结果不可变 | 否 |
| `late_dropped` | 迟到且窗口仍开，但超出 `AllowedLateMs`，或策略为 `reject` | 否 |
| `schema_unknown_version` | 事件声明的 schema 版本未注册 | 否 |
| `schema_missing_field` | 缺少所声明版本要求、且无安全默认值的字段 | 否 |
| `schema_extra_field` | 携带了所声明版本未定义的字段 | 否 |
| `schema_field_type_mismatch` | 字段值类型与声明不符 | 否 |

判定优先级：结构校验 → 去重/跨版本调和/冲突 → 迟到/窗口分类（首个失败规则生效）。

## 水位推进（Aggregator.AdvanceWatermark）

| 原因 | 触发条件 |
|---|---|
| `watermark_regression_rejected` | 新水位不大于当前水位（水位只单调前进） |

## 结构演进（Registry.Register，见 CompatReport）

| 原因 | 触发条件 |
|---|---|
| `schema_empty` | 注册一个没有任何字段（或字段名重复）的 schema |
| `schema_version_gap` | 首版本不是 1，或版本号跳号 |
| `schema_version_regressed` | 注册比最新版本更低的全新版本号 |
| `schema_duplicate_version` | 用已存在的版本号注册不同内容（版本不可变） |
| `schema_removing_field` | 删除或重命名字段 |
| `schema_type_incompatible` | 类型收窄或跨类型变更（仅允许 `int64 -> float64` 拓宽，`CompatReport` 带 `Field/FromType/ToType`） |
| `schema_added_field_without_default` | **灰度启用**（`CommitUpgrade`/干跑试算）新增字段却没有安全默认值，会使已存在事件失效，升级生效前拒绝 |
| `schema_default_type_mismatch` | 字段声明的默认值与其类型不符 |

相同版本号 + 相同内容的重复注册幂等成功，保证注册历史可重放。

## 干跑试算（UpgradePlanner.TrialUpgrade，见 TrialItem.Outcome）

上线前预演请用 `NewOnlineUpgradePlanner(agg)`：它对**当前在线状态的快照**（水位、
已封存窗口由水位推出、已见事件集合、窗口/迟到配置）做只读重放。预演在影子注册表
（历史版本重放 + 候选 `CommitUpgrade`）和影子聚合器（在线状态深拷贝）上逐条调用与
`Aggregator.Submit` 相同的判定代码，因此逐条结论与正式处理一致。报告字段
`OnlineBound/OnlineWatermark/OnlineSeen` 记录所依据的在线快照。

| 结论 | 含义 | 是否计数 |
|---|---|---|
| `accepted` | **首次计入**：当前状态下接受（按时，或窗口仍开且宽限内按策略计入） | 是 |
| `duplicate` | **重复不计入**：逻辑 ID 已在线上或本批更早出现；回放首次判定。`Duplicate/CrossVersion/MatchedVer/Defaulted` 与在线一致；跨版本等价重投不会因新旧表示不同被误判成冲突 | 否（已计过） |
| `data_rejected` | **明确拒绝**：升级方案可行，但该条在当前状态下本就该拒，`Reason` 给逐条依据——`window_sealed`（所属窗口已封）、`late_dropped`（超宽限或策略为 reject）、`conflict`、`cross_version_conflict`、各类 `schema_*` | 否 |
| `skipped_plan_incompatible` | **升级方案本身不兼容**（`TrialReport.Compatible=false`，如新增字段无默认值），逐条跳过，与单条数据失败严格分开 | — |

注意：同 ID 内容冲突的重投在线上同样带 `Duplicate=true` 标记，但该判定不被接受，
预演中归为 `data_rejected`（不是 `duplicate`）；首次判定本身是拒绝（如
`window_sealed`/`late_dropped`）的重投，回放该拒绝，仍归 `data_rejected`。

干跑为只读：不修改水位、已封存窗口、已见事件、侧输出或任何在线统计；相同输入 +
相同在线状态重复试跑报告逐字节一致。通过后 `CommitUpgrade` 并用同一批事件、同样的
当前状态在线处理，逐条“接受/拒绝 + 原因 + 重复/跨版本标记 + 窗口 + 判定时水位”都
与试算一致（`pipeline/trial_online_test.go` 覆盖封窗范围内重投、迟到、重复、跨版本
等价重投、数值拓宽等价与真实冲突）。纯结构预览可用
`NewUpgradePlanner(reg)`，但它按空在线状态判断，不能代表上线结论。
