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

试算构造时绑定**实时聚合器**：`NewUpgradePlanner(reg, agg)`。它对当前在线状态
（水位、开/封窗、允许迟到宽限与策略、已见事件台账、侧输出）做深拷贝快照，并在
影子注册表上登记候选 schema，然后在影子聚合器上走与 `Aggregator.Submit` 完全相同
的判定路径。因此封窗、迟到、重复/冲突都会按当前状态逐条判定。

| 结论 | 含义 | 是否计数 |
|---|---|---|
| `accepted` | **首次**投递且当前状态下接受（含宽限内迟到计入+侧输出）；`Counted=true` | 是（仅这一类计入） |
| `duplicate` | 已见 ID 的重复投递（在线台账或本批内已见），不重复计数；`Decision` 字段原样回放首次判定：相同内容重复、跨版本等价重投（`CrossVersion=true`，`MatchedVer`/`Defaulted` 可审计）回放接受；首次判定本就是拒绝（如首投时已封窗）的重投回放该拒绝（`Accepted=false`+原原因），仍归 `duplicate` | 否 |
| `data_rejected` | **首次**投递在当前状态下被**明确拒绝**（非空 `*RejectedError`）：`window_sealed`、`late_dropped`、`conflict`、`cross_version_conflict`、`schema_*`。冲突重投虽带 `Duplicate=true`，但因同时带拒绝错误，归入本类，与等价重复的 `duplicate` 可区分 | 否 |
| `skipped_plan_incompatible` | **这次升级本身不兼容**（`TrialReport.Compatible=false`，如新增字段无默认值），逐条跳过，不访问在线判定 | 否 |

- 方案级不兼容（`Compatible=false`，全部 `skipped_plan_incompatible`）与单条数据/
  当前状态不满足（方案兼容但个别 `data_rejected`）严格分开；`TrialReport.AllPassed()`
  是“方案兼容且无任何明确拒绝”的启用闸门。
- 干跑为只读：不推进水位、不封窗、不登记已见 ID、不改统计与侧输出、不写注册表；
  相同输入 + 相同在线状态重复试跑结论一致（`OnlineWatermark` 记录试算所依据水位）。
- 通过后 `CommitUpgrade` 并以同一批事件、同一当前状态在线处理，逐条“接受/拒绝 +
  原因 + 重复/跨版本标记 + 窗口 + 水位”与试算一致（`assertTrialMatchesLive`
  逐字段断言，见 `TestTrial*` 用例）。
