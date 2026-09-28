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

| 结论 | 含义 |
|---|---|
| `accepted` | 事件满足候选 schema；缺失的可选字段已按声明默认值补齐（`Defaulted` 列出字段） |
| `data_rejected` | **升级本身可行**，但这一条历史数据不满足候选结构（`Reason` 给出事件级原因） |
| `skipped_plan_incompatible` | **这次升级本身不兼容**（`TrialReport.Compatible=false`，如新增字段无默认值），逐条跳过 |

干跑为只读：不修改注册表与任何在线状态；同一批输入重复试跑结论一致；通过后
`CommitUpgrade` 并在线处理同一批事件，逐条结论与试跑一致。
