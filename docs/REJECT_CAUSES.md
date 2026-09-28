# 判定原因（RejectCause）速查

所有失败判定都携带 `market.RejectCause` 枚举值（同时出现在 `Decision.Reason`
与 `*pipeline.RejectedError.Cause`，可用 `pipeline.AsRejected(err)` 提取），
各值彼此可区分，不依赖错误文本解析。

## 事件提交（Aggregator.Submit）

| 原因 | 触发条件 | 是否计数 |
|---|---|---|
| `duplicate`（由 `Decision.Duplicate=true` 表示，接受） | 相同 ID、相同**规范化逻辑指纹**的重复投递，回放首次判定；包含灰度期新老生产者各投一次的**跨版本重复**（版本标签不同、新字段取默认值或纯 `int64→float64` 拓宽且数值不变都不算差异，`Detail` 注明两个版本号） | 否（已计过） |
| `conflict` | 相同 ID、不同规范化逻辑指纹（价格/数量不同，或新增字段取了非默认值，或拓宽后数值实质变化）；绝不静默覆盖，与跨版本重复严格区分 | 否 |
| `window_sealed` | 事件所属窗口已被水位封存，结果不可变（跨版本重投也回放首次结论，绝不改封窗结果） | 否 |
| `late_dropped` | 迟到且窗口仍开，但超出 `AllowedLateMs`，或策略为 `reject` | 否 |
| `schema_unknown_version` | 事件声明的 schema 版本未注册 | 否 |
| `schema_missing_field` | 缺少所声明版本要求的字段（带声明默认值的可选字段缺省不算缺失，会按默认值补齐） | 否 |
| `schema_extra_field` | 携带了所声明版本未定义的字段 | 否 |
| `schema_field_type_mismatch` | 字段值类型与声明不符；注册时声明的默认值类型不符也用此原因 | 否 |

判定优先级：结构校验 → 去重/冲突 → 迟到/窗口分类（首个失败规则生效）。

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
| `schema_no_safe_default` | **仅灰度升级路径** `RegisterUpgrade` / `ApplyUpgrade`：新版本新增的字段没有声明安全默认值，存量事件无法按新版本重建——升级在生效前被拒，`CompatReport.Field` 给出字段名；与普通 `Register` 的历史语义（允许新增必填字段）相区别 |

相同版本号 + 相同内容的重复注册幂等成功，保证注册历史可重放。

## 升级干跑（Aggregator.DryRunUpgrade，见 UpgradePreview）

干跑在注册表与聚合器当前状态的**深拷贝**上执行，只读、可重复。它区分两类失败：

| 形态 | 判定 |
|---|---|
| 升级本身不兼容（如 `schema_no_safe_default`、移除字段、类型不兼容、版本规则） | `UpgradePreview.UpgradeOK=false`，`Reason/Field` 给出依据，`Events` 为空 |
| 升级可接受，仅个别历史数据不满足要求 | `UpgradeOK=true`，`Summary.Rejected>0`，逐条原因见 `Events[i].Reason` |

干跑通过后 `ApplyUpgrade` 真正启用，无穿插流量时对同一批事件的 `Submit` 判定与
`Events` 逐条一致（接受/重复/原因/逻辑指纹）。
