package aggregator

import (
	"fmt"
	"sort"
)

// Registry 保存各版本 schema，并在注册时校验前后版本兼容性。
type Registry struct {
	name    string
	schemas map[int]Schema
	latest  int
}

func NewRegistry() *Registry {
	return &Registry{schemas: map[int]Schema{}}
}

// Register 注册一个新版本。规则：
//  1. 首次注册版本号必须为 1，之后必须严格 +1（禁止跳号/重号）。
//  2. 允许的兼容变更：
//     - 新增字段，但新字段必须是 optional；
//     - 字段类型"拓宽"：int64 -> float64（旧整数都能精确表示）；
//     - 必填字段放宽为 optional。
//  3. 其余变更一律拒绝：删除字段、新增必填字段、类型收窄(float64->int64)、
//     字段重命名（先删后增）、optional 收严为 required、其它类型互转。
func (r *Registry) Register(s Schema) error {
	if r.name == "" {
		r.name = s.Name
	}
	if s.Name != r.name {
		return &SchemaIncompatibleError{Name: s.Name, From: r.latest, To: s.Version,
			Detail: fmt.Sprintf("schema name cannot change: %q -> %q", r.name, s.Name)}
	}
	if len(r.schemas) == 0 {
		if s.Version != 1 {
			return &SchemaIncompatibleError{Name: s.Name, From: 0, To: s.Version,
				Detail: "first schema version must be 1"}
		}
	} else {
		if _, exists := r.schemas[s.Version]; exists {
			return &SchemaIncompatibleError{Name: s.Name, From: r.latest, To: s.Version,
				Detail: fmt.Sprintf("version %d already registered", s.Version)}
		}
		if s.Version != r.latest+1 {
			return &SchemaIncompatibleError{Name: s.Name, From: r.latest, To: s.Version,
				Detail: fmt.Sprintf("versions must increase by exactly 1, got v%d -> v%d", r.latest, s.Version)}
		}
		if err := checkCompatible(r.schemas[r.latest], s); err != nil {
			return err
		}
	}
	if err := checkFieldDefs(s); err != nil {
		return err
	}
	r.schemas[s.Version] = s
	r.latest = s.Version
	return nil
}

func checkFieldDefs(s Schema) error {
	seen := map[string]bool{}
	for _, f := range s.Fields {
		if f.Name == "" {
			return &SchemaIncompatibleError{Name: s.Name, From: 0, To: s.Version,
				Detail: "field name must not be empty"}
		}
		if seen[f.Name] {
			return &SchemaIncompatibleError{Name: s.Name, From: 0, To: s.Version,
				Detail: fmt.Sprintf("duplicate field %q", f.Name)}
		}
		seen[f.Name] = true
		switch f.Type {
		case FieldInt64, FieldFloat64, FieldString, FieldBool:
		default:
			return &SchemaIncompatibleError{Name: s.Name, From: 0, To: s.Version,
				Detail: fmt.Sprintf("field %q has unsupported type %q", f.Name, f.Type)}
		}
	}
	return nil
}

func checkCompatible(old, neu Schema) error {
	oldByName := map[string]FieldDef{}
	for _, f := range old.Fields {
		oldByName[f.Name] = f
	}
	newByName := map[string]FieldDef{}
	for _, f := range neu.Fields {
		newByName[f.Name] = f
	}

	// 旧字段必须全部存在（禁止删除/重命名），并检查类型与可选性。
	for _, of := range old.Fields {
		nf, ok := newByName[of.Name]
		if !ok {
			return &SchemaIncompatibleError{Name: neu.Name, From: old.Version, To: neu.Version,
				Detail: fmt.Sprintf("field %q removed or renamed", of.Name)}
		}
		switch {
		case of.Type == nf.Type:
			// 同类型：唯一允许的是 required -> optional（放宽）。
			if of.Optional && !nf.Optional {
				return &SchemaIncompatibleError{Name: neu.Name, From: old.Version, To: neu.Version,
					Detail: fmt.Sprintf("field %q changed optional -> required (not allowed)", of.Name)}
			}
		case of.Type == FieldInt64 && nf.Type == FieldFloat64:
			// int64 -> float64 是唯一允许的类型拓宽。
		default:
			return &SchemaIncompatibleError{Name: neu.Name, From: old.Version, To: neu.Version,
				Detail: fmt.Sprintf("field %q type changed %q -> %q (incompatible; only int64 -> float64 widening is allowed)",
					of.Name, of.Type, nf.Type)}
		}
	}

	// 新增字段必须是 optional。
	for _, nf := range neu.Fields {
		if _, existed := oldByName[nf.Name]; !existed && !nf.Optional {
			return &SchemaIncompatibleError{Name: neu.Name, From: old.Version, To: neu.Version,
				Detail: fmt.Sprintf("new field %q must be optional", nf.Name)}
		}
	}
	return nil
}

func (r *Registry) Get(version int) (Schema, bool) {
	s, ok := r.schemas[version]
	return s, ok
}

// Latest 返回最近注册的版本号。
func (r *Registry) Latest() int { return r.latest }

// normalizeEvent 按 schema 校验并规范化事件 payload：
//   - 拒绝未注册版本、未知字段、缺失必填字段、类型不符；
//   - 规范化类型：数字归一成 int64/float64，输出按 key 排序；
//   - 返回规范化后的 payload 以及聚合需要的 price(float64) 与 qty(int64)。
//
// 特殊规则：若某版本的 price 字段类型为 float64，允许 int64 数值自动拓宽，
// 这与 schema 的 int64->float64 兼容规则保持一致。
func (r *Registry) normalizeEvent(ev Event) (map[string]any, float64, int64, error) {
	s, ok := r.schemas[ev.SchemaVersion]
	if !ok {
		return nil, 0, 0, &SchemaIncompatibleError{Name: r.name, From: 0, To: ev.SchemaVersion,
			Detail: fmt.Sprintf("unknown schema version %d", ev.SchemaVersion)}
	}
	normalized := make(map[string]any, len(ev.Payload))
	for _, f := range s.Fields {
		raw, present := ev.Payload[f.Name]
		if !present {
			if !f.Optional {
				return nil, 0, 0, fmt.Errorf("%w: missing required field %q", errInvalidPayload, f.Name)
			}
			continue
		}
		v, err := coerceValue(f, raw)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("%w: field %q: %v", errInvalidPayload, f.Name, err)
		}
		normalized[f.Name] = v
	}
	// 拒绝未知字段：避免历史版本重放时静默吞掉拼写错误的数据。
	for k := range ev.Payload {
		if _, known := normalized[k]; !known {
			return nil, 0, 0, fmt.Errorf("%w: unknown field %q for schema v%d", errInvalidPayload, k, s.Version)
		}
	}
	price, err := extractPrice(normalized)
	if err != nil {
		return nil, 0, 0, err
	}
	qty, err := extractQty(normalized)
	if err != nil {
		return nil, 0, 0, err
	}
	return normalized, price, qty, nil
}

func coerceValue(f FieldDef, raw any) (any, error) {
	switch f.Type {
	case FieldInt64:
		switch n := raw.(type) {
		case int64:
			return n, nil
		case int:
			return int64(n), nil
		case float64:
			return nil, fmt.Errorf("expected int64, got float64 %v", n)
		default:
			return nil, fmt.Errorf("expected int64, got %T", raw)
		}
	case FieldFloat64:
		switch n := raw.(type) {
		case float64:
			return n, nil
		case int64: // 与 int64->float64 拓宽规则一致
			return float64(n), nil
		case int:
			return float64(n), nil
		default:
			return nil, fmt.Errorf("expected float64, got %T", raw)
		}
	case FieldString:
		if s, ok := raw.(string); ok {
			return s, nil
		}
		return nil, fmt.Errorf("expected string, got %T", raw)
	case FieldBool:
		if b, ok := raw.(bool); ok {
			return b, nil
		}
		return nil, fmt.Errorf("expected bool, got %T", raw)
	default:
		return nil, fmt.Errorf("unsupported type %q", f.Type)
	}
}

func extractPrice(m map[string]any) (float64, error) {
	v, ok := m["price"]
	if !ok {
		return 0, fmt.Errorf("%w: required field %q", errInvalidPayload, "price")
	}
	switch n := v.(type) {
	case float64:
		return n, nil
	case int64:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("%w: price must be int64/float64", errInvalidPayload)
	}
}

func extractQty(m map[string]any) (int64, error) {
	v, ok := m["qty"]
	if !ok {
		return 0, fmt.Errorf("%w: required field %q", errInvalidPayload, "qty")
	}
	switch n := v.(type) {
	case int64:
		return n, nil
	case float64:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("%w: qty must be int64", errInvalidPayload)
	}
}

// sortedKeys 返回规范化 payload 的 key 顺序，供日志展示。
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
