package aggregator

import (
	"errors"
	"testing"
)

func quoteSchema(version int, priceType FieldType, fields ...FieldDef) Schema {
	base := []FieldDef{
		{Name: "price", Type: priceType},
		{Name: "qty", Type: FieldInt64},
	}
	return Schema{Name: "quote", Version: version, Fields: append(base, fields...)}
}

func TestSchemaEvolutionCompatibility(t *testing.T) {
	cases := []struct {
		name    string
		build   func() (*Registry, error)
		wantErr bool
		detail  string // 不兼容时 Detail 中应包含的关键片段
	}{
		{
			name: "v1 基线",
			build: func() (*Registry, error) {
				r := NewRegistry()
				return r, r.Register(quoteSchema(1, FieldFloat64))
			},
		},
		{
			name: "新增 optional 字段 -> 兼容",
			build: func() (*Registry, error) {
				r := NewRegistry()
				_ = r.Register(quoteSchema(1, FieldFloat64))
				return r, r.Register(quoteSchema(2, FieldFloat64, FieldDef{Name: "venue", Type: FieldString, Optional: true}))
			},
		},
		{
			name: "int64 price 拓宽为 float64 -> 兼容",
			build: func() (*Registry, error) {
				r := NewRegistry()
				_ = r.Register(quoteSchema(1, FieldInt64))
				return r, r.Register(quoteSchema(2, FieldFloat64))
			},
		},
		{
			name: "required 放宽为 optional -> 兼容",
			build: func() (*Registry, error) {
				r := NewRegistry()
				_ = r.Register(Schema{Name: "quote", Version: 1, Fields: []FieldDef{
					{Name: "price", Type: FieldFloat64},
					{Name: "qty", Type: FieldInt64, Optional: true},
				}})
				return r, r.Register(Schema{Name: "quote", Version: 2, Fields: []FieldDef{
					{Name: "price", Type: FieldFloat64, Optional: true},
					{Name: "qty", Type: FieldInt64, Optional: true},
				}})
			},
		},
		{
			name: "float64 收窄为 int64 -> 拒绝",
			build: func() (*Registry, error) {
				r := NewRegistry()
				_ = r.Register(quoteSchema(1, FieldFloat64))
				return r, r.Register(quoteSchema(2, FieldInt64))
			},
			wantErr: true,
			detail:  "type changed",
		},
		{
			name: "删除字段 -> 拒绝",
			build: func() (*Registry, error) {
				r := NewRegistry()
				_ = r.Register(quoteSchema(1, FieldFloat64))
				return r, r.Register(Schema{Name: "quote", Version: 2, Fields: []FieldDef{
					{Name: "price", Type: FieldFloat64},
				}})
			},
			wantErr: true,
			detail:  "removed or renamed",
		},
		{
			name: "新增 required 字段 -> 拒绝",
			build: func() (*Registry, error) {
				r := NewRegistry()
				_ = r.Register(quoteSchema(1, FieldFloat64))
				return r, r.Register(quoteSchema(2, FieldFloat64, FieldDef{Name: "venue", Type: FieldString}))
			},
			wantErr: true,
			detail:  "must be optional",
		},
		{
			name: "optional 收严为 required -> 拒绝",
			build: func() (*Registry, error) {
				r := NewRegistry()
				_ = r.Register(Schema{Name: "quote", Version: 1, Fields: []FieldDef{
					{Name: "price", Type: FieldFloat64, Optional: true},
					{Name: "qty", Type: FieldInt64},
				}})
				return r, r.Register(Schema{Name: "quote", Version: 2, Fields: []FieldDef{
					{Name: "price", Type: FieldFloat64},
					{Name: "qty", Type: FieldInt64},
				}})
			},
			wantErr: true,
			detail:  "optional -> required",
		},
		{
			name: "string 改成 bool -> 拒绝",
			build: func() (*Registry, error) {
				r := NewRegistry()
				_ = r.Register(quoteSchema(1, FieldFloat64, FieldDef{Name: "venue", Type: FieldString, Optional: true}))
				return r, r.Register(quoteSchema(2, FieldFloat64, FieldDef{Name: "venue", Type: FieldBool, Optional: true}))
			},
			wantErr: true,
			detail:  "type changed",
		},
		{
			name: "版本跳号 v1 -> v3 -> 拒绝",
			build: func() (*Registry, error) {
				r := NewRegistry()
				_ = r.Register(quoteSchema(1, FieldFloat64))
				return r, r.Register(quoteSchema(3, FieldFloat64))
			},
			wantErr: true,
			detail:  "exactly 1",
		},
		{
			name: "首版本非 1 -> 拒绝",
			build: func() (*Registry, error) {
				r := NewRegistry()
				return r, r.Register(quoteSchema(2, FieldFloat64))
			},
			wantErr: true,
			detail:  "first schema version must be 1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := tc.build()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("期望不兼容错误，实际注册成功 latest=v%d", r.Latest())
				}
				var ie *SchemaIncompatibleError
				if !errors.As(err, &ie) {
					t.Fatalf("期望 *SchemaIncompatibleError，实际 %T: %v", err, err)
				}
				if tc.detail != "" && !contains(ie.Detail, tc.detail) {
					t.Fatalf("错误原因 %q 不包含期望片段 %q", ie.Detail, tc.detail)
				}
				t.Logf("符合预期地拒绝: %s", err)
				return
			}
			if err != nil {
				t.Fatalf("期望兼容，实际被拒绝: %v", err)
			}
			t.Logf("符合预期地接受至 v%d", r.Latest())
		})
	}
}

func TestNormalizeRejectsBadPayload(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(quoteSchema(1, FieldFloat64)); err != nil {
		t.Fatal(err)
	}

	bad := []Event{
		{ID: "e1", SchemaVersion: 1, Payload: map[string]any{"qty": int64(1)}},                         // 缺 price
		{ID: "e2", SchemaVersion: 1, Payload: map[string]any{"price": "100", "qty": int64(1)}},         // 类型错误
		{ID: "e3", SchemaVersion: 1, Payload: map[string]any{"price": 100.0, "qty": int64(1), "x": 1}}, // 未知字段
		{ID: "e4", SchemaVersion: 9, Payload: map[string]any{"price": 100.0, "qty": int64(1)}},         // 未知版本
	}
	for _, ev := range bad {
		if _, _, _, err := r.normalizeEvent(ev); err == nil {
			t.Fatalf("事件 %s 应被拒绝", ev.ID)
		} else {
			t.Logf("符合预期地拒绝 %s: %v", ev.ID, err)
		}
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
