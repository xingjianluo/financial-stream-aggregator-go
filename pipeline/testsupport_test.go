package pipeline

import (
	"strconv"
	"testing"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// logDecision prints the audit line required by the test spec: schema
// version, window bounds, watermark, event time and the exact judgement basis
// (reason/detail), plus cross-version reconciliation markers.
func logDecision(t *testing.T, d market.Decision) {
	t.Helper()
	win := "<none>"
	if d.Window != nil {
		win = "[" + strconv.FormatInt(d.Window.Start, 10) + "," +
			strconv.FormatInt(d.Window.End, 10) + ")"
	}
	t.Logf("[decision] event_id=%-6s ver=%-2d matched_ver=%-2d event_time=%7d window=%-18s watermark=%7d accepted=%-5v duplicate=%-5v cross=%-5v reason=%-30q defaulted=%v detail=%s",
		d.EventID, d.SchemaVer, d.MatchedVer, d.EventTime, win, d.Watermark,
		d.Accepted, d.Duplicate, d.CrossVersion, string(d.Reason), d.Defaulted, d.Detail)
}

func logWindows(t *testing.T, label string, ws []market.WindowStats) {
	t.Helper()
	for _, w := range ws {
		t.Logf("[%s] sealed window [%d,%d) events=%d volume=%d sealed_at_wm=%d symbols=%d",
			label, w.Window.Start, w.Window.End, w.EventCount, w.Volume,
			w.SealedAtWM, len(w.BySymbol))
	}
}

func v1Schema() Schema { return CoreSchemaV1 }

func v2SchemaAddVenue() Schema {
	return Schema{Version: 2, Fields: append(append([]FieldSpec{},
		CoreSchemaV1.Fields...), FieldSpec{Name: "venue", Type: market.TypeString})}
}
