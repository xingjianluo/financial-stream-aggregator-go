package aggregator

import (
	"fmt"
	"math/rand"
)

func fmtID(i int) string { return fmt.Sprintf("e%02d", i) }

func shuffledCopy(in []Event, seed int) []Event {
	out := make([]Event, len(in))
	copy(out, in)
	r := rand.New(rand.NewSource(int64(seed + 1)))
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}
