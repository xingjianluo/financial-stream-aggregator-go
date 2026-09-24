package pipeline

import (
	"errors"
	"fmt"

	"github.com/xingjianluo/financial-stream-aggregator-go/market"
)

// RejectedError carries a distinguishable cause for a rejected event or an
// incompatible schema change. Callers can inspect Cause programmatically
// instead of parsing error text.
type RejectedError struct {
	Cause  market.RejectCause
	Detail string
}

func (e *RejectedError) Error() string {
	if e.Detail == "" {
		return string(e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Cause, e.Detail)
}

// AsRejected extracts a *RejectedError from err, if present.
func AsRejected(err error) (*RejectedError, bool) {
	var re *RejectedError
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}
