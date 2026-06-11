package agent

import "fmt"

const defaultResultReducerCutoff = 4

type resultReducer struct {
	items       []any
	cutoff      int
	appendCount int
	truncated   bool
}

func newResultReducer() *resultReducer {
	return &resultReducer{
		cutoff: defaultResultReducerCutoff,
	}
}

func (r *resultReducer) Append(item any) {
	r.appendCount++
	if item == nil {
		return
	}
	if r.cutoff > 0 && len(r.items) >= r.cutoff {
		r.truncated = true
		return
	}

	r.items = append(r.items, item)
}

func (r *resultReducer) Write() any {
	if len(r.items) == 0 && !r.truncated {
		return []any{}
	}

	out := append([]any{}, r.items...)
	if r.truncated {
		out = append(out, map[string]any{
			"_result_reducer_notice": fmt.Sprintf("Results were %d and were cutoff after first %d rows. The UI already displayed these sample rows; do not repeat them in your answer.", r.appendCount, r.cutoff),
		})
	}
	return out
}
