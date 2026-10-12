package k8sgate

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// maxSeries bounds the number of distinct admission label sets. Operation and
// resource come from the request, so without a bound a caller could grow the
// map without limit. Past the bound new label sets are counted as "other".
const maxSeries = 256

type admissionKey struct{ operation, resource, result string }

// metrics holds the gate's counters and renders them in the Prometheus text
// format. It is small on purpose: counters only, no client library.
type metrics struct {
	mu        sync.Mutex
	admission map[admissionKey]uint64
	commit    map[string]uint64
}

func newMetrics() *metrics {
	return &metrics{admission: map[admissionKey]uint64{}, commit: map[string]uint64{}}
}

func (m *metrics) admit(operation, resource, result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := admissionKey{operation, resource, result}
	if _, seen := m.admission[k]; !seen && len(m.admission) >= maxSeries {
		k = admissionKey{"other", "other", result}
	}
	m.admission[k]++
}

func (m *metrics) committed(result string) {
	m.mu.Lock()
	m.commit[result]++
	m.mu.Unlock()
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func (m *metrics) write(w io.Writer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Fprintln(w, "# HELP joltrin_k8sgate_admission_total Admission requests by outcome.")
	fmt.Fprintln(w, "# TYPE joltrin_k8sgate_admission_total counter")
	keys := make([]admissionKey, 0, len(m.admission))
	for k := range m.admission {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		return a.operation+a.resource+a.result < b.operation+b.resource+b.result
	})
	for _, k := range keys {
		fmt.Fprintf(w, "joltrin_k8sgate_admission_total{operation=\"%s\",resource=\"%s\",result=\"%s\"} %d\n",
			labelEscaper.Replace(k.operation), labelEscaper.Replace(k.resource), k.result, m.admission[k])
	}

	fmt.Fprintln(w, "# HELP joltrin_k8sgate_commit_total Commit requests by outcome.")
	fmt.Fprintln(w, "# TYPE joltrin_k8sgate_commit_total counter")
	results := make([]string, 0, len(m.commit))
	for r := range m.commit {
		results = append(results, r)
	}
	sort.Strings(results)
	for _, r := range results {
		fmt.Fprintf(w, "joltrin_k8sgate_commit_total{result=\"%s\"} %d\n", r, m.commit[r])
	}
}

func (g *Gate) serveMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	g.metrics.write(w)
}
