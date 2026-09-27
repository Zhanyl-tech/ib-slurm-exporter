package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/cgroup"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/collector"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/fixture"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/ib"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/procfd"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/slurm"
)

// All inputs here are synthetic: the demo fixture, and a shell script
// standing in for squeue. Nothing ran against Slurm or an HCA.

func runOnce(t *testing.T, args ...string) (map[string]*dto.MetricFamily, string) {
	t.Helper()
	var out, errb bytes.Buffer
	if err := run(context.Background(), append(args, "--once"), &out, &errb); err != nil {
		t.Fatalf("run %v: %v\nstderr:\n%s", args, err, errb.String())
	}
	if strings.Contains(out.String(), "level=") {
		t.Fatalf("log lines leaked into stdout:\n%s", out.String())
	}
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("--once output is not valid exposition: %v\n%s", err, out.String())
	}
	for name := range fams {
		if !strings.HasPrefix(name, "ib_slurm_") {
			t.Errorf("--once must only print ib_slurm_* families, got %s", name)
		}
	}
	return fams, errb.String()
}

func metricValue(f *dto.MetricFamily, labels ...string) (float64, bool) {
	if f == nil {
		return 0, false
	}
outer:
	for _, m := range f.GetMetric() {
		for i := 0; i+1 < len(labels); i += 2 {
			found := false
			for _, l := range m.GetLabel() {
				if l.GetName() == labels[i] && l.GetValue() == labels[i+1] {
					found = true
				}
			}
			if !found {
				continue outer
			}
		}
		if m.GetCounter() != nil {
			return m.GetCounter().GetValue(), true
		}
		return m.GetGauge().GetValue(), true
	}
	return 0, false
}

func TestOnceDemoAllLayoutsAndModes(t *testing.T) {
	for _, layout := range []string{"v1", "v2", "v2-sluid"} {
		for _, own := range []string{"fd", "qp"} {
			t.Run(layout+"/"+own, func(t *testing.T) {
				fams, stderr := runOnce(t, "--demo", "--demo-layout", layout, "--ownership", own)
				if !strings.Contains(stderr, "demo mode") {
					t.Error("logs belong on stderr")
				}
				if v, _ := metricValue(fams["ib_slurm_unattributed_jobs"]); v != 2 {
					t.Errorf("unattributed = %v, want 2", v)
				}
				if v, _ := metricValue(fams["ib_slurm_device_jobs"], "device", "mlx5_1"); v != 2 {
					t.Errorf("mlx5_1 jobs = %v, want 2", v)
				}
				if v, _ := metricValue(fams["ib_slurm_unresolved_sluid_allocations"]); v != 0 {
					t.Errorf("unresolved = %v", v)
				}
				// The demo advances its counters once between two samples,
				// so an attributed job shows a non-zero increase.
				if v, ok := metricValue(fams["ib_slurm_job_counter_total"], "job_id", "918001", "counter", "port_xmit_data"); !ok || v <= 0 {
					t.Errorf("918001 port_xmit_data increase = %v,%v", v, ok)
				}
				if _, ok := metricValue(fams["ib_slurm_job_counter_total"], "device", "mlx5_1"); ok {
					t.Error("shared mlx5_1 attributed")
				}
				wantLayout := map[string]string{"v1": "cgroup-v1", "v2": "cgroup-v2", "v2-sluid": "cgroup-v2-sluid"}[layout]
				if _, ok := metricValue(fams["ib_slurm_cgroup_layout_info"], "layout", wantLayout); !ok {
					t.Errorf("layout_info{layout=%q} missing", wantLayout)
				}
				for _, f := range fams {
					if f.GetHelp() == "" {
						t.Errorf("%s has no HELP line", f.GetName())
					}
				}
			})
		}
	}
}

func writeScript(t *testing.T, name, body string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell script stand-in")
	}
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestOnceProductionWiringWithSqueueStandIn(t *testing.T) {
	// Not --demo: the real code path, pointed at a synthetic 26.05 tree and a
	// script that prints what squeue --Format would.
	spec := fixture.Default(fixture.V2SLUID)
	spec.OmitStepd = true // SLUIDs must resolve through the squeue cache
	roots, err := fixture.Build(t.TempDir(), spec)
	if err != nil {
		t.Fatal(err)
	}
	squeue := writeScript(t, "squeue", `
case "$*" in *--nodelist=localhost*) ;; *) echo "not node-scoped: $*" >&2; exit 2;; esac
printf '%s|%s|%s|%s|%s\n' 918001 alice research gpu `+fixture.SLUIDFor("918001")+`
printf '%s|%s|%s|%s|%s\n' 918004 dave trading roce `+fixture.SLUIDFor("918004")+`
`)
	fams, _ := runOnce(t,
		"--sys-root", roots.Sys, "--verbs-root", roots.Verbs,
		"--proc-root", roots.Proc, "--cgroup-root", roots.Cgroup,
		"--squeue-path", squeue)

	if v, _ := metricValue(fams["ib_slurm_unresolved_sluid_allocations"]); v != 2 {
		t.Errorf("918002/918003 are not in squeue output: unresolved = %v, want 2", v)
	}
	if _, ok := metricValue(fams["ib_slurm_job_counter_total"], "job_id", "918001", "user", "alice"); !ok {
		t.Error("918001 should be resolved via squeue and attributed with its user label")
	}
	if v, _ := metricValue(fams["ib_slurm_metadata_jobs"]); v != 2 {
		t.Errorf("metadata_jobs = %v", v)
	}
	if v, _ := metricValue(fams["ib_slurm_metadata_last_success_timestamp_seconds"]); v <= 0 {
		t.Error("last success timestamp not set")
	}
}

func TestOnceWithSqueueDisabledAndBroken(t *testing.T) {
	roots, err := fixture.Build(t.TempDir(), fixture.Default(fixture.V2SLUID))
	if err != nil {
		t.Fatal(err)
	}
	base := []string{"--sys-root", roots.Sys, "--verbs-root", roots.Verbs,
		"--proc-root", roots.Proc, "--cgroup-root", roots.Cgroup}

	// Disabled: SLUIDs still resolve from slurmstepd titles; no metadata metrics.
	fams, _ := runOnce(t, append(base, "--squeue-path=")...)
	if v, _ := metricValue(fams["ib_slurm_unresolved_sluid_allocations"]); v != 0 {
		t.Errorf("titles should resolve every SLUID, unresolved = %v", v)
	}
	if _, ok := fams["ib_slurm_metadata_refresh_errors_total"]; ok {
		t.Error("no squeue, no squeue health metrics")
	}

	// Broken: failure is counted and visible, output still valid.
	broken := writeScript(t, "squeue", "echo 'Unable to contact slurm controller' >&2\nexit 1\n")
	fams, stderr := runOnce(t, append(base, "--squeue-path", broken)...)
	if v, _ := metricValue(fams["ib_slurm_metadata_refresh_errors_total"]); v < 1 {
		t.Errorf("refresh errors = %v", v)
	}
	if v, _ := metricValue(fams["ib_slurm_metadata_last_success_timestamp_seconds"]); v != 0 {
		t.Errorf("never succeeded: timestamp = %v, want 0", v)
	}
	if !strings.Contains(stderr, "squeue") {
		t.Error("squeue failure should be logged")
	}
}

func TestFlagValidation(t *testing.T) {
	for _, args := range [][]string{
		{"--squeue-path", "squeue"},
		{"--ownership", "magic"},
		{"--demo-layout", "v3"},
		{"--ownership", "qp", "--rdma-path", "rdma"},
		{"stray-argument"},
		// 0 reads like "off" but would run squeue back to back on every node.
		{"--squeue-interval", "0"},
		{"--squeue-interval", "-1s"},
		{"--squeue-interval", "9s"},
	} {
		var out, errb bytes.Buffer
		if err := run(context.Background(), append(args, "--once"), &out, &errb); err == nil {
			t.Errorf("%v: want an error", args)
		}
	}
}

func TestSqueueIntervalAtTheMinimumIsAccepted(t *testing.T) {
	o, err := parseFlags([]string{"--squeue-interval", slurm.MinInterval.String()}, io.Discard)
	if err != nil || o.squeueInterval != slurm.MinInterval {
		t.Fatalf("got %v, %v", o.squeueInterval, err)
	}
}

func TestVersion(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"--version"}, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out.String()) != version {
		t.Fatalf("got %q", out.String())
	}
}

// blockingCollector holds Collect open until released, to exercise the
// in-flight limit.
type blockingCollector struct {
	desc    *prometheus.Desc
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingCollector) Describe(ch chan<- *prometheus.Desc) { ch <- b.desc }
func (b *blockingCollector) Collect(ch chan<- prometheus.Metric) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	ch <- prometheus.MustNewConstMetric(b.desc, prometheus.GaugeValue, 1)
}

func TestMetricsEndpointLimitsConcurrency(t *testing.T) {
	bc := &blockingCollector{
		desc:    prometheus.NewDesc("ib_slurm_test", "test", nil, nil),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(bc)
	srv := httptest.NewServer(newMux(reg, 1, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer srv.Close()

	first := make(chan int, 1)
	go func() {
		resp, err := http.Get(srv.URL + "/metrics")
		if err != nil {
			first <- 0
			return
		}
		resp.Body.Close()
		first <- resp.StatusCode
	}()
	<-bc.entered
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("second concurrent scrape: status %d, want 503", resp.StatusCode)
	}
	close(bc.release)
	if code := <-first; code != http.StatusOK {
		t.Errorf("first scrape: status %d", code)
	}

	resp, err = http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz: %d", resp.StatusCode)
	}
}

func TestMetricsEndpointServesTheCollector(t *testing.T) {
	roots, err := fixture.Build(t.TempDir(), fixture.Default(fixture.V2))
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collector.New(collector.Config{
		Scanner: cgroup.NewScanner(roots.Cgroup),
		Reader:  ib.NewReader(roots.Sys),
		Mapper:  procfd.NewMapper(roots.Proc, roots.Verbs),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}))
	srv := httptest.NewServer(newMux(reg, 2, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `ib_slurm_device_jobs{device="mlx5_1",port="1"} 2`) {
		t.Fatalf("unexpected body:\n%s", body)
	}
}

// ── Dashboard ──────────────────────────────────────────────────────────────

var (
	descNameRe   = regexp.MustCompile(`fqName: "([^"]+)"`)
	descLabelsRe = regexp.MustCompile(`variableLabels: \{([^}]*)\}`)
	metricRe     = regexp.MustCompile(`\bib_slurm_[a-z_]+`)
	counterRe    = regexp.MustCompile(`counter=~?"([^"]+)"`)
	byRe         = regexp.MustCompile(`by \(([^)]*)\)`)
	legendRe     = regexp.MustCompile(`\{\{\s*([a-z_]+)\s*\}\}`)
	// A selector: metric{matchers}. Matcher values are quoted strings.
	selectorRe = regexp.MustCompile(`\b(ib_slurm_[a-z_]+)\{([^}]*)\}`)
	matcherRe  = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=~|!~|!=|=)\s*"(?:[^"\\]|\\.)*"`)
	// label_values(metric[{...}], label) in a template variable.
	labelValuesRe = regexp.MustCompile(`label_values\((ib_slurm_[a-z_]+)(?:\{[^}]*\})?,\s*([a-zA-Z_]+)\)`)
	// An aggregation with no by() before its argument, e.g. sum(...).
	bareAggRe = regexp.MustCompile(`\b(sum|count|avg|min|max|group|topk|bottomk|quantile|stddev|stdvar)\s*\(`)
)

// exporterMetrics returns every metric the exporter can emit, with its labels.
func exporterMetrics(t *testing.T) map[string]map[string]bool {
	t.Helper()
	ch := make(chan *prometheus.Desc, 64)
	collector.New(collector.Config{}).Describe(ch)
	slurm.New("/x", "", nil).Describe(ch)
	close(ch)
	out := map[string]map[string]bool{}
	for d := range ch {
		s := d.String()
		name := descNameRe.FindStringSubmatch(s)
		if name == nil {
			t.Fatalf("cannot parse %s", s)
		}
		labels := map[string]bool{}
		if m := descLabelsRe.FindStringSubmatch(s); m != nil {
			for _, l := range strings.Split(m[1], ",") {
				if l = strings.TrimSpace(l); l != "" {
					labels[l] = true
				}
			}
		}
		out[name[1]] = labels
	}
	return out
}

func TestDashboardQueriesMatchTheExporter(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "grafana-dashboard.json"))
	if err != nil {
		t.Fatal(err)
	}
	var dash struct {
		Templating struct {
			List []struct {
				Name  string `json:"name"`
				Query string `json:"query"`
			} `json:"list"`
		} `json:"templating"`
		Panels []struct {
			Title   string `json:"title"`
			Type    string `json:"type"`
			Targets []struct {
				Expr   string `json:"expr"`
				Legend string `json:"legendFormat"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("dashboard is not valid JSON: %v", err)
	}

	known := exporterMetrics(t)
	counters := map[string]bool{}
	for _, c := range ib.AllCounterNames() {
		counters[c] = true
	}
	// Labels Prometheus adds at scrape time.
	targetLabels := map[string]bool{"instance": true, "job": true}

	check := func(where, panelType, expr, legend string) {
		names := metricRe.FindAllString(expr, -1)
		if len(names) == 0 {
			t.Errorf("%s: no ib_slurm metric in %q", where, expr)
		}
		// A label typo inside a selector ({jobid=~"$job_id"}) matches no
		// series, so the panel silently shows nothing.
		for _, sel := range selectorRe.FindAllStringSubmatch(expr, -1) {
			ls, ok := known[sel[1]]
			if !ok {
				continue // reported below
			}
			for _, m := range matcherRe.FindAllStringSubmatch(sel[2], -1) {
				if !ls[m[1]] && !targetLabels[m[1]] {
					t.Errorf("%s: selector label %q is not on %s", where, m[1], sel[1])
				}
			}
		}
		for _, m := range labelValuesRe.FindAllStringSubmatch(expr, -1) {
			if ls, ok := known[m[1]]; ok && !ls[m[2]] && !targetLabels[m[2]] {
				t.Errorf("%s: label_values label %q is not on %s", where, m[2], m[1])
			}
		}
		// Per-series panels keep instance (checked on by() below); an
		// aggregation without by() drops it, which is only acceptable in a
		// stat that is documented as a total across the selected nodes.
		if bareAggRe.MatchString(expr) && panelType != "stat" {
			t.Errorf("%s: aggregation without by (instance, ...) outside a stat panel: %q", where, expr)
		}
		labels := map[string]bool{}
		for k := range targetLabels {
			labels[k] = true
		}
		for _, n := range names {
			ls, ok := known[n]
			if !ok {
				t.Errorf("%s: unknown metric %s", where, n)
				continue
			}
			for l := range ls {
				labels[l] = true
			}
		}
		for _, m := range counterRe.FindAllStringSubmatch(expr, -1) {
			for _, c := range strings.Split(m[1], "|") {
				if !counters[c] {
					t.Errorf("%s: counter %q is not collected (internal/ib)", where, c)
				}
			}
		}
		for _, m := range byRe.FindAllStringSubmatch(expr, -1) {
			if !strings.Contains(m[1], "instance") {
				t.Errorf("%s: aggregation drops instance, so the failing node cannot be identified: %q", where, m[0])
			}
			for _, l := range strings.Split(m[1], ",") {
				if l = strings.TrimSpace(l); !labels[l] {
					t.Errorf("%s: by() label %q is not on %v", where, l, names)
				}
			}
		}
		for _, m := range legendRe.FindAllStringSubmatch(legend, -1) {
			if !labels[m[1]] {
				t.Errorf("%s: legend label %q is not on %v", where, m[1], names)
			}
		}
	}

	for _, v := range dash.Templating.List {
		check("variable "+v.Name, "variable", v.Query, "")
	}
	n := 0
	for _, p := range dash.Panels {
		for _, tg := range p.Targets {
			check("panel "+p.Title, p.Type, tg.Expr, tg.Legend)
			n++
		}
	}
	if n < 10 {
		t.Fatalf("only %d queries found; is the dashboard structure what this test expects?", n)
	}
}
