package collector

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/cgroup"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/fixture"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/ib"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/procfd"
)

type staticMeta map[string]JobMeta

func (m staticMeta) Meta(jobID string) (JobMeta, bool) {
	v, ok := m[jobID]
	return v, ok
}

type fixtureResolver struct{}

func (fixtureResolver) ResolveSLUID(s string) (string, error) {
	if id, ok := fixture.SLUIDToJob(s); ok {
		return id, nil
	}
	return "", io.EOF
}

func build(t *testing.T, layout fixture.Layout) (*Collector, *cgroup.Scanner) {
	t.Helper()
	roots, err := fixture.Build(t.TempDir(), fixture.Default(layout))
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	sc := cgroup.NewScanner(roots.Cgroup)
	c := New(sc,
		ib.NewReader(roots.Sys),
		procfd.NewMapper(roots.Proc, roots.Verbs),
		staticMeta{
			"918001": {User: "alice", Account: "research", Partition: "gpu"},
			"918002": {User: "bob", Account: "infra", Partition: "gpu"},
			"918003": {User: "carol", Account: "research", Partition: "gpu"},
			"918004": {User: "dave", Account: "trading", Partition: "roce"},
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	return c, sc
}

// gather collects into a map keyed by metric name.
func gather(t *testing.T, c prometheus.Collector) map[string][]*dto.Metric {
	t.Helper()
	r := prometheus.NewRegistry()
	if err := r.Register(c); err != nil {
		t.Fatalf("register: %v", err)
	}
	families, err := r.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string][]*dto.Metric{}
	for _, f := range families {
		out[f.GetName()] = f.GetMetric()
	}
	return out
}

func label(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

// ── The attribution rule ───────────────────────────────────────────────────

func TestSoleUserIsAttributed(t *testing.T) {
	c, _ := build(t, fixture.V2)
	got := gather(t, c)

	found := false
	for _, m := range got["ib_slurm_job_counter_total"] {
		if label(m, "job_id") == "918001" && label(m, "device") == "mlx5_0" {
			found = true
			if u := label(m, "user"); u != "alice" {
				t.Errorf("expected user alice, got %q", u)
			}
		}
	}
	if !found {
		t.Fatal("job 918001 is the sole user of mlx5_0 and should be attributed")
	}
}

func TestSharedDeviceIsNeverAttributed(t *testing.T) {
	// 918002 and 918003 both hold mlx5_1. Splitting its counters between them
	// is impossible, so neither may claim them.
	c, _ := build(t, fixture.V2)
	got := gather(t, c)

	for _, m := range got["ib_slurm_job_counter_total"] {
		if dev := label(m, "device"); dev == "mlx5_1" {
			t.Fatalf("mlx5_1 is shared; job %s must not be attributed its counters",
				label(m, "job_id"))
		}
	}
}

func TestSharedDeviceStillReportsDeviceLevelCounters(t *testing.T) {
	// Suppressing attribution must not lose the data — the retries on mlx5_1
	// are the whole reason someone opens this dashboard.
	c, _ := build(t, fixture.V2)
	got := gather(t, c)

	var seq float64 = -1
	for _, m := range got["ib_slurm_device_counter_total"] {
		if label(m, "device") == "mlx5_1" && label(m, "counter") == "packet_seq_err" {
			seq = m.GetCounter().GetValue()
		}
	}
	if seq != 48221 {
		t.Fatalf("expected packet_seq_err 48221 at device level, got %v", seq)
	}
}

func TestDeviceJobsCountExplainsMissingAttribution(t *testing.T) {
	c, _ := build(t, fixture.V2)
	got := gather(t, c)

	counts := map[string]float64{}
	for _, m := range got["ib_slurm_device_jobs"] {
		counts[label(m, "device")] = m.GetGauge().GetValue()
	}
	if counts["mlx5_1"] != 2 {
		t.Errorf("mlx5_1 should report 2 jobs, got %v", counts["mlx5_1"])
	}
	if counts["mlx5_0"] != 1 {
		t.Errorf("mlx5_0 should report 1 job, got %v", counts["mlx5_0"])
	}
}

func TestUnattributedJobsAreCounted(t *testing.T) {
	c, _ := build(t, fixture.V2)
	got := gather(t, c)

	m := got["ib_slurm_unattributed_jobs"]
	if len(m) != 1 {
		t.Fatal("expected the unattributed gauge")
	}
	if v := m[0].GetGauge().GetValue(); v != 2 {
		t.Fatalf("918002 and 918003 share a device; expected 2 unattributed, got %v", v)
	}
}

// ── cgroup layouts ─────────────────────────────────────────────────────────

func TestDetectsV2Layout(t *testing.T) {
	_, sc := build(t, fixture.V2)
	_, layout, err := sc.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if layout != cgroup.LayoutV2 {
		t.Fatalf("expected %s, got %s", cgroup.LayoutV2, layout)
	}
}

func TestDetectsV1Layout(t *testing.T) {
	_, sc := build(t, fixture.V1)
	allocs, layout, err := sc.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if layout != cgroup.LayoutV1 {
		t.Fatalf("expected %s, got %s", cgroup.LayoutV1, layout)
	}
	if len(allocs) != 4 {
		t.Fatalf("expected 4 allocations, got %d", len(allocs))
	}
	for _, a := range allocs {
		if a.UID != 1000 {
			t.Errorf("v1 layout encodes uid in the path; got %d", a.UID)
		}
	}
}

func TestSLUIDLayoutIsDetectedNotMisparsed(t *testing.T) {
	// The Slurm 26.05 regression this tool exists to survive: the directory is
	// keyed by SLUID, so anything doing Atoi on it silently breaks.
	_, sc := build(t, fixture.V2SLUID)
	allocs, layout, err := sc.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if layout != cgroup.LayoutV2SLUID {
		t.Fatalf("expected %s, got %s", cgroup.LayoutV2SLUID, layout)
	}
	for _, a := range allocs {
		if !a.IsSLUID {
			t.Errorf("%q should be flagged as a SLUID", a.Identifier)
		}
		if a.JobID != "" {
			t.Errorf("a SLUID must not be used as a job ID without resolution, got %q", a.JobID)
		}
	}
}

func TestSLUIDResolvesToJobID(t *testing.T) {
	_, sc := build(t, fixture.V2SLUID)
	allocs, _, _ := sc.Scan()

	resolved := cgroup.Resolve(allocs, fixtureResolver{})
	for _, a := range resolved {
		if a.JobID == "" {
			t.Fatalf("%q should resolve", a.Identifier)
		}
		if strings.HasPrefix(a.JobID, "s") {
			t.Fatalf("resolved to a SLUID rather than a job id: %q", a.JobID)
		}
	}
}

func TestUnresolvedSLUIDsAreCountedNotGuessed(t *testing.T) {
	c, _ := build(t, fixture.V2SLUID)
	got := gather(t, c)

	m := got["ib_slurm_unresolved_sluid_allocations"]
	if len(m) != 1 {
		t.Fatal("expected the unresolved gauge")
	}
	// No resolver is wired into the collector here, so all four stay unresolved
	// rather than being labelled with a fabricated job id.
	if v := m[0].GetGauge().GetValue(); v != 4 {
		t.Fatalf("expected 4 unresolved, got %v", v)
	}
	if len(got["ib_slurm_job_counter_total"]) != 0 {
		t.Fatal("unresolved allocations must not produce job-labelled series")
	}
}

// ── PID → device mapping ───────────────────────────────────────────────────

func TestPIDsResolveThroughUverbsNotByNameGuess(t *testing.T) {
	roots, err := fixture.Build(t.TempDir(), fixture.Default(fixture.V2))
	if err != nil {
		t.Fatal(err)
	}
	m := procfd.NewMapper(roots.Proc, roots.Verbs)

	if got := m.DevicesForPID(41001); len(got) != 1 || got[0] != "mlx5_0" {
		t.Fatalf("pid 41001 should map to mlx5_0, got %v", got)
	}
	if got := m.DevicesForPIDs([]int{42001, 43001}); len(got) != 1 || got[0] != "mlx5_1" {
		t.Fatalf("both pids share mlx5_1, got %v", got)
	}
	if got := m.DevicesForPID(999999); len(got) != 0 {
		t.Fatalf("a dead pid should yield nothing, got %v", got)
	}
}

// ── Counter reading ────────────────────────────────────────────────────────

func TestRoCEPortIsIdentifiedAndCarriesPauseCounters(t *testing.T) {
	roots, _ := fixture.Build(t.TempDir(), fixture.Default(fixture.V2))
	r := ib.NewReader(roots.Sys)

	c, err := r.Read("mlx5_2", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !c.IsRoCE() {
		t.Error("mlx5_2 has link_layer Ethernet and should be RoCE")
	}
	if !c.IsActive() {
		t.Error("port state '4: ACTIVE' should parse as active")
	}
	if c.Values["rx_pause"] != 88412 {
		t.Errorf("rx_pause = %d", c.Values["rx_pause"])
	}
	if c.Values["np_ecn_marked_roce_packets"] != 441882 {
		t.Errorf("ECN marks missing")
	}
}

func TestMissingCountersAreAbsentNotZero(t *testing.T) {
	// A counter this kernel does not expose and a counter that is genuinely
	// zero are different facts. Defaulting the former to 0 invents data.
	roots, _ := fixture.Build(t.TempDir(), fixture.Default(fixture.V2))
	r := ib.NewReader(roots.Sys)

	c, _ := r.Read("mlx5_0", 1)
	if _, present := c.Values["rx_pause"]; present {
		t.Error("mlx5_0 has no rx_pause in the fixture; it must be absent, not 0")
	}
	if v, present := c.Values["port_xmit_discards"]; !present || v != 0 {
		t.Error("a real zero must be present with value 0")
	}
}

func TestErrorCounterClassification(t *testing.T) {
	for _, name := range []string{"packet_seq_err", "link_downed", "port_rcv_errors"} {
		if !ib.IsErrorCounter(name) {
			t.Errorf("%s should classify as an error counter", name)
		}
	}
	for _, name := range []string{"port_xmit_data", "np_cnp_sent", "rx_pause_duration"} {
		if ib.IsErrorCounter(name) {
			t.Errorf("%s is volume, not an error", name)
		}
	}
}

func TestDeviceLevelSeriesAlwaysPresent(t *testing.T) {
	c, _ := build(t, fixture.V2)
	got := gather(t, c)

	devices := map[string]bool{}
	for _, m := range got["ib_slurm_device_port_up"] {
		devices[label(m, "device")] = true
	}
	for _, want := range []string{"mlx5_0", "mlx5_1", "mlx5_2"} {
		if !devices[want] {
			t.Errorf("%s missing from device-level series", want)
		}
	}
}
