package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/cgroup"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/fixture"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/ib"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/procfd"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/rdmares"
)

// Every tree in these tests is synthetic (package fixture): shapes from the
// Slurm and kernel docs, values invented. Nothing here ran on a Slurm node or
// an HCA.

type staticMeta map[string]JobMeta

func (m staticMeta) Meta(jobID string) (JobMeta, bool) {
	v, ok := m[jobID]
	return v, ok
}

var defaultMeta = staticMeta{
	"918001": {User: "alice", Account: "research", Partition: "gpu"},
	"918002": {User: "bob", Account: "infra", Partition: "gpu"},
	"918003": {User: "carol", Account: "research", Partition: "gpu"},
	"918004": {User: "dave", Account: "trading", Partition: "roce"},
}

type fixtureResolver struct {
	mu    sync.Mutex
	calls int
	fn    func(string) (string, bool)
}

func (r *fixtureResolver) ResolveSLUID(s string) (string, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	fn := r.fn
	if fn == nil {
		fn = fixture.SLUIDToJob
	}
	if id, ok := fn(s); ok {
		return id, nil
	}
	return "", io.EOF
}

type qpSource struct {
	mu  sync.Mutex
	qps []rdmares.QP
	err error
}

func (q *qpSource) QPs(context.Context) ([]rdmares.QP, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.qps, q.err
}

type env struct {
	t     *testing.T
	roots fixture.Roots
	spec  fixture.Spec
	c     *Collector
}

func newEnv(t *testing.T, spec fixture.Spec, tweak func(*Config)) *env {
	t.Helper()
	roots, err := fixture.Build(t.TempDir(), spec)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	cfg := Config{
		Scanner: cgroup.NewScanner(roots.Cgroup),
		Reader:  ib.NewReader(roots.Sys),
		Mapper:  procfd.NewMapper(roots.Proc, roots.Verbs),
		Meta:    defaultMeta,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if tweak != nil {
		tweak(&cfg)
	}
	return &env{t: t, roots: roots, spec: spec, c: New(cfg)}
}

func build(t *testing.T, layout fixture.Layout) *env {
	return newEnv(t, fixture.Default(layout), nil)
}

// gather collects into a map keyed by metric name.
func (e *env) gather() map[string][]*dto.Metric {
	e.t.Helper()
	r := prometheus.NewRegistry()
	if err := r.Register(e.c); err != nil {
		e.t.Fatalf("register: %v", err)
	}
	families, err := r.Gather()
	if err != nil {
		e.t.Fatalf("gather: %v", err)
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

func value(m *dto.Metric) float64 {
	if m.GetCounter() != nil {
		return m.GetCounter().GetValue()
	}
	return m.GetGauge().GetValue()
}

// find returns the value of the series matching all labels, and whether one
// exists.
func find(ms []*dto.Metric, labels ...string) (float64, bool) {
	for _, m := range ms {
		ok := true
		for i := 0; i+1 < len(labels); i += 2 {
			if label(m, labels[i]) != labels[i+1] {
				ok = false
				break
			}
		}
		if ok {
			return value(m), true
		}
	}
	return 0, false
}

func (e *env) scalar(got map[string][]*dto.Metric, name string, labels ...string) float64 {
	e.t.Helper()
	v, ok := find(got[name], labels...)
	if !ok {
		e.t.Fatalf("%s%v not emitted", name, labels)
	}
	return v
}

func (e *env) setCounter(dev string, port int, name string, v uint64) {
	e.t.Helper()
	if err := os.WriteFile(fixture.CounterPath(e.roots, dev, port, name), []byte(strconv.FormatUint(v, 10)), 0o644); err != nil {
		e.t.Fatal(err)
	}
}

// jobProcs is the leaf cgroup.procs holding a job's user processes.
func (e *env) jobProcs(job string) string {
	switch e.spec.Layout {
	case fixture.V1:
		return filepath.Join(e.roots.Cgroup, "freezer", "slurm", "uid_1000", "job_"+job, "step_0", "cgroup.procs")
	case fixture.V2SLUID:
		return filepath.Join(e.roots.Cgroup, fixture.DefaultScope, fixture.SLUIDFor(job), "step_0", "user", "task_0", "cgroup.procs")
	default:
		return filepath.Join(e.roots.Cgroup, fixture.DefaultScope, "job_"+job, "step_0", "user", "task_0", "cgroup.procs")
	}
}

func (e *env) setJobPIDs(job string, pids ...int) {
	e.t.Helper()
	var b strings.Builder
	for _, p := range pids {
		b.WriteString(strconv.Itoa(p) + "\n")
	}
	if err := os.WriteFile(e.jobProcs(job), []byte(b.String()), 0o644); err != nil {
		e.t.Fatal(err)
	}
}

// addProc fakes /proc/<pid> holding the given uverbs indexes open.
func (e *env) addProc(pid int, uverbs ...int) {
	e.t.Helper()
	fd := filepath.Join(e.roots.Proc, strconv.Itoa(pid), "fd")
	if err := os.MkdirAll(fd, 0o755); err != nil {
		e.t.Fatal(err)
	}
	for i, idx := range uverbs {
		target := filepath.Join(e.roots.Base, "dev", "infiniband", "uverbs"+strconv.Itoa(idx))
		if err := os.Symlink(target, filepath.Join(fd, strconv.Itoa(10+i))); err != nil {
			e.t.Fatal(err)
		}
	}
}

func skipIfRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions; this test needs an unprivileged user")
	}
}

// ── The attribution rule ───────────────────────────────────────────────────

func TestSoleUserIsAttributed(t *testing.T) {
	e := build(t, fixture.V2)
	got := e.gather()

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
	if v := e.scalar(got, "ib_slurm_job_device_attributed", "job_id", "918001", "device", "mlx5_0"); v != 1 {
		t.Errorf("job_device_attributed = %v", v)
	}
}

func TestSharedDeviceIsNeverAttributed(t *testing.T) {
	// 918002 and 918003 both hold mlx5_1. Splitting its counters between them
	// is impossible, so neither may claim them.
	e := build(t, fixture.V2)
	got := e.gather()

	for _, m := range got["ib_slurm_job_counter_total"] {
		if dev := label(m, "device"); dev == "mlx5_1" {
			t.Fatalf("mlx5_1 is shared; job %s must not be attributed its counters",
				label(m, "job_id"))
		}
	}
	for _, job := range []string{"918002", "918003"} {
		if v := e.scalar(got, "ib_slurm_job_device_attributed", "job_id", job, "device", "mlx5_1"); v != 0 {
			t.Errorf("%s: job_device_attributed = %v, want 0", job, v)
		}
	}
}

func TestSharedDeviceStillReportsDeviceLevelCounters(t *testing.T) {
	// Suppressing attribution must not lose the data — the retries on mlx5_1
	// are the whole reason someone opens this dashboard.
	e := build(t, fixture.V2)
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_device_counter_total", "device", "mlx5_1", "counter", "packet_seq_err"); v != 48221 {
		t.Fatalf("expected packet_seq_err 48221 at device level, got %v", v)
	}
}

func TestDeviceJobsCountExplainsMissingAttribution(t *testing.T) {
	e := build(t, fixture.V2)
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_device_jobs", "device", "mlx5_1"); v != 2 {
		t.Errorf("mlx5_1 should report 2 jobs, got %v", v)
	}
	if v := e.scalar(got, "ib_slurm_device_jobs", "device", "mlx5_0"); v != 1 {
		t.Errorf("mlx5_0 should report 1 job, got %v", v)
	}
}

func TestIdleDeviceReportsZeroJobsNotAbsent(t *testing.T) {
	// Audit reproduction: an idle, up HCA used to have no device_jobs series,
	// so "absent" and "0 jobs" looked the same.
	spec := fixture.Default(fixture.V2)
	spec.Devices["mlx5_3"] = 3
	spec.Counters["mlx5_3:1"] = map[string]uint64{"port_xmit_data": 5}
	e := newEnv(t, spec, nil)
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_device_jobs", "device", "mlx5_3", "port", "1"); v != 0 {
		t.Fatalf("idle mlx5_3: device_jobs = %v, want a real 0", v)
	}
}

func TestUnattributedJobsAreCounted(t *testing.T) {
	e := build(t, fixture.V2)
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_unattributed_jobs"); v != 2 {
		t.Fatalf("918002 and 918003 share a device; expected 2 unattributed, got %v", v)
	}
}

func TestPartialAttributionIsVisible(t *testing.T) {
	// A job on mlx5_0 (sole) and mlx5_1 (shared) is attributed on one HCA
	// only. job_device_attributed says so per port.
	spec := fixture.Default(fixture.V2)
	spec.PIDDevices[41002] = []int{0, 1}
	e := newEnv(t, spec, nil)
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_job_device_attributed", "job_id", "918001", "device", "mlx5_0"); v != 1 {
		t.Errorf("mlx5_0: %v", v)
	}
	if v := e.scalar(got, "ib_slurm_job_device_attributed", "job_id", "918001", "device", "mlx5_1"); v != 0 {
		t.Errorf("mlx5_1 is shared by three jobs now: %v", v)
	}
}

// ── Slurm 26.05 and SLUID resolution ───────────────────────────────────────

func TestSlurm2605DefaultLayoutIsAttributedWithoutController(t *testing.T) {
	// Critical regression: on a default 26.05 node the old exporter found 0
	// allocations. Here, with no resolver at all, the SLUIDs resolve from the
	// slurmstepd process titles and attribution works.
	e := build(t, fixture.V2SLUID)
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_cgroup_layout_info", "layout", "cgroup-v2-sluid"); v != 1 {
		t.Fatal("layout not detected")
	}
	if v := e.scalar(got, "ib_slurm_unresolved_sluid_allocations"); v != 0 {
		t.Errorf("unresolved = %v, want 0", v)
	}
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918001", "device", "mlx5_0"); !ok {
		t.Error("918001 should be attributed on mlx5_0")
	}
	if v := e.scalar(got, "ib_slurm_unattributed_jobs"); v != 2 {
		t.Errorf("unattributed = %v, want 2", v)
	}
}

func TestSLUIDWithNoTitleAndNoResolverIsCountedNotGuessed(t *testing.T) {
	spec := fixture.Default(fixture.V2SLUID)
	spec.OmitStepd = true
	e := newEnv(t, spec, nil)
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_unresolved_sluid_allocations"); v != 4 {
		t.Fatalf("expected 4 unresolved, got %v", v)
	}
	if len(got["ib_slurm_job_counter_total"]) != 0 {
		t.Fatal("unresolved allocations must not produce job-labelled series")
	}
	// Unresolved allocations still hold devices: mlx5_1 is still shared.
	if v := e.scalar(got, "ib_slurm_device_jobs", "device", "mlx5_1"); v != 2 {
		t.Errorf("device_jobs on mlx5_1 = %v, want 2", v)
	}
}

func TestUnresolvedAllocationStillMakesADeviceShared(t *testing.T) {
	// 918002 resolves; 918003 (same HCA) does not. Before, the unresolved one
	// was dropped from the user count and 918002 looked like the sole user
	// of mlx5_1 — absorbing 918003's traffic.
	spec := fixture.Default(fixture.V2SLUID)
	spec.OmitStepd = true
	e := newEnv(t, spec, func(c *Config) {
		c.Resolver = &fixtureResolver{fn: func(s string) (string, bool) {
			id, ok := fixture.SLUIDToJob(s)
			return id, ok && id != "918003"
		}}
	})
	got := e.gather()
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918002"); ok {
		t.Fatal("918002 shares mlx5_1 with an unresolved allocation and must not be attributed")
	}
	if v := e.scalar(got, "ib_slurm_unresolved_sluid_allocations"); v != 1 {
		t.Errorf("unresolved = %v", v)
	}
}

func TestResolverDisagreeingWithTitleLeavesItUnresolved(t *testing.T) {
	e := newEnv(t, fixture.Default(fixture.V2SLUID), func(c *Config) {
		c.Resolver = &fixtureResolver{fn: func(s string) (string, bool) { return "1", true }}
	})
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_unresolved_sluid_allocations"); v != 4 {
		t.Fatalf("conflicting sources must leave SLUIDs unresolved, got %v", v)
	}
	if len(got["ib_slurm_job_counter_total"]) != 0 {
		t.Fatal("no job series on conflicting evidence")
	}
}

func TestWarmCacheMakesNoResolverCalls(t *testing.T) {
	// Audit reproduction: 3 scrapes × 4 SLUIDs used to be 12 scontrol execs.
	spec := fixture.Default(fixture.V2SLUID)
	spec.OmitStepd = true // force the resolver path
	r := &fixtureResolver{}
	e := newEnv(t, spec, func(c *Config) { c.Resolver = r })
	e.gather()
	if r.calls != 4 {
		t.Fatalf("first scrape: want 4 resolver calls, got %d", r.calls)
	}
	for i := 0; i < 3; i++ {
		e.gather()
	}
	if r.calls != 4 {
		t.Fatalf("warm cache: want no further resolver calls, got %d total", r.calls)
	}
}

func TestSLUIDCacheIsPrunedWhenTheCgroupGoes(t *testing.T) {
	e := build(t, fixture.V2SLUID)
	e.gather()
	if len(e.c.sluidCache) != 4 {
		t.Fatalf("cache = %v", e.c.sluidCache)
	}
	dir := filepath.Join(e.roots.Cgroup, fixture.DefaultScope, fixture.SLUIDFor("918004"))
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	e.gather()
	if _, ok := e.c.sluidCache[fixture.SLUIDFor("918004")]; ok || len(e.c.sluidCache) != 3 {
		t.Fatalf("ended job's SLUID must leave the cache: %v", e.c.sluidCache)
	}
}

// ── Failing safe on incomplete evidence ────────────────────────────────────

func TestProcPermissionDeniedFailsSafeAndIsVisible(t *testing.T) {
	// Audit reproduction: with every /proc/<pid>/fd unreadable the exporter
	// reported unattributed_jobs=0 and scrape_error=0 — indistinguishable
	// from a node where no job uses RDMA.
	skipIfRoot(t)
	e := build(t, fixture.V2)
	for pids := range e.spec.PIDDevices {
		fd := filepath.Join(e.roots.Proc, strconv.Itoa(pids), "fd")
		if err := os.Chmod(fd, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(fd, 0o755) })
	}
	got := e.gather()
	if len(got["ib_slurm_job_counter_total"]) != 0 {
		t.Error("no job series may be emitted when descriptors cannot be read")
	}
	if v := e.scalar(got, "ib_slurm_unattributed_jobs"); v != 4 {
		t.Errorf("unattributed = %v, want all 4 jobs", v)
	}
	if v := e.scalar(got, "ib_slurm_pid_fd_unreadable"); v != 6 {
		t.Errorf("pid_fd_unreadable = %v, want 6", v)
	}
	if v := e.scalar(got, "ib_slurm_scrape_error", "source", "procfd"); v != 1 {
		t.Errorf("scrape_error{procfd} = %v", v)
	}
	// Device-level truth is unaffected.
	if v := e.scalar(got, "ib_slurm_device_counter_total", "device", "mlx5_1", "counter", "packet_seq_err"); v != 48221 {
		t.Errorf("device counter = %v", v)
	}
}

func TestOneUnreadableProcessSuppressesEveryJob(t *testing.T) {
	// If 918003's descriptors cannot be read, 918002 would look like
	// mlx5_1's sole user and be charged 918003's traffic. So nothing is
	// attributed until the evidence is complete.
	skipIfRoot(t)
	e := build(t, fixture.V2)
	fd := filepath.Join(e.roots.Proc, "43001", "fd")
	if err := os.Chmod(fd, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(fd, 0o755) })
	got := e.gather()
	if len(got["ib_slurm_job_counter_total"]) != 0 {
		t.Fatal("an incomplete user set must suppress all job attribution")
	}
	if v := e.scalar(got, "ib_slurm_pid_fd_unreadable"); v != 1 {
		t.Errorf("pid_fd_unreadable = %v", v)
	}
}

func TestHiddenProcessIsNotMistakenForExited(t *testing.T) {
	// hidepid=2 (or another PID namespace): the PID is in cgroup.procs but
	// /proc/<pid> does not exist. That is missing evidence, not an exit.
	e := build(t, fixture.V2)
	if err := os.RemoveAll(filepath.Join(e.roots.Proc, "43001")); err != nil {
		t.Fatal(err)
	}
	got := e.gather()
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918002"); ok {
		t.Fatal("918002 must not become sole user because 918003 is invisible")
	}
	if v := e.scalar(got, "ib_slurm_pid_fd_unreadable"); v != 1 {
		t.Errorf("pid_fd_unreadable = %v", v)
	}
}

func TestExitedProcessIsBenign(t *testing.T) {
	// 918003's only process exits: gone from /proc and from its cgroup.
	// 918002 is then genuinely mlx5_1's sole user.
	e := build(t, fixture.V2)
	if err := os.RemoveAll(filepath.Join(e.roots.Proc, "43001")); err != nil {
		t.Fatal(err)
	}
	e.setJobPIDs("918003")
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_pid_fd_unreadable"); v != 0 {
		t.Errorf("an exited process is not unreadable: %v", v)
	}
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918002", "device", "mlx5_1"); !ok {
		t.Error("918002 is now the sole user of mlx5_1")
	}
}

func TestUnreadableCgroupFailsSafe(t *testing.T) {
	skipIfRoot(t)
	e := build(t, fixture.V2)
	user := filepath.Dir(filepath.Dir(e.jobProcs("918003")))
	if err := os.Chmod(user, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(user, 0o755) })
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_scrape_error", "source", "cgroup"); v != 1 {
		t.Errorf("scrape_error{cgroup} = %v", v)
	}
	if len(got["ib_slurm_job_counter_total"]) != 0 {
		t.Fatal("missing PIDs must suppress attribution")
	}
}

func TestScopeFoundSeparatesIdleFromMisconfigured(t *testing.T) {
	spec := fixture.Default(fixture.V2SLUID)
	spec.Jobs = nil
	idle := newEnv(t, spec, nil).gather()
	if v, _ := find(idle["ib_slurm_cgroup_scope_found"]); v != 1 {
		t.Errorf("idle node: scope_found = %v", v)
	}
	if v, _ := find(idle["ib_slurm_scrape_error"], "source", "cgroup"); v != 0 {
		t.Errorf("idle node is not an error")
	}

	wrong := newEnv(t, fixture.Default(fixture.V2), func(c *Config) {
		c.Scanner = cgroup.NewScanner(filepath.Join(t.TempDir(), "not-a-cgroup-root"))
	}).gather()
	if v, _ := find(wrong["ib_slurm_cgroup_scope_found"]); v != 0 {
		t.Errorf("wrong root: scope_found = %v", v)
	}
	if v, _ := find(wrong["ib_slurm_scrape_error"], "source", "cgroup"); v != 1 {
		t.Errorf("wrong root: scrape_error{cgroup} = %v", v)
	}
}

func TestSysfsFailureKeepsCgroupStateVisible(t *testing.T) {
	e := newEnv(t, fixture.Default(fixture.V2), func(c *Config) {
		c.Reader = ib.NewReader(filepath.Join(t.TempDir(), "no-sysfs"))
	})
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_scrape_error", "source", "sysfs"); v != 1 {
		t.Errorf("scrape_error{sysfs} = %v", v)
	}
	if len(got["ib_slurm_job_processes"]) != 4 {
		t.Error("job_processes should still be reported")
	}
	if len(got["ib_slurm_job_counter_total"]) != 0 {
		t.Error("no counters, no attribution")
	}
}

// ── Accumulated job counters ───────────────────────────────────────────────

func TestJobCounterStartsAtZero(t *testing.T) {
	// A job series is the increase seen while the job is sole user, not the
	// port's lifetime value — a new job does not inherit old errors.
	e := build(t, fixture.V2)
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_job_counter_total", "job_id", "918001", "counter", "port_xmit_data"); v != 0 {
		t.Errorf("first sample: job counter = %v, want 0", v)
	}
	if v := e.scalar(got, "ib_slurm_device_counter_total", "device", "mlx5_0", "counter", "port_xmit_data"); v != 8812004331 {
		t.Errorf("device counter = %v", v)
	}
}

func TestJobCounterExcludesSharedPeriods(t *testing.T) {
	// Audit reproduction: A is sole, B joins and generates traffic, B leaves,
	// A is sole again. rate() over A's series must not include B's traffic.
	e := build(t, fixture.V2)
	const dev, cnt = "mlx5_0", "packet_seq_err"
	jobValue := func() (float64, bool) {
		return find(e.gather()["ib_slurm_job_counter_total"], "job_id", "918001", "device", dev, "counter", cnt)
	}

	if v, _ := jobValue(); v != 0 { // baseline
		t.Fatalf("baseline %v", v)
	}
	e.setCounter(dev, 1, cnt, 10)
	if v, _ := jobValue(); v != 10 {
		t.Fatalf("sole period: %v, want 10", v)
	}

	// B (918002) opens mlx5_0 too and the device counts 1000 more errors.
	e.addProc(42002, 0)
	e.setJobPIDs("918002", 42001, 42002)
	e.setCounter(dev, 1, cnt, 1010)
	if _, ok := jobValue(); ok {
		t.Fatal("shared: 918001's series must be absent")
	}
	e.setCounter(dev, 1, cnt, 1015)

	// B leaves.
	e.setJobPIDs("918002", 42001)
	if v, _ := jobValue(); v != 10 {
		t.Fatalf("first sole sample after sharing must not add the gap: %v, want 10", v)
	}
	e.setCounter(dev, 1, cnt, 1022)
	if v, _ := jobValue(); v != 17 {
		t.Fatalf("sole again: %v, want 17", v)
	}
}

func TestCounterDecreaseIsTreatedAsReset(t *testing.T) {
	// A decrease adds the new raw value, as rate() does. Whatever the port
	// counted between the last sample and the clear is lost: here the port
	// may have gone 100 → 120 → cleared → 30, a true increase of 50, and
	// the job is credited 30. An under-count, never an over-count.
	e := build(t, fixture.V2)
	get := func() float64 {
		v, _ := find(e.gather()["ib_slurm_job_counter_total"], "job_id", "918001", "counter", "symbol_error")
		return v
	}
	e.setCounter("mlx5_0", 1, "symbol_error", 100)
	get() // baseline at 100
	e.setCounter("mlx5_0", 1, "symbol_error", 30)
	if v := get(); v != 30 {
		t.Fatalf("after a clear and 30 more errors: %v, want 30 (anything counted before the clear is lost)", v)
	}
}

func TestAccumulatorsAreDroppedWhenAJobEnds(t *testing.T) {
	e := build(t, fixture.V2)
	e.gather()
	before := len(e.c.acc)
	if err := os.RemoveAll(filepath.Join(e.roots.Cgroup, fixture.DefaultScope, "job_918001")); err != nil {
		t.Fatal(err)
	}
	e.gather()
	for k := range e.c.acc {
		if k.job == "918001" {
			t.Fatalf("accumulator for ended job kept: %+v (had %d)", k, before)
		}
	}
}

func TestBackgroundSamplingFeedsAccumulators(t *testing.T) {
	e := build(t, fixture.V2)
	e.c.Sample(context.Background())
	e.setCounter("mlx5_0", 1, "packet_seq_err", 5)
	e.c.Sample(context.Background())
	e.setCounter("mlx5_0", 1, "packet_seq_err", 9)
	v, _ := find(e.gather()["ib_slurm_job_counter_total"], "job_id", "918001", "counter", "packet_seq_err")
	if v != 9 {
		t.Fatalf("got %v, want 9", v)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.c.Run(ctx, time.Millisecond); close(done) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
}

// ── Counter width ──────────────────────────────────────────────────────────

func TestSaturatedCounterIsFlagged(t *testing.T) {
	spec := fixture.Default(fixture.V2)
	spec.Counters["mlx5_1:1"]["link_downed"] = 255
	e := newEnv(t, spec, nil)
	for i := 0; i < 2; i++ {
		got := e.gather()
		if v := e.scalar(got, "ib_slurm_device_counter_saturated", "device", "mlx5_1", "counter", "link_downed"); v != 1 {
			t.Fatalf("scrape %d: saturated = %v", i, v)
		}
		if v := e.scalar(got, "ib_slurm_device_counter_saturated", "device", "mlx5_0", "counter", "link_downed"); v != 0 {
			t.Fatalf("mlx5_0 link_downed=0 is not saturated: %v", v)
		}
		if _, ok := find(got["ib_slurm_device_counter_saturated"], "counter", "port_xmit_data"); ok {
			t.Fatal("width of port_xmit_data is unknown; no saturation series")
		}
	}
}

// ── Ports and ownership modes ──────────────────────────────────────────────

func dualPortSpec() fixture.Spec {
	spec := fixture.Default(fixture.V2)
	spec.Ports = map[string]int{"mlx5_0": 2}
	spec.Counters["mlx5_0:2"] = map[string]uint64{"port_xmit_data": 777}
	return spec
}

func TestFDModeAttributesEveryPortOfTheHCA(t *testing.T) {
	// Documented limitation: a descriptor carries no port. The job is the
	// only Slurm job on the HCA, so both ports are attributed to it.
	e := newEnv(t, dualPortSpec(), nil)
	got := e.gather()
	for _, port := range []string{"1", "2"} {
		if v := e.scalar(got, "ib_slurm_job_device_attributed", "job_id", "918001", "device", "mlx5_0", "port", port); v != 1 {
			t.Errorf("port %s: %v", port, v)
		}
	}
}

func TestQPModeAttributesOnlyThePortWithQueuePairs(t *testing.T) {
	// Audit reproduction: in fd mode job 918001 was charged port 2's
	// traffic with nothing tying it to port 2. With QP ownership it is not.
	spec := dualPortSpec()
	e := newEnv(t, spec, func(c *Config) {
		c.Ownership = OwnershipQP
		c.QPs = &qpSource{qps: fixture.QPs(spec)}
	})
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_job_device_attributed", "job_id", "918001", "device", "mlx5_0", "port", "1"); v != 1 {
		t.Errorf("port 1: %v", v)
	}
	if _, ok := find(got["ib_slurm_job_device_attributed"], "job_id", "918001", "port", "2"); ok {
		t.Error("918001 has no QP on port 2 and must not be linked to it")
	}
	if v := e.scalar(got, "ib_slurm_device_jobs", "device", "mlx5_0", "port", "2"); v != 0 {
		t.Errorf("port 2 users = %v", v)
	}
}

func TestQPModeManagementQPsDoNotCount(t *testing.T) {
	spec := fixture.Default(fixture.V2)
	e := newEnv(t, spec, func(c *Config) {
		c.Ownership = OwnershipQP
		c.QPs = &qpSource{qps: fixture.QPs(spec)} // includes SMI/GSI
	})
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_device_nonjob_users", "device", "mlx5_0"); v != 0 {
		t.Errorf("SMI/GSI counted as users: %v", v)
	}
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918001"); !ok {
		t.Error("918001 should be attributed")
	}
}

func TestQPModeKernelConsumerSuppressesAttribution(t *testing.T) {
	// Kernel QPs created through ib_core have no PID. Invisible in fd mode;
	// in qp mode they make the port shared. This one is an NFS/RDMA client
	// QP: xprtrdma calls rdma_create_qp, and cma.c creates it with the
	// ib_create_qp inline, which passes KBUILD_MODNAME, so restrack names it
	// "rdma_cm" (read from the kernel source, not seen on a live node).
	// IPoIB is not modelled as a QP here: mlx5 enhanced IPoIB has none in
	// resource tracking; see the IPoIB tests below.
	spec := fixture.Default(fixture.V2)
	src := &qpSource{qps: append(fixture.QPs(spec),
		rdmares.QP{Device: "mlx5_0", Port: 1, LQPN: 900, Type: "RC", Kernel: true, Comm: "rdma_cm"})}
	e := newEnv(t, spec, func(c *Config) { c.Ownership = OwnershipQP; c.QPs = src })
	got := e.gather()
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918001"); ok {
		t.Fatal("kernel QP on mlx5_0: 918001 must not be attributed")
	}
	if v := e.scalar(got, "ib_slurm_device_nonjob_users", "device", "mlx5_0"); v != 1 {
		t.Errorf("nonjob users = %v", v)
	}
	if v := e.scalar(got, "ib_slurm_unattributed_jobs"); v != 3 {
		t.Errorf("unattributed = %v, want 3", v)
	}
}

func TestQPModeNonSlurmProcessSuppressesAttribution(t *testing.T) {
	spec := fixture.Default(fixture.V2)
	src := &qpSource{qps: append(fixture.QPs(spec),
		rdmares.QP{Device: "mlx5_2", Port: 1, LQPN: 901, Type: "RC", PID: 777, Comm: "ib_write_bw"})}
	e := newEnv(t, spec, func(c *Config) { c.Ownership = OwnershipQP; c.QPs = src })
	got := e.gather()
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918004"); ok {
		t.Fatal("a process outside Slurm shares mlx5_2")
	}
}

func TestQPModeThreadIDMapsToItsProcess(t *testing.T) {
	spec := fixture.Default(fixture.V2)
	e := newEnv(t, spec, nil)
	dir := filepath.Join(e.roots.Proc, "41077")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "status"), []byte("Tgid:\t41001\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	e.c.cfg.Ownership = OwnershipQP
	e.c.cfg.QPs = &qpSource{qps: []rdmares.QP{{Device: "mlx5_0", Port: 1, LQPN: 5, Type: "RC", PID: 41077}}}
	got := e.gather()
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918001"); !ok {
		t.Fatal("a QP created by a thread of 918001's process belongs to 918001")
	}
}

func TestNCCLOpenAllHCAs(t *testing.T) {
	// NCCL opens every active HCA. Two partial-node jobs doing so share
	// every device in fd mode — nothing attributed (documented). With QPs on
	// distinct HCAs, qp mode attributes both.
	spec := fixture.Default(fixture.V2)
	spec.Jobs = map[string][]int{"918001": {41001}, "918004": {44001}}
	spec.PIDDevices = map[int][]int{41001: {0, 1, 2}, 44001: {0, 1, 2}}
	fd := newEnv(t, spec, nil).gather()
	if len(fd["ib_slurm_job_counter_total"]) != 0 {
		t.Error("fd mode: every HCA is held by both jobs; nothing can be attributed")
	}

	src := &qpSource{qps: []rdmares.QP{
		{Device: "mlx5_0", Port: 1, LQPN: 10, Type: "RC", PID: 41001},
		{Device: "mlx5_2", Port: 1, LQPN: 11, Type: "RC", PID: 44001},
	}}
	qp := newEnv(t, spec, func(c *Config) { c.Ownership = OwnershipQP; c.QPs = src }).gather()
	if _, ok := find(qp["ib_slurm_job_counter_total"], "job_id", "918001", "device", "mlx5_0"); !ok {
		t.Error("qp mode: 918001 has the only QPs on mlx5_0")
	}
	if _, ok := find(qp["ib_slurm_job_counter_total"], "job_id", "918004", "device", "mlx5_2"); !ok {
		t.Error("qp mode: 918004 has the only QPs on mlx5_2")
	}
}

func TestQPListingFailureFailsSafe(t *testing.T) {
	spec := fixture.Default(fixture.V2)
	e := newEnv(t, spec, func(c *Config) {
		c.Ownership = OwnershipQP
		c.QPs = &qpSource{err: errors.New("rdma: Operation not permitted")}
	})
	got := e.gather()
	if len(got["ib_slurm_job_counter_total"]) != 0 {
		t.Fatal("unknown QP ownership: no job series")
	}
	if v := e.scalar(got, "ib_slurm_scrape_error", "source", "rdma"); v != 1 {
		t.Errorf("scrape_error{rdma} = %v", v)
	}
	if v := e.scalar(got, "ib_slurm_unattributed_jobs"); v != 4 {
		t.Errorf("unattributed = %v, want 4", v)
	}
	if len(got["ib_slurm_device_nonjob_users"]) != 0 {
		t.Error("nonjob_users is unknown, so absent — not 0")
	}
}

// ── Labels, lint, concurrency ──────────────────────────────────────────────

func TestNoUserLabels(t *testing.T) {
	e := newEnv(t, fixture.Default(fixture.V2), func(c *Config) { c.NoUserLabels = true })
	got := e.gather()
	for _, m := range got["ib_slurm_job_counter_total"] {
		if label(m, "user") != "" || label(m, "account") != "" {
			t.Fatal("user/account must be empty with NoUserLabels")
		}
	}
	if len(got["ib_slurm_job_info"]) != 0 {
		t.Fatal("job_info must not be emitted with NoUserLabels")
	}
}

func TestJobInfoCarriesMetadata(t *testing.T) {
	e := build(t, fixture.V2)
	got := e.gather()
	if _, ok := find(got["ib_slurm_job_info"], "job_id", "918002", "user", "bob", "account", "infra"); !ok {
		t.Fatal("job_info missing")
	}
}

func TestLintReportsOnlyTheKnownNamingProblems(t *testing.T) {
	// promlint objects to "counter" in the two *_counter_total names. Renaming
	// them breaks every existing query, so it is deferred to the planned
	// typed-family split; this test fails if anything else creeps in.
	e := build(t, fixture.V2)
	problems, err := testutil.CollectAndLint(e.c)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		known := (p.Metric == "ib_slurm_job_counter_total" || p.Metric == "ib_slurm_device_counter_total") &&
			strings.Contains(p.Text, "should not include type 'counter'")
		if !known {
			t.Errorf("new lint problem: %s: %s", p.Metric, p.Text)
		}
	}
}

func TestConcurrentScrapes(t *testing.T) {
	e := build(t, fixture.V2SLUID)
	r := prometheus.NewRegistry()
	r.MustRegister(e.c)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.Gather(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); e.c.Sample(context.Background()) }()
	wg.Wait()
}

func TestDeviceLevelSeriesAlwaysPresent(t *testing.T) {
	e := build(t, fixture.V2)
	got := e.gather()
	for _, want := range []string{"mlx5_0", "mlx5_1", "mlx5_2"} {
		if _, ok := find(got["ib_slurm_device_port_up"], "device", want); !ok {
			t.Errorf("%s missing from device-level series", want)
		}
	}
}

func TestV1LayoutEndToEnd(t *testing.T) {
	e := build(t, fixture.V1)
	got := e.gather()
	if _, ok := find(got["ib_slurm_cgroup_layout_info"], "layout", "cgroup-v1", "scope", "freezer/slurm"); !ok {
		t.Fatal("v1 layout_info missing")
	}
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918001"); !ok {
		t.Fatal("918001 should be attributed on v1")
	}
}

// ── IPoIB interfaces (qp mode) ─────────────────────────────────────────────

func qpEnv(t *testing.T, spec fixture.Spec) *env {
	return newEnv(t, spec, func(c *Config) {
		c.Ownership = OwnershipQP
		c.QPs = &qpSource{qps: fixture.QPs(spec)}
	})
}

func TestQPModeIPoIBInterfaceUpSuppressesAttribution(t *testing.T) {
	// Audit reproduction: mlx5 enhanced IPoIB creates its QP with a raw
	// firmware command, so resource tracking lists nothing for it. With
	// ib0 up on mlx5_0, TCP over ib0 would have been charged to 918001 as
	// the port's "sole" user.
	spec := fixture.Default(fixture.V2)
	spec.IPoIB[0].OperState = "up" // ib0 on mlx5_0
	got := qpEnv(t, spec).gather()
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918001"); ok {
		t.Fatal("ib0 is up on mlx5_0: 918001 must not be attributed")
	}
	if v, _ := find(got["ib_slurm_device_nonjob_users"], "device", "mlx5_0", "port", "1"); v != 1 {
		t.Errorf("nonjob_users on mlx5_0 = %v, want 1 (the interface)", v)
	}
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918004", "device", "mlx5_2"); !ok {
		t.Error("an interface on mlx5_0 says nothing about mlx5_2")
	}
}

func TestQPModeDownIPoIBInterfacesDoNotCount(t *testing.T) {
	// The default fixture has ib0 and ib1 administratively down, as ib_ipoib
	// leaves an unconfigured port. They cannot carry traffic.
	e := qpEnv(t, fixture.Default(fixture.V2))
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_device_nonjob_users", "device", "mlx5_0"); v != 0 {
		t.Errorf("down ib0 counted: %v", v)
	}
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918001", "device", "mlx5_0"); !ok {
		t.Error("918001 should be attributed")
	}
}

func TestQPModeIPoIBChildInterfaceIsFoundThroughItsParent(t *testing.T) {
	// A P_Key child is a virtual device; only its iflink ties it to ib0 and
	// so to mlx5_0 port 1. Up while its parent is down, it still counts.
	spec := fixture.Default(fixture.V2)
	spec.IPoIB = append(spec.IPoIB, fixture.IPoIB{Name: "ib0.8001", Parent: "ib0", OperState: "up"})
	got := qpEnv(t, spec).gather()
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918001"); ok {
		t.Fatal("ib0.8001 is up on mlx5_0: 918001 must not be attributed")
	}
	if v, _ := find(got["ib_slurm_device_nonjob_users"], "device", "mlx5_1"); v != 0 {
		t.Errorf("the child is on mlx5_0, not mlx5_1: %v", v)
	}
}

func TestQPModeUntiedIPoIBInterfaceCountsOnEveryInfiniBandPort(t *testing.T) {
	// An IPoIB interface whose parent cannot be found could be on any IB
	// port. It says nothing about a RoCE (Ethernet) port.
	spec := fixture.Default(fixture.V2)
	e := qpEnv(t, spec)
	dir := filepath.Join(e.roots.Base, "sys", "devices", "virtual", "net", "ib9.8001")
	if err := fixture.NetDev(e.roots, dir, 32, 0, 90, 89, "up"); err != nil {
		t.Fatal(err)
	}
	got := e.gather()
	for dev, want := range map[string]float64{"mlx5_0": 1, "mlx5_1": 1, "mlx5_2": 0} {
		if v := e.scalar(got, "ib_slurm_device_nonjob_users", "device", dev); v != want {
			t.Errorf("%s: nonjob_users = %v, want %v", dev, v, want)
		}
	}
	if _, ok := find(got["ib_slurm_job_counter_total"], "job_id", "918004", "device", "mlx5_2"); !ok {
		t.Error("918004 on the RoCE port should still be attributed")
	}
}

func TestQPModeIPoIBScanErrorFailsSafe(t *testing.T) {
	skipIfRoot(t)
	e := qpEnv(t, fixture.Default(fixture.V2))
	dir := filepath.Join(fixture.DeviceDir(e.roots, "mlx5_0"), "net")
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	got := e.gather()
	if len(got["ib_slurm_job_counter_total"]) != 0 {
		t.Fatal("unknown IPoIB state: no job series")
	}
	if v := e.scalar(got, "ib_slurm_scrape_error", "source", "rdma"); v != 1 {
		t.Errorf("scrape_error{rdma} = %v", v)
	}
	if len(got["ib_slurm_device_nonjob_users"]) != 0 {
		t.Error("nonjob_users is unknown, so absent — not 0")
	}
}

// ── Cgroup scan errors ─────────────────────────────────────────────────────

func (e *env) chmodScope(mode os.FileMode) {
	e.t.Helper()
	if err := os.Chmod(filepath.Join(e.roots.Cgroup, fixture.DefaultScope), mode); err != nil {
		e.t.Fatal(err)
	}
}

func TestTransientCgroupErrorKeepsJobCounters(t *testing.T) {
	// Audit reproduction: one sample with an unreadable scope deleted every
	// accumulator, so once the error cleared 918001's series restarted from
	// 0 mid-job instead of continuing from its value.
	skipIfRoot(t)
	e := build(t, fixture.V2)
	t.Cleanup(func() { e.chmodScope(0o755) })
	get := func() (float64, bool) {
		return find(e.gather()["ib_slurm_job_counter_total"], "job_id", "918001", "counter", "packet_seq_err")
	}
	get() // baseline
	e.setCounter("mlx5_0", 1, "packet_seq_err", 50)
	if v, _ := get(); v != 50 {
		t.Fatalf("before: %v", v)
	}
	e.chmodScope(0)
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_scrape_error", "source", "cgroup"); v != 1 {
		t.Fatalf("scrape_error{cgroup} = %v", v)
	}
	e.chmodScope(0o755)
	if v, _ := get(); v != 50 {
		t.Fatalf("after the error cleared: %v, want 50 (not restarted)", v)
	}
	e.setCounter("mlx5_0", 1, "packet_seq_err", 60)
	if v, _ := get(); v != 60 {
		t.Fatalf("then +10: %v, want 60", v)
	}
}

func TestPersistentCgroupErrorStillPrunesAfterStaleAfter(t *testing.T) {
	// Pruning waits for a clean scan, but not forever: a cgroup error that
	// never clears must not keep every ended job's state.
	skipIfRoot(t)
	e := build(t, fixture.V2)
	t.Cleanup(func() { e.chmodScope(0o755) })
	now := time.Unix(1_800_000_000, 0)
	e.c.now = func() time.Time { return now }
	e.gather()
	if len(e.c.acc) == 0 {
		t.Fatal("no accumulators")
	}
	e.chmodScope(0)
	now = now.Add(time.Minute)
	e.gather()
	if len(e.c.acc) == 0 {
		t.Fatal("one failed scan must not prune")
	}
	now = now.Add(staleAfter)
	e.gather()
	if len(e.c.acc) != 0 || len(e.c.jobSeen) != 0 {
		t.Fatalf("missing for longer than staleAfter: %d accumulators, %d jobs kept", len(e.c.acc), len(e.c.jobSeen))
	}
}

func TestSLUIDCacheSurvivesAScanError(t *testing.T) {
	skipIfRoot(t)
	spec := fixture.Default(fixture.V2SLUID)
	spec.OmitStepd = true // resolve through the counting resolver
	r := &fixtureResolver{}
	e := newEnv(t, spec, func(c *Config) { c.Resolver = r })
	t.Cleanup(func() { e.chmodScope(0o755) })
	e.gather()
	e.chmodScope(0)
	e.gather()
	if len(e.c.sluidCache) != 4 {
		t.Fatalf("a failed scan pruned the SLUID cache: %v", e.c.sluidCache)
	}
	e.chmodScope(0o755)
	e.gather()
	if r.calls != 4 {
		t.Fatalf("SLUIDs were resolved again after a transient error: %d resolver calls", r.calls)
	}
}

func TestUnreadableSLUIDJobDirectoryFailsSafe(t *testing.T) {
	// 918003's SLUID directory cannot be listed, so the scan cannot even
	// tell it is a job: no allocation, nothing marked Incomplete, only the
	// scan error. 918002 would then look like mlx5_1's sole user. The
	// cgroup error alone has to suppress attribution.
	skipIfRoot(t)
	e := build(t, fixture.V2SLUID)
	dir := filepath.Join(e.roots.Cgroup, fixture.DefaultScope, fixture.SLUIDFor("918003"))
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	got := e.gather()
	if v := e.scalar(got, "ib_slurm_scrape_error", "source", "cgroup"); v != 1 {
		t.Errorf("scrape_error{cgroup} = %v", v)
	}
	if len(got["ib_slurm_job_counter_total"]) != 0 {
		t.Fatal("an unreadable job directory must suppress all attribution")
	}
}

func TestQPModeUnreadableCgroupFailsSafe(t *testing.T) {
	skipIfRoot(t)
	spec := fixture.Default(fixture.V2SLUID)
	e := qpEnv(t, spec)
	dir := filepath.Join(e.roots.Cgroup, fixture.DefaultScope, fixture.SLUIDFor("918003"))
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	got := e.gather()
	if len(got["ib_slurm_job_counter_total"]) != 0 {
		t.Fatal("qp mode: an unreadable cgroup must suppress all attribution")
	}
}

// ── Logging ────────────────────────────────────────────────────────────────

func TestLogErrorsIsRateLimitedButLogsANewCause(t *testing.T) {
	var logs strings.Builder
	e := newEnv(t, fixture.Default(fixture.V2), func(c *Config) {
		c.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	})
	now := time.Unix(1_800_000_000, 0)
	e.c.now = func() time.Time { return now }
	failed := func() int { return strings.Count(logs.String(), "sample source failed") }
	fail := func(err error) {
		e.c.gen++
		e.c.logErrors(&sample{errs: map[string]error{"procfd": err}})
	}
	eacces := func(pid int) error {
		return &os.PathError{Op: "open", Path: "/proc/" + strconv.Itoa(pid) + "/fd", Err: os.ErrPermission}
	}

	fail(eacces(41001))
	if failed() != 1 {
		t.Fatalf("first failure not logged:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "CAP_DAC_READ_SEARCH") || !strings.Contains(logs.String(), "CAP_SYS_PTRACE") {
		t.Errorf("listing another user's 0500 fd dir needs CAP_DAC_READ_SEARCH, readlink needs CAP_SYS_PTRACE; hint:\n%s", logs.String())
	}
	now = now.Add(time.Minute)
	fail(eacces(41002)) // same cause, another PID
	if failed() != 1 {
		t.Fatalf("same cause on another PID must stay rate-limited:\n%s", logs.String())
	}
	now = now.Add(time.Minute)
	fail(errors.New("1 job PIDs are listed in cgroup.procs but not visible in /proc (hidepid mount or different PID namespace?)"))
	if failed() != 2 {
		t.Fatalf("a new cause must be logged at once:\n%s", logs.String())
	}
	now = now.Add(time.Minute)
	fail(errors.New("3 job PIDs are listed in cgroup.procs but not visible in /proc (hidepid mount or different PID namespace?)"))
	if failed() != 2 {
		t.Fatalf("the same cause with another count must stay rate-limited:\n%s", logs.String())
	}
	now = now.Add(11 * time.Minute)
	fail(errors.New("2 job PIDs are listed in cgroup.procs but not visible in /proc (hidepid mount or different PID namespace?)"))
	if failed() != 3 {
		t.Fatalf("a persisting cause is logged again after ten minutes:\n%s", logs.String())
	}
	e.c.logErrors(&sample{errs: map[string]error{}})
	if !strings.Contains(logs.String(), "sample source recovered") {
		t.Error("recovery should be logged")
	}
}
