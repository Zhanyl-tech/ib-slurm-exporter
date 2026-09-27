package fixture

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestSLUIDRoundTrip(t *testing.T) {
	for _, job := range []string{"918001", "7", "123456789"} {
		s := SLUIDFor(job)
		if !strings.HasPrefix(s, "s") || strings.HasPrefix(s, "job_") {
			t.Errorf("%q does not look like a SLUID directory name", s)
		}
		if got, ok := SLUIDToJob(s); !ok || got != job {
			t.Errorf("SLUIDToJob(%q) = %q,%v", s, got, ok)
		}
	}
	if _, ok := SLUIDToJob("sEKNKTV3WPV500"); ok {
		t.Error("a real-looking SLUID is not one of ours")
	}
}

func TestV2SLUIDMatchesDocumentedShape(t *testing.T) {
	// <scope>/<SLUID>/step_0/{slurm,user/task_0}/cgroup.procs, plus system/.
	r, err := Build(t.TempDir(), Default(V2SLUID))
	if err != nil {
		t.Fatal(err)
	}
	scope := filepath.Join(r.Cgroup, DefaultScope)
	for _, p := range []string{
		filepath.Join(scope, "system", "cgroup.procs"),
		filepath.Join(scope, SLUIDFor("918001"), "step_0", "slurm", "cgroup.procs"),
		filepath.Join(scope, SLUIDFor("918001"), "step_0", "user", "task_0", "cgroup.procs"),
		filepath.Join(r.Proc, strconv.Itoa(StepdPID("918001")), "cmdline"),
	} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("missing %s", p)
		}
	}
	if _, err := os.Stat(filepath.Join(scope, "job_918001")); err == nil {
		t.Error("26.05 default layout has no job_ directories")
	}
}

func TestAdvanceClampsFixedWidthCounters(t *testing.T) {
	spec := Default(V2)
	spec.Counters["mlx5_1:1"]["link_downed"] = 254
	r, err := Build(t.TempDir(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := Advance(r, spec, 5); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(CounterPath(r, "mlx5_1", 1, "link_downed"))
	if strings.TrimSpace(string(b)) != "255" {
		t.Errorf("8-bit link_downed should stop at 255, got %s", b)
	}
	b, _ = os.ReadFile(CounterPath(r, "mlx5_0", 1, "packet_seq_err"))
	if strings.TrimSpace(string(b)) != "0" {
		t.Errorf("a healthy zero counter stays zero, got %s", b)
	}
}

func TestQPsIncludeManagementQPs(t *testing.T) {
	qps := QPs(Default(V2))
	mgmt, user := 0, 0
	for _, q := range qps {
		if q.IsManagement() {
			mgmt++
		} else if !q.Kernel {
			user++
		}
	}
	if mgmt != 6 || user != 6 {
		t.Fatalf("want 6 SMI/GSI and 6 user QPs, got %d and %d", mgmt, user)
	}
}
