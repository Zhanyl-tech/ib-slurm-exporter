package cgroup_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/cgroup"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/fixture"
)

// All trees here are synthetic. Shapes follow
// https://slurm.schedmd.com/cgroup_v2.html and cgroup.conf.html; none was
// captured from a live node.

func build(t *testing.T, spec fixture.Spec) fixture.Roots {
	t.Helper()
	roots, err := fixture.Build(t.TempDir(), spec)
	if err != nil {
		t.Fatal(err)
	}
	return roots
}

func write(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func skipIfRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions; this test needs an unprivileged user")
	}
}

func TestSlurm2605DefaultLayoutFindsEveryJob(t *testing.T) {
	// Regression: job directories on 26.05 are bare SLUIDs with no "job_"
	// prefix. The old scanner matched only ^job_ and found nothing here.
	roots := build(t, fixture.Default(fixture.V2SLUID))
	res, err := cgroup.NewScanner(roots.Cgroup).Scan()
	if err != nil {
		t.Fatal(err)
	}
	if res.Layout != cgroup.LayoutV2SLUID {
		t.Fatalf("layout = %s, want %s", res.Layout, cgroup.LayoutV2SLUID)
	}
	if len(res.Allocations) != 4 {
		t.Fatalf("want 4 allocations, got %d", len(res.Allocations))
	}
	for _, a := range res.Allocations {
		if strings.HasPrefix(filepath.Base(a.Path), "job_") {
			t.Errorf("%s: fixture should use bare SLUID directories", a.Path)
		}
		if !a.IsSLUID || a.JobID != "" {
			t.Errorf("%q must be an unresolved SLUID, got IsSLUID=%v JobID=%q", a.Identifier, a.IsSLUID, a.JobID)
		}
		if _, ok := fixture.SLUIDToJob(a.Identifier); !ok {
			t.Errorf("identifier %q is not the directory's SLUID", a.Identifier)
		}
		if len(a.StepdPIDs) != 1 {
			t.Errorf("%s: want the step_0 slurmstepd PID, got %v", a.Identifier, a.StepdPIDs)
		}
	}
}

func TestDocumentedExampleSLUIDDirectory(t *testing.T) {
	// The exact example from cgroup.conf(5) CgroupJobIdPaths:
	// ".../slurmstepd.scope/sEKNKTV3WPV500/", with the step layout from the
	// cgroup_v2.html tree (step_0/{slurm,user/task_0}).
	root := t.TempDir()
	scope := filepath.Join(root, "system.slice", "slurmstepd.scope")
	write(t, filepath.Join(scope, "system", "cgroup.procs"), "113094\n")
	write(t, filepath.Join(scope, "sEKNKTV3WPV500", "step_0", "slurm", "cgroup.procs"), "113630\n")
	write(t, filepath.Join(scope, "sEKNKTV3WPV500", "step_0", "user", "task_0", "cgroup.procs"), "113635\n")
	write(t, filepath.Join(scope, "sEKNKTV3WPV500", "step_extern", "user", "task_0", "cgroup.procs"), "113569\n")

	res, err := cgroup.NewScanner(root).Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Allocations) != 1 {
		t.Fatalf("want 1 allocation, got %+v", res.Allocations)
	}
	a := res.Allocations[0]
	if a.Identifier != "sEKNKTV3WPV500" || !a.IsSLUID {
		t.Fatalf("got %+v", a)
	}
	pids := append([]int(nil), a.PIDs...)
	sort.Ints(pids)
	if len(pids) != 3 || pids[0] != 113569 || pids[1] != 113630 || pids[2] != 113635 {
		t.Fatalf("PIDs from every leaf cgroup, got %v", pids)
	}
	if len(a.StepdPIDs) != 1 || a.StepdPIDs[0] != 113630 {
		t.Fatalf("stepd PIDs from step_*/slurm only, got %v", a.StepdPIDs)
	}
}

func TestCgroupJobIdPathsLayout(t *testing.T) {
	// Pre-26.05, or 26.05 with CgroupJobIdPaths=yes: job_<jobid>.
	roots := build(t, fixture.Default(fixture.V2))
	res, err := cgroup.NewScanner(roots.Cgroup).Scan()
	if err != nil {
		t.Fatal(err)
	}
	if res.Layout != cgroup.LayoutV2 || len(res.Allocations) != 4 {
		t.Fatalf("got layout %s with %d allocations", res.Layout, len(res.Allocations))
	}
	for _, a := range res.Allocations {
		if a.IsSLUID || a.JobID != a.Identifier {
			t.Errorf("numeric job dir %q must be its own job ID", a.Identifier)
		}
	}
}

func TestSystemDirectoryIsNotAJob(t *testing.T) {
	// system/ holds slurmstepd's infinity process (cgroup_v2.html). Even with
	// a step-like child it must never be read as a job.
	roots := build(t, fixture.Default(fixture.V2SLUID))
	scope := filepath.Join(roots.Cgroup, fixture.DefaultScope)
	write(t, filepath.Join(scope, "system", "step_0", "cgroup.procs"), "1\n")
	res, _ := cgroup.NewScanner(roots.Cgroup).Scan()
	for _, a := range res.Allocations {
		if a.Identifier == "system" {
			t.Fatal("system/ was treated as a job")
		}
	}
}

func TestDirectoryWithoutStepsIsNotAJob(t *testing.T) {
	roots := build(t, fixture.Default(fixture.V2SLUID))
	write(t, filepath.Join(roots.Cgroup, fixture.DefaultScope, "not-a-job", "cgroup.procs"), "7\n")
	res, _ := cgroup.NewScanner(roots.Cgroup).Scan()
	if len(res.Allocations) != 4 {
		t.Fatalf("a directory with no step_* child is not a job; got %d allocations", len(res.Allocations))
	}
}

func TestLegacyJobPrefixedNonNumericIsNeverAJobID(t *testing.T) {
	// Not a documented layout, but the safety property from before stays:
	// a non-numeric identifier is never used as a job ID unresolved.
	root := t.TempDir()
	write(t, filepath.Join(root, fixture.DefaultScope, "job_sABC", "step_0", "cgroup.procs"), "5\n")
	res, _ := cgroup.NewScanner(root).Scan()
	if len(res.Allocations) != 1 || !res.Allocations[0].IsSLUID || res.Allocations[0].JobID != "" {
		t.Fatalf("got %+v", res.Allocations)
	}
}

func TestIdleScopeIsFoundButEmpty(t *testing.T) {
	spec := fixture.Default(fixture.V2SLUID)
	spec.Jobs = nil
	roots := build(t, spec)
	res, err := cgroup.NewScanner(roots.Cgroup).Scan()
	if err != nil {
		t.Fatal(err)
	}
	if !res.ScopeFound || len(res.Allocations) != 0 || len(res.Scopes) != 1 {
		t.Fatalf("idle node: want scope found with no jobs, got %+v", res)
	}
}

func TestNoScopeIsDistinguishedFromIdle(t *testing.T) {
	res, err := cgroup.NewScanner(t.TempDir()).Scan()
	if err != nil {
		t.Fatal(err)
	}
	if res.ScopeFound || res.Layout != cgroup.LayoutUnknown {
		t.Fatalf("empty cgroup root: want no scope, got %+v", res)
	}
}

func TestMissingRootIsAnError(t *testing.T) {
	res, err := cgroup.NewScanner(filepath.Join(t.TempDir(), "nope")).Scan()
	if err == nil || res.ScopeFound {
		t.Fatalf("wrong --cgroup-root must be an error, got %+v, %v", res, err)
	}
}

func TestCgroupSliceScopeIsDiscovered(t *testing.T) {
	// cgroup.conf CgroupSlice moves slurmstepd.scope out of system.slice.
	spec := fixture.Default(fixture.V2SLUID)
	spec.Scope = "slurm.slice/slurmstepd.scope"
	roots := build(t, spec)
	res, err := cgroup.NewScanner(roots.Cgroup).Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Allocations) != 4 || len(res.Scopes) != 1 || res.Scopes[0] != "slurm.slice/slurmstepd.scope" {
		t.Fatalf("got %d allocations in %v", len(res.Allocations), res.Scopes)
	}
}

func TestNestedSliceAndMultipleSlurmdScopesAreDiscovered(t *testing.T) {
	// --enable-multiple-slurmd prepends the node name to slurmstepd.scope.
	spec := fixture.Default(fixture.V2)
	spec.Scope = "slurm.slice/slurm-gpu.slice/node7_slurmstepd.scope"
	roots := build(t, spec)
	res, err := cgroup.NewScanner(roots.Cgroup).Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Allocations) != 4 {
		t.Fatalf("got %d allocations in %v", len(res.Allocations), res.Scopes)
	}
}

func TestDeepScopeNeedsExplicitFlag(t *testing.T) {
	// Discovery is bounded; a scope inside a container's cgroup is not found
	// unless --slurm-scope names it. The path here is invented: where
	// slurmstepd.scope lands under Kubernetes has not been verified.
	spec := fixture.Default(fixture.V2)
	spec.Scope = "kubepods.slice/pod-x.slice/cri-y.scope/system.slice/slurmstepd.scope"
	roots := build(t, spec)

	res, _ := cgroup.NewScanner(roots.Cgroup).Scan()
	if res.ScopeFound {
		t.Fatal("bounded discovery should not reach this deep")
	}
	sc := cgroup.NewScanner(roots.Cgroup)
	sc.Scope = spec.Scope
	res, err := sc.Scan()
	if err != nil || len(res.Allocations) != 4 {
		t.Fatalf("explicit scope: got %d allocations, %v", len(res.Allocations), err)
	}
}

func TestUnreadableScopeIsAnError(t *testing.T) {
	skipIfRoot(t)
	roots := build(t, fixture.Default(fixture.V2SLUID))
	scope := filepath.Join(roots.Cgroup, fixture.DefaultScope)
	if err := os.Chmod(scope, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(scope, 0o755) })
	res, err := cgroup.NewScanner(roots.Cgroup).Scan()
	if err == nil || !res.ScopeFound {
		t.Fatalf("unreadable scope: want an error with the scope reported found, got %+v, %v", res, err)
	}
}

func TestUnreadableStepMarksAllocationIncomplete(t *testing.T) {
	skipIfRoot(t)
	roots := build(t, fixture.Default(fixture.V2))
	step := filepath.Join(roots.Cgroup, fixture.DefaultScope, "job_918002", "step_0", "user")
	if err := os.Chmod(step, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(step, 0o755) })
	res, err := cgroup.NewScanner(roots.Cgroup).Scan()
	if err == nil {
		t.Fatal("a partly unreadable job must surface an error")
	}
	for _, a := range res.Allocations {
		if (a.JobID == "918002") != a.Incomplete {
			t.Errorf("job %s Incomplete=%v", a.JobID, a.Incomplete)
		}
	}
}

func TestV1Layout(t *testing.T) {
	roots := build(t, fixture.Default(fixture.V1))
	res, err := cgroup.NewScanner(roots.Cgroup).Scan()
	if err != nil {
		t.Fatal(err)
	}
	if res.Layout != cgroup.LayoutV1 || len(res.Allocations) != 4 {
		t.Fatalf("got %s with %d allocations", res.Layout, len(res.Allocations))
	}
	for _, a := range res.Allocations {
		if a.UID != 1000 {
			t.Errorf("v1 layout encodes uid in the path; got %d", a.UID)
		}
		if len(a.PIDs) == 0 {
			t.Errorf("job %s: PIDs live in step cgroups and must be found", a.JobID)
		}
	}
}

func TestReadPIDs(t *testing.T) {
	roots := build(t, fixture.Default(fixture.V2))
	pids, err := cgroup.ReadPIDs(filepath.Join(roots.Cgroup, fixture.DefaultScope, "job_918001"))
	if err != nil {
		t.Fatal(err)
	}
	// Two user processes and the step's slurmstepd.
	if len(pids) != 3 {
		t.Fatalf("got %v", pids)
	}
	if _, err := cgroup.ReadPIDs(filepath.Join(roots.Cgroup, "gone")); err != nil {
		t.Fatalf("a job that ended is not an error: %v", err)
	}
}

// ── cgroup v1 edge cases ───────────────────────────────────────────────────

func v1Base(roots fixture.Roots) string {
	return filepath.Join(roots.Cgroup, "freezer", "slurm")
}

func TestV1UidDirVanishingMidScanIsNotAnError(t *testing.T) {
	// Slurm removes uid_<uid> when the user's last job on the node ends, so
	// a directory listed a moment ago can be gone when it is read. A
	// dangling symlink reproduces "listed, then ENOENT" deterministically.
	roots := build(t, fixture.Default(fixture.V1))
	if err := os.Symlink(filepath.Join(t.TempDir(), "gone"), filepath.Join(v1Base(roots), "uid_2000")); err != nil {
		t.Fatal(err)
	}
	res, err := cgroup.NewScanner(roots.Cgroup).Scan()
	if err != nil {
		t.Fatalf("a job ending mid-scan is not a scan error: %v", err)
	}
	if res.Layout != cgroup.LayoutV1 || len(res.Allocations) != 4 {
		t.Fatalf("the other user's jobs must still be found: %s with %d allocations", res.Layout, len(res.Allocations))
	}
}

func TestV1UnreadableUidDirIsAnError(t *testing.T) {
	skipIfRoot(t)
	roots := build(t, fixture.Default(fixture.V1))
	uid := filepath.Join(v1Base(roots), "uid_1000")
	if err := os.Chmod(uid, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(uid, 0o755) })
	res, err := cgroup.NewScanner(roots.Cgroup).Scan()
	if err == nil || !res.ScopeFound {
		t.Fatalf("unreadable uid dir: want an error with the scope found, got %+v, %v", res, err)
	}
}

func TestV1UnreadableSlurmDirIsAnError(t *testing.T) {
	skipIfRoot(t)
	roots := build(t, fixture.Default(fixture.V1))
	if err := os.Chmod(v1Base(roots), 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(v1Base(roots), 0o755) })
	res, err := cgroup.NewScanner(roots.Cgroup).Scan()
	if err == nil || !res.ScopeFound || res.Layout != cgroup.LayoutV1 {
		t.Fatalf("unreadable slurm/: want a v1 error with the scope found, got %+v, %v", res, err)
	}
}

func TestV1IdleSlurmDirIsFoundButEmpty(t *testing.T) {
	spec := fixture.Default(fixture.V1)
	spec.Jobs = nil
	roots := build(t, spec)
	if err := os.MkdirAll(v1Base(roots), 0o755); err != nil {
		t.Fatal(err)
	}
	res, err := cgroup.NewScanner(roots.Cgroup).Scan()
	if err != nil {
		t.Fatal(err)
	}
	if !res.ScopeFound || res.Layout != cgroup.LayoutV1 || len(res.Allocations) != 0 ||
		len(res.Scopes) != 1 || res.Scopes[0] != "freezer/slurm" {
		t.Fatalf("idle v1 node: want freezer/slurm found with no jobs, got %+v", res)
	}
}
