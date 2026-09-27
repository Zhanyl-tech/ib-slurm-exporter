package procfd_test

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/fixture"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/procfd"
)

// All trees here are synthetic (see package fixture).

func build(t *testing.T, spec fixture.Spec) fixture.Roots {
	t.Helper()
	roots, err := fixture.Build(t.TempDir(), spec)
	if err != nil {
		t.Fatal(err)
	}
	return roots
}

func skipIfRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions; this test needs an unprivileged user")
	}
}

func TestPIDsResolveThroughUverbsNotByNameGuess(t *testing.T) {
	// Deliberately scrambled: uverbs numbering does not follow HCA naming.
	spec := fixture.Default(fixture.V2)
	spec.Devices = map[string]int{"mlx5_0": 2, "mlx5_1": 0, "mlx5_2": 1}
	roots := build(t, spec)
	m := procfd.NewMapper(roots.Proc, roots.Verbs)

	// PID 41001 holds uverbs0, which is mlx5_1 on this host.
	got, err := m.DevicesForPID(41001)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "mlx5_1" {
		t.Fatalf("pid 41001 holds uverbs0 → mlx5_1, got %v", got)
	}
	devs, st := m.DevicesForPIDs([]int{42001, 43001})
	if len(devs) != 1 || devs[0] != "mlx5_2" || st.Unreadable != 0 {
		t.Fatalf("both pids hold uverbs1 → mlx5_2, got %v %+v", devs, st)
	}
}

func TestExitedProcessIsGoneNotAnError(t *testing.T) {
	roots := build(t, fixture.Default(fixture.V2))
	m := procfd.NewMapper(roots.Proc, roots.Verbs)
	if _, err := m.DevicesForPID(999999); !errors.Is(err, procfd.ErrGone) {
		t.Fatalf("missing /proc entry should wrap ErrGone, got %v", err)
	}
	_, st := m.DevicesForPIDs([]int{41001, 999999})
	if st.Unreadable != 0 || len(st.Gone) != 1 || st.Gone[0] != 999999 {
		t.Fatalf("unexpected stats %+v", st)
	}
}

func TestPermissionDeniedIsReportedNotEmpty(t *testing.T) {
	// Without CAP_DAC_READ_SEARCH (to list it) and CAP_SYS_PTRACE (to read
	// its links), /proc/<pid>/fd of another user's process is EACCES. That
	// must not look like "this process uses no HCA".
	skipIfRoot(t)
	roots := build(t, fixture.Default(fixture.V2))
	fd := filepath.Join(roots.Proc, "41001", "fd")
	if err := os.Chmod(fd, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(fd, 0o755) })

	m := procfd.NewMapper(roots.Proc, roots.Verbs)
	_, err := m.DevicesForPID(41001)
	if err == nil || errors.Is(err, procfd.ErrGone) || !procfd.IsPermission(err) {
		t.Fatalf("want a permission error, got %v", err)
	}
	_, st := m.DevicesForPIDs([]int{41001, 41002})
	if st.Unreadable != 1 || st.FirstErr == nil {
		t.Fatalf("one unreadable PID expected, got %+v", st)
	}
}

func TestUverbsWithoutIbdevIsAnError(t *testing.T) {
	// A process holding an HCA we cannot name leaves the device's user set
	// incomplete; that is an error, not an empty result.
	spec := fixture.Default(fixture.V2)
	spec.PIDDevices[41001] = []int{9} // uverbs9 has no sysfs entry
	roots := build(t, spec)
	m := procfd.NewMapper(roots.Proc, roots.Verbs)
	if _, err := m.DevicesForPID(41001); err == nil {
		t.Fatal("unresolvable uverbs node must be an error")
	}
}

func writeCmdline(t *testing.T, roots fixture.Roots, pid int, cmdline string) {
	t.Helper()
	dir := filepath.Join(roots.Proc, strconv.Itoa(pid))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cmdline"), []byte(cmdline), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSlurmstepdJobID(t *testing.T) {
	roots := build(t, fixture.Default(fixture.V2SLUID))
	m := procfd.NewMapper(roots.Proc, roots.Verbs)

	// The fixture writes "slurmstepd: [918001.0]\x00".
	if id, ok := m.SlurmstepdJobID([]int{fixture.StepdPID("918001")}); !ok || id != "918001" {
		t.Fatalf("got %q,%v", id, ok)
	}

	writeCmdline(t, roots, 70001, "slurmstepd: [3385.extern]\x00\x00\x00")
	writeCmdline(t, roots, 70002, "slurmstepd: [3385.interactive]   ")
	if id, ok := m.SlurmstepdJobID([]int{70001, 70002}); !ok || id != "3385" {
		t.Fatalf("extern/interactive titles: got %q,%v", id, ok)
	}

	writeCmdline(t, roots, 70003, "slurmstepd: [4000.0]")
	if _, ok := m.SlurmstepdJobID([]int{70001, 70003}); ok {
		t.Fatal("stepds naming different jobs must not resolve")
	}

	writeCmdline(t, roots, 70004, "/usr/sbin/slurmstepd\x00infinity\x00")
	if _, ok := m.SlurmstepdJobID([]int{70004}); ok {
		t.Fatal("the infinity process has no job")
	}
	writeCmdline(t, roots, 70005, "slurmstepd: [3385+0.0]")
	if _, ok := m.SlurmstepdJobID([]int{70005}); ok {
		t.Fatal("an unrecognised title shape must stay unresolved, not be guessed")
	}
	if _, ok := m.SlurmstepdJobID(nil); ok {
		t.Fatal("no stepd, no answer")
	}
	if _, ok := m.SlurmstepdJobID([]int{888888}); ok {
		t.Fatal("exited stepd, no answer")
	}
}

func TestTGID(t *testing.T) {
	roots := build(t, fixture.Default(fixture.V2))
	m := procfd.NewMapper(roots.Proc, roots.Verbs)
	if tgid, ok := m.TGID(41001); !ok || tgid != 41001 {
		t.Fatalf("got %d,%v", tgid, ok)
	}
	// A thread: /proc/<tid>/status names its process.
	dir := filepath.Join(roots.Proc, "41077")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "status"), []byte("Name:\tpython\nTgid:\t41001\nPid:\t41077\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if tgid, ok := m.TGID(41077); !ok || tgid != 41001 {
		t.Fatalf("thread 41077 belongs to 41001, got %d,%v", tgid, ok)
	}
	if _, ok := m.TGID(999999); ok {
		t.Fatal("missing pid has no tgid")
	}
}

func TestDevicesForPIDsUnionsAcrossProcesses(t *testing.T) {
	spec := fixture.Default(fixture.V2)
	spec.PIDDevices[41002] = []int{0, 1} // one rank opens a second HCA
	roots := build(t, spec)
	devs, _ := procfd.NewMapper(roots.Proc, roots.Verbs).DevicesForPIDs([]int{41001, 41002})
	sort.Strings(devs)
	if len(devs) != 2 || devs[0] != "mlx5_0" || devs[1] != "mlx5_1" {
		t.Fatalf("got %v", devs)
	}
}
