// Package procfd maps processes to the HCAs they have open.
//
// This is the per-process link the exporter uses by default. Slurm's cgroups
// give you PIDs; sysfs gives you per-port counters. A userspace process doing
// RDMA through libibverbs holds an open file descriptor on a uverbs character
// device, so:
//
//	/proc/<pid>/fd/*  →  /dev/infiniband/uverbs3  →  mlx5_3
//
// What this link does not show, and the collector has to be honest about:
// kernel RDMA consumers (IPoIB, NFS/RDMA, Lustre o2ib, ...) create queue pairs
// with no process and no file descriptor, and an open descriptor carries no
// port number. See the collector package for how that limits attribution.
//
// The final hop is not a string manipulation — uverbs numbering does not have
// to match HCA numbering — so it is resolved through sysfs:
//
//	/sys/class/infiniband_verbs/uverbs3/ibdev  →  "mlx5_3"
//
// A host where that mapping happens to be the identity is exactly the host
// where a shortcut would look correct and be wrong somewhere else.
package procfd

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
)

// Mapper resolves PIDs to HCA device names. Both roots are injectable so the
// package is testable against a synthetic tree.
type Mapper struct {
	ProcRoot  string // /proc
	VerbsRoot string // /sys/class/infiniband_verbs
}

func NewMapper(procRoot, verbsRoot string) *Mapper {
	if procRoot == "" {
		procRoot = "/proc"
	}
	if verbsRoot == "" {
		verbsRoot = "/sys/class/infiniband_verbs"
	}
	return &Mapper{ProcRoot: procRoot, VerbsRoot: verbsRoot}
}

var uverbsRe = regexp.MustCompile(`uverbs(\d+)$`)

// ErrGone reports that a process's /proc entry does not exist. Usually the
// process exited between the cgroup read and this one, which is normal
// churn. It is also exactly what a hidepid=2 /proc mount, or a /proc from a
// different PID namespace, looks like — so callers must not treat it as
// proof the process is gone without re-checking cgroup membership.
var ErrGone = errors.New("process not visible in /proc")

// DevicesForPID returns the HCA names a process has open.
//
// Errors are classified rather than swallowed. A missing /proc entry wraps
// ErrGone. Anything else is returned as-is; EACCES is the common case (for
// another user's process, listing fd/ needs CAP_DAC_READ_SEARCH and reading
// its links needs CAP_SYS_PTRACE, see IsPermission). An unreadable
// descriptor table is not evidence that the process holds no HCA, and
// treating it as such would let another job look like a device's sole user.
func (m *Mapper) DevicesForPID(pid int) ([]string, error) {
	fdDir := filepath.Join(m.ProcRoot, strconv.Itoa(pid), "fd")
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		if isGone(err) {
			return nil, fmt.Errorf("pid %d: %w", pid, ErrGone)
		}
		return nil, fmt.Errorf("read %s: %w", fdDir, err)
	}

	seen := map[string]bool{}
	for _, e := range entries {
		target, err := os.Readlink(filepath.Join(fdDir, e.Name()))
		if err != nil {
			if isGone(err) {
				continue // descriptor closed while we were listing
			}
			return nil, fmt.Errorf("readlink %s: %w", filepath.Join(fdDir, e.Name()), err)
		}
		mm := uverbsRe.FindStringSubmatch(target)
		if mm == nil {
			continue
		}
		dev, err := m.ibdevForUverbs("uverbs" + mm[1])
		if err != nil {
			// The process holds an HCA we cannot name. Same consequence as
			// an unreadable fd table: the device's user set is incomplete.
			return nil, fmt.Errorf("pid %d: %w", pid, err)
		}
		seen[dev] = true
	}

	out := make([]string, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	return out, nil
}

// Stats summarises what could and could not be read for a set of PIDs.
type Stats struct {
	// Gone lists PIDs whose /proc entry was missing (see ErrGone).
	Gone []int
	// Unreadable counts PIDs whose descriptors could not be read for any
	// other reason. Non-zero means the device→user mapping is incomplete.
	Unreadable int
	// FirstErr is the first unreadable error, for a log line.
	FirstErr error
}

// DevicesForPIDs unions the devices across a job's processes and reports
// which PIDs could not be inspected.
func (m *Mapper) DevicesForPIDs(pids []int) ([]string, Stats) {
	var st Stats
	seen := map[string]bool{}
	for _, pid := range pids {
		devs, err := m.DevicesForPID(pid)
		if err != nil {
			if errors.Is(err, ErrGone) {
				st.Gone = append(st.Gone, pid)
				continue
			}
			st.Unreadable++
			if st.FirstErr == nil {
				st.FirstErr = err
			}
			continue
		}
		for _, d := range devs {
			seen[d] = true
		}
	}
	out := make([]string, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	return out, st
}

// IsPermission reports whether err is a permission failure, so the caller can
// name the privileges instead of a bare errno. From the kernel source (not
// tested on a live kernel from this repository): /proc/<pid>/fd is created
// as DIR("fd", S_IRUSR|S_IXUSR, ...) in fs/proc/base.c, mode 0500 and owned
// by the task's user; proc_fd_permission (fs/proc/fd.c) checks only
// generic_permission and the caller's own thread group, and
// generic_permission (fs/namei.c) lets CAP_DAC_READ_SEARCH (or
// CAP_DAC_OVERRIDE) past a directory's mode. Reading an entry's link then
// goes through call_proc_get_link (fs/proc/base.c), which requires
// ptrace_may_access(PTRACE_MODE_READ_FSCREDS), i.e. CAP_SYS_PTRACE for
// another user's process. A non-root exporter needs both capabilities.
func IsPermission(err error) bool {
	return errors.Is(err, fs.ErrPermission)
}

// ibdevForUverbs resolves a uverbs node to its HCA name via sysfs.
func (m *Mapper) ibdevForUverbs(uverbs string) (string, error) {
	b, err := os.ReadFile(filepath.Join(m.VerbsRoot, uverbs, "ibdev"))
	if err != nil {
		return "", fmt.Errorf("resolve %s to an HCA: %w", uverbs, err)
	}
	dev := strings.TrimSpace(string(b))
	if dev == "" {
		return "", fmt.Errorf("resolve %s to an HCA: empty ibdev", uverbs)
	}
	return dev, nil
}

// stepdTitleRe matches slurmstepd's process title, e.g. "slurmstepd:
// [3385.0]" or "slurmstepd: [3385.extern]". The format is the one shown in
// the example process tree on https://slurm.schedmd.com/cgroup_v2.html
// (systemd-cgls output, which prints /proc/<pid>/cmdline). It has not been
// checked on a live node. Anything that does not match exactly — for example
// a heterogeneous-job title in a different shape — is left unresolved.
var stepdTitleRe = regexp.MustCompile(`^slurmstepd: \[(\d+)\.[A-Za-z0-9_]+\]$`)

// SlurmstepdJobID reads the process titles of a job's slurmstepd processes
// (the PIDs in <job>/step_*/slurm/cgroup.procs) and returns the numeric job
// ID they name.
//
// Slurm 26.05 names job cgroups by SLUID, which does not contain the job ID.
// The slurmstepd title does, and it is local: no controller call. Every
// readable title must agree; any disagreement returns false, because a
// metric on the wrong job is worse than a metric on no job.
func (m *Mapper) SlurmstepdJobID(pids []int) (string, bool) {
	var id string
	for _, pid := range pids {
		b, err := os.ReadFile(filepath.Join(m.ProcRoot, strconv.Itoa(pid), "cmdline"))
		if err != nil {
			continue // exited; another stepd may still answer
		}
		// setproctitle leaves the title NUL- or space-padded.
		title := strings.TrimSpace(string(bytes.ReplaceAll(b, []byte{0}, []byte{' '})))
		mm := stepdTitleRe.FindStringSubmatch(title)
		if mm == nil {
			return "", false
		}
		if id != "" && id != mm[1] {
			return "", false
		}
		id = mm[1]
	}
	return id, id != ""
}

// TGID returns the thread-group (process) ID for a PID or thread ID, from
// /proc/<pid>/status. RDMA resource tracking can report a thread's ID,
// while cgroup.procs lists processes.
func (m *Mapper) TGID(pid int) (int, bool) {
	f, err := os.Open(filepath.Join(m.ProcRoot, strconv.Itoa(pid), "status"))
	if err != nil {
		return 0, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "Tgid:"); ok {
			n, err := strconv.Atoi(strings.TrimSpace(v))
			return n, err == nil && n > 0
		}
	}
	return 0, false
}

func isGone(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH)
}
