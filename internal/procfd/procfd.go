// Package procfd maps processes to the HCAs they are actually using.
//
// This is the link that makes per-job network attribution possible at all.
// Slurm's cgroups give you PIDs; sysfs gives you per-device counters; nothing
// connects them. But a process doing RDMA must hold an open file descriptor on
// a uverbs character device, so:
//
//	/proc/<pid>/fd/*  →  /dev/infiniband/uverbs3  →  mlx5_3
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
	"os"
	"path/filepath"
	"regexp"
	"strings"
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

// DevicesForPID returns the HCA names a process has open.
//
// Errors are swallowed deliberately: /proc entries vanish constantly as jobs
// churn, and a race with process exit is normal operation, not a fault worth
// surfacing.
func (m *Mapper) DevicesForPID(pid int) []string {
	fdDir := filepath.Join(m.ProcRoot, itoa(pid), "fd")
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return nil
	}

	seen := map[string]bool{}
	for _, e := range entries {
		target, err := os.Readlink(filepath.Join(fdDir, e.Name()))
		if err != nil {
			continue
		}
		mm := uverbsRe.FindStringSubmatch(target)
		if mm == nil {
			continue
		}
		if dev := m.ibdevForUverbs("uverbs" + mm[1]); dev != "" {
			seen[dev] = true
		}
	}

	out := make([]string, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	return out
}

// DevicesForPIDs unions the devices across a job's processes.
func (m *Mapper) DevicesForPIDs(pids []int) []string {
	seen := map[string]bool{}
	for _, pid := range pids {
		for _, d := range m.DevicesForPID(pid) {
			seen[d] = true
		}
	}
	out := make([]string, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	return out
}

// ibdevForUverbs resolves a uverbs node to its HCA name via sysfs.
func (m *Mapper) ibdevForUverbs(uverbs string) string {
	b, err := os.ReadFile(filepath.Join(m.VerbsRoot, uverbs, "ibdev"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
