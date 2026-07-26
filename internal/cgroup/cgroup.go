// Package cgroup discovers which PIDs belong to which Slurm job by walking the
// cgroup hierarchy slurmstepd creates.
//
// Three layouts exist in the wild and a deployable exporter has to handle all
// of them:
//
//	cgroup v1                 /sys/fs/cgroup/<ctrl>/slurm/uid_<uid>/job_<jobid>/step_<n>/
//	cgroup v2                 /sys/fs/cgroup/system.slice/slurmstepd.scope/job_<jobid>/step_<n>/
//	cgroup v2, Slurm 26.05+   ...slurmstepd.scope/job_<SLUID>/step_<n>/
//
// The 26.05 change is the one that breaks existing exporters silently. Its
// release notes say cgroup/v2 directories are "keyed off of SLUID and not the
// JobId" — so code that does strconv.Atoi on the segment after "job_" now gets
// either a parse error or, worse, a number that is not a job ID.
//
// This package therefore treats the identifier as opaque. Whether it is a
// JobId or a SLUID is decided by inspecting it, and mapping a SLUID back to a
// job is left to a Resolver, because that requires talking to the controller.
//
// Caveat worth stating plainly: the SLUID path here is implemented from the
// release notes. It has not been validated against a live 26.05 cluster.
package cgroup

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// Allocation is one job's cgroup, with the processes currently inside it.
type Allocation struct {
	// Identifier is whatever slurmstepd put in the directory name. Numeric on
	// Slurm < 26.05 (a JobId), opaque on 26.05+ (a SLUID).
	Identifier string
	// IsSLUID reports that Identifier needs resolving before it means anything
	// to a human or to squeue.
	IsSLUID bool
	// JobID is populated once resolved. Equal to Identifier when numeric.
	JobID string
	// UID is only available in the v1 layout, which encodes it in the path.
	UID  int
	PIDs []int
	// Path is kept for diagnostics; operators asking "where did this come
	// from" deserve a real answer.
	Path string
}

// Layout names the hierarchy a scan matched.
type Layout string

const (
	LayoutV1        Layout = "cgroup-v1"
	LayoutV2        Layout = "cgroup-v2"
	LayoutV2SLUID   Layout = "cgroup-v2-sluid"
	LayoutUnknown   Layout = "unknown"
)

// Scanner walks a cgroup root. Root is injectable so the whole package is
// testable against a synthetic tree — which is also the only way to develop
// this on anything that is not Linux.
type Scanner struct {
	Root string
}

func NewScanner(root string) *Scanner {
	if root == "" {
		root = "/sys/fs/cgroup"
	}
	return &Scanner{Root: root}
}

var (
	jobDirRe = regexp.MustCompile(`^job_(.+)$`)
	uidDirRe = regexp.MustCompile(`^uid_(\d+)$`)
	numericRe = regexp.MustCompile(`^\d+$`)
)

// Scan returns every Slurm job cgroup found under Root.
func (s *Scanner) Scan() ([]Allocation, Layout, error) {
	// v2 first: it is the default from Slurm 22.05 and the layout new clusters
	// will have.
	if allocs, err := s.scanV2(); err == nil && len(allocs) > 0 {
		layout := LayoutV2
		for _, a := range allocs {
			if a.IsSLUID {
				layout = LayoutV2SLUID
				break
			}
		}
		return allocs, layout, nil
	}

	if allocs, err := s.scanV1(); err == nil && len(allocs) > 0 {
		return allocs, LayoutV1, nil
	}

	return nil, LayoutUnknown, nil
}

// scanV2 walks system.slice/slurmstepd.scope.
func (s *Scanner) scanV2() ([]Allocation, error) {
	// Slurm has shipped both of these; check each.
	roots := []string{
		filepath.Join(s.Root, "system.slice", "slurmstepd.scope"),
		filepath.Join(s.Root, "system.slice", "slurmstepd.scope", "system"),
	}

	var out []Allocation
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			m := jobDirRe.FindStringSubmatch(e.Name())
			if m == nil {
				continue
			}
			path := filepath.Join(root, e.Name())
			pids, err := collectPIDs(path)
			if err != nil {
				continue
			}
			out = append(out, newAllocation(m[1], -1, pids, path))
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	return out, nil
}

// scanV1 walks <controller>/slurm/uid_*/job_*.
func (s *Scanner) scanV1() ([]Allocation, error) {
	// freezer and memory are the controllers Slurm reliably populates.
	var out []Allocation
	for _, controller := range []string{"freezer", "memory", "cpuacct", "cpu,cpuacct"} {
		base := filepath.Join(s.Root, controller, "slurm")
		uidDirs, err := os.ReadDir(base)
		if err != nil {
			continue
		}
		for _, ud := range uidDirs {
			um := uidDirRe.FindStringSubmatch(ud.Name())
			if um == nil {
				continue
			}
			uid, _ := strconv.Atoi(um[1])

			jobDirs, err := os.ReadDir(filepath.Join(base, ud.Name()))
			if err != nil {
				continue
			}
			for _, jd := range jobDirs {
				jm := jobDirRe.FindStringSubmatch(jd.Name())
				if jm == nil {
					continue
				}
				path := filepath.Join(base, ud.Name(), jd.Name())
				pids, err := collectPIDs(path)
				if err != nil {
					continue
				}
				out = append(out, newAllocation(jm[1], uid, pids, path))
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	return out, nil
}

func newAllocation(identifier string, uid int, pids []int, path string) Allocation {
	numeric := numericRe.MatchString(identifier)
	a := Allocation{
		Identifier: identifier,
		IsSLUID:    !numeric,
		UID:        uid,
		PIDs:       pids,
		Path:       path,
	}
	if numeric {
		a.JobID = identifier
	}
	return a
}

// collectPIDs reads cgroup.procs for a job and every step beneath it.
//
// The job-level file is often empty because the processes live in the step
// cgroups, so a reader that only looks at the top level reports no PIDs for
// every running job and silently produces an exporter with no data.
func collectPIDs(jobPath string) ([]int, error) {
	seen := map[int]bool{}

	if err := filepath.WalkDir(jobPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // an unreadable subtree is not fatal
		}
		if d.IsDir() || d.Name() != "cgroup.procs" {
			return nil
		}
		pids, err := readProcs(path)
		if err != nil {
			return nil
		}
		for _, p := range pids {
			seen[p] = true
		}
		return nil
	}); err != nil {
		return nil, err
	}

	out := make([]int, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	return out, nil
}

func readProcs(path string) ([]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var pids []int
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		pid, err := strconv.Atoi(line)
		if err != nil {
			continue
		}
		pids = append(pids, pid)
	}
	return pids, sc.Err()
}

// Resolver maps an opaque SLUID back to a job ID.
type Resolver interface {
	ResolveSLUID(sluid string) (jobID string, err error)
}

// Resolve fills in JobID for allocations keyed by SLUID. Allocations that
// cannot be resolved keep an empty JobID and are reported by the caller rather
// than silently mislabelled — a metric attributed to the wrong job is worse
// than one attributed to none.
func Resolve(allocs []Allocation, r Resolver) []Allocation {
	if r == nil {
		return allocs
	}
	out := make([]Allocation, len(allocs))
	copy(out, allocs)
	for i := range out {
		if !out[i].IsSLUID || out[i].JobID != "" {
			continue
		}
		if jid, err := r.ResolveSLUID(out[i].Identifier); err == nil {
			out[i].JobID = jid
		}
	}
	return out
}

// String aids log output.
func (a Allocation) String() string {
	id := a.JobID
	if id == "" {
		id = fmt.Sprintf("sluid:%s", a.Identifier)
	}
	return fmt.Sprintf("job=%s pids=%d", id, len(a.PIDs))
}
