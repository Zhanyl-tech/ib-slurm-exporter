// Package cgroup discovers which PIDs belong to which Slurm job by walking the
// cgroup hierarchy slurmstepd creates.
//
// Three layouts exist in the wild and a deployable exporter has to handle all
// of them:
//
//	cgroup v1                 <root>/<ctrl>/slurm/uid_<uid>/job_<jobid>/step_<n>/
//	cgroup v2, before 26.05   <root>/system.slice/slurmstepd.scope/job_<jobid>/step_<n>/user/task_<t>/
//	cgroup v2, 26.05+         <root>/system.slice/slurmstepd.scope/<SLUID>/step_<n>/user/task_<t>/
//
// The 26.05 layout is the one that breaks existing exporters silently: the
// job directory is the bare SLUID — no "job_" prefix — e.g.
// ".../slurmstepd.scope/sEKNKTV3WPV500/" instead of ".../job_123/". That is
// the default; CgroupJobIdPaths=yes in cgroup.conf restores job_<jobid>.
// Sources: https://slurm.schedmd.com/cgroup.conf.html (CgroupJobIdPaths),
// https://slurm.schedmd.com/cgroup_v2.html (the example tree), and the 26.05
// CHANGELOG ("Slurm cgroup/v2 paths are now constructed using the SLUID
// instead of the numeric job id").
//
// Where the scope itself lives is not fixed either: CgroupSlice (cgroup.conf)
// moves it out of system.slice, and --enable-multiple-slurmd builds prepend
// the node name to "slurmstepd.scope" (cgroup_v2.html). The scope also holds
// a "system" directory for slurmstepd's infinity process, which is not a job.
//
// This package therefore treats the identifier as opaque. Whether it is a
// JobId or a SLUID is decided by inspecting it, and mapping a SLUID back to a
// job is left to the caller, because the SLUID does not contain the job ID.
//
// Caveat worth stating plainly: the 26.05 handling here is implemented from
// the documentation above and tested against synthetic trees. It has not
// been validated against a live 26.05 node.
package cgroup

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Allocation is one job's cgroup, with the processes currently inside it.
type Allocation struct {
	// Identifier is whatever slurmstepd put in the directory name: a numeric
	// JobId (v1, v2 before 26.05, or CgroupJobIdPaths=yes) or an opaque
	// SLUID (v2 on 26.05+ by default).
	Identifier string
	// IsSLUID reports that Identifier needs resolving before it means anything
	// to a human or to squeue.
	IsSLUID bool
	// JobID is populated once resolved. Equal to Identifier when numeric.
	JobID string
	// UID is only available in the v1 layout, which encodes it in the path.
	UID  int
	PIDs []int
	// StepdPIDs are the slurmstepd processes of this job, read from
	// step_*/slurm/cgroup.procs (v2). Their process title carries the job
	// ID, which is how a SLUID can be resolved without asking the controller.
	StepdPIDs []int
	// Path is kept for diagnostics; operators asking "where did this come
	// from" deserve a real answer.
	Path string
	// Incomplete is set when part of the job's cgroup subtree could not be
	// read, so PIDs may be missing. A job with unknown processes may be
	// using any HCA, which the caller has to treat as an evidence gap.
	Incomplete bool
}

// Layout names the hierarchy a scan matched.
type Layout string

const (
	LayoutV1      Layout = "cgroup-v1"
	LayoutV2      Layout = "cgroup-v2"
	LayoutV2SLUID Layout = "cgroup-v2-sluid"
	LayoutUnknown Layout = "unknown"
)

// DefaultScope is where Slurm puts slurmstepd.scope with the default
// CgroupSlice=system.slice.
const DefaultScope = "system.slice/slurmstepd.scope"

// Scanner walks a cgroup root. Root is injectable so the whole package is
// testable against a synthetic tree — which is also the only way to develop
// this on anything that is not Linux.
type Scanner struct {
	Root string
	// Scope, if set, is the slurmstepd scope directory relative to Root and
	// disables discovery. Needed when slurmd runs somewhere discovery does
	// not look, e.g. inside a container.
	Scope string
}

func NewScanner(root string) *Scanner {
	if root == "" {
		root = "/sys/fs/cgroup"
	}
	return &Scanner{Root: root}
}

// Result is one scan.
type Result struct {
	Allocations []Allocation
	Layout      Layout
	// Scopes are the Slurm cgroup roots found, relative to Root: v2
	// slurmstepd scopes, or v1 "<controller>/slurm" directories.
	Scopes []string
	// ScopeFound separates "Slurm's cgroup root exists but holds no jobs"
	// (an idle node) from "no Slurm cgroup root found" (wrong --cgroup-root,
	// wrong --slurm-scope, or slurmd not running).
	ScopeFound bool
}

var (
	jobDirRe  = regexp.MustCompile(`^job_(.+)$`)
	uidDirRe  = regexp.MustCompile(`^uid_(\d+)$`)
	numericRe = regexp.MustCompile(`^\d+$`)
)

// Scan returns every Slurm job cgroup found under Root. The error is non-nil
// when something that exists could not be read — the result may then be
// incomplete, and the caller should say so rather than report an idle node.
func (s *Scanner) Scan() (Result, error) {
	if _, err := os.Stat(s.Root); err != nil {
		return Result{Layout: LayoutUnknown}, fmt.Errorf("cgroup root: %w", err)
	}

	// v2 first: it is the default from Slurm 22.05 and the layout new clusters
	// will have.
	if scopes := s.v2Scopes(); len(scopes) > 0 {
		res := Result{Layout: LayoutV2, Scopes: scopes, ScopeFound: true}
		var errs []error
		for _, sc := range scopes {
			allocs, err := s.scanV2Scope(filepath.Join(s.Root, sc))
			if err != nil {
				errs = append(errs, err)
			}
			res.Allocations = append(res.Allocations, allocs...)
		}
		for _, a := range res.Allocations {
			if a.IsSLUID {
				res.Layout = LayoutV2SLUID
				break
			}
		}
		return res, errors.Join(errs...)
	}

	if res, err := s.scanV1(); res.ScopeFound || err != nil {
		return res, err
	}

	return Result{Layout: LayoutUnknown}, nil
}

// v2Scopes returns the slurmstepd scope directories to scan, relative to Root.
//
// Discovery is bounded to two slice levels on purpose: it covers the default
// system.slice, any CgroupSlice value (systemd slices end in ".slice", and a
// dash-named slice nests one level down), and the multiple-slurmd
// "<nodename>_slurmstepd.scope" naming, while staying cheap enough to run on
// every scrape. Anything deeper — slurmd inside a Kubernetes pod, for
// example — needs an explicit Scope.
func (s *Scanner) v2Scopes() []string {
	if s.Scope != "" {
		if isDir(filepath.Join(s.Root, s.Scope)) {
			return []string{filepath.Clean(s.Scope)}
		}
		return nil
	}
	var out []string
	for _, pattern := range []string{
		filepath.Join(s.Root, "*.slice", "*slurmstepd.scope"),
		filepath.Join(s.Root, "*.slice", "*.slice", "*slurmstepd.scope"),
	} {
		matches, _ := filepath.Glob(pattern) // only ErrBadPattern, impossible here
		for _, m := range matches {
			if !isDir(m) {
				continue
			}
			if rel, err := filepath.Rel(s.Root, m); err == nil {
				out = append(out, rel)
			}
		}
	}
	sort.Strings(out)
	return out
}

// scanV2Scope reads one slurmstepd scope.
func (s *Scanner) scanV2Scope(scope string) ([]Allocation, error) {
	entries, err := os.ReadDir(scope)
	if err != nil {
		return nil, fmt.Errorf("read slurmstepd scope %s: %w", scope, err)
	}

	var out []Allocation
	var errs []error
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		// "system" holds slurmstepd's infinity process and new stepds
		// before they move into a job (cgroup_v2.html). It is never a job.
		if name == "system" {
			continue
		}
		path := filepath.Join(scope, name)

		var ident string
		if m := jobDirRe.FindStringSubmatch(name); m != nil {
			// job_<jobid>: pre-26.05, or 26.05 with CgroupJobIdPaths=yes.
			ident = m[1]
		} else {
			// 26.05 default: the bare SLUID. Recognised structurally — a
			// job directory holds step_* children — rather than by guessing
			// the SLUID alphabet, which SchedMD does not document.
			ok, err := hasStepChild(path)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if !ok {
				continue
			}
			ident = name
		}

		a := newAllocation(ident, -1, path)
		a.PIDs, a.StepdPIDs, err = collectPIDs(path)
		if err != nil {
			a.Incomplete = true
			errs = append(errs, err)
		}
		out = append(out, a)
	}
	return out, errors.Join(errs...)
}

// scanV1 walks <controller>/slurm/uid_*/job_*.
func (s *Scanner) scanV1() (Result, error) {
	// freezer and memory are the controllers Slurm reliably populates. The
	// first controller that has a slurm/ directory with jobs wins; if none
	// has jobs, the first slurm/ directory found still counts as "Slurm is
	// here, idle".
	var idle *Result
	for _, controller := range []string{"freezer", "memory", "cpuacct", "cpu,cpuacct"} {
		rel := filepath.Join(controller, "slurm")
		base := filepath.Join(s.Root, rel)
		res := Result{Layout: LayoutV1, Scopes: []string{rel}, ScopeFound: true}

		uidDirs, err := os.ReadDir(base)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return res, fmt.Errorf("read %s: %w", base, err)
		}

		var errs []error
		for _, ud := range uidDirs {
			um := uidDirRe.FindStringSubmatch(ud.Name())
			if um == nil {
				continue
			}
			uid, _ := strconv.Atoi(um[1])

			jobDirs, err := os.ReadDir(filepath.Join(base, ud.Name()))
			if err != nil {
				// Slurm's v1 plugin removes uid_<uid> when that user's last
				// job on the node ends (_remove_cg_subsystem in
				// src/plugins/cgroup/v1/cgroup_v1.c), so one listed a moment
				// ago can be gone: a job ending, like ENOENT in collectPIDs,
				// not an unreadable cgroup.
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				errs = append(errs, fmt.Errorf("read %s: %w", filepath.Join(base, ud.Name()), err))
				continue
			}
			for _, jd := range jobDirs {
				jm := jobDirRe.FindStringSubmatch(jd.Name())
				if jm == nil {
					continue
				}
				path := filepath.Join(base, ud.Name(), jd.Name())
				a := newAllocation(jm[1], uid, path)
				a.PIDs, _, err = collectPIDs(path)
				if err != nil {
					a.Incomplete = true
					errs = append(errs, err)
				}
				res.Allocations = append(res.Allocations, a)
			}
		}
		if len(res.Allocations) > 0 || len(errs) > 0 {
			return res, errors.Join(errs...)
		}
		if idle == nil {
			idle = &res
		}
	}
	if idle != nil {
		return *idle, nil
	}
	return Result{Layout: LayoutUnknown}, nil
}

func newAllocation(identifier string, uid int, path string) Allocation {
	numeric := numericRe.MatchString(identifier)
	a := Allocation{
		Identifier: identifier,
		IsSLUID:    !numeric,
		UID:        uid,
		Path:       path,
	}
	if numeric {
		a.JobID = identifier
	}
	return a
}

// hasStepChild reports whether dir contains a step_* directory, which is
// what makes a directory under the scope a job.
func hasStepChild(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil // job ended mid-scan
		}
		return false, fmt.Errorf("read %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "step_") {
			return true, nil
		}
	}
	return false, nil
}

// ReadPIDs re-reads the processes in a job's cgroup subtree. The collector
// uses it to tell a process that exited from one it cannot see.
func ReadPIDs(jobPath string) ([]int, error) {
	pids, _, err := collectPIDs(jobPath)
	return pids, err
}

// collectPIDs reads cgroup.procs for a job and every cgroup beneath it.
//
// The job-level file is often empty because the processes live in the step
// (v1) or step_<n>/user/task_<t> (v2) cgroups, so a reader that only looks at
// the top level reports no PIDs for every running job and silently produces
// an exporter with no data.
//
// A subtree that disappears mid-walk is a job or step ending, which is
// normal. Anything else that cannot be read is returned as an error, because
// a missing PID list is not the same thing as an empty one.
func collectPIDs(jobPath string) (pids, stepd []int, err error) {
	seen := map[int]bool{}
	seenStepd := map[int]bool{}
	var errs []error

	walkErr := filepath.WalkDir(jobPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, err)
			}
			if d != nil && d.IsDir() && path != jobPath {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() || d.Name() != "cgroup.procs" {
			return nil
		}
		ps, err := readProcs(path)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, err)
			}
			return nil
		}
		// step_<n>/slurm/cgroup.procs holds that step's slurmstepd.
		dir := filepath.Dir(path)
		isStepd := filepath.Base(dir) == "slurm" &&
			strings.HasPrefix(filepath.Base(filepath.Dir(dir)), "step_")
		for _, p := range ps {
			seen[p] = true
			if isStepd {
				seenStepd[p] = true
			}
		}
		return nil
	})
	if walkErr != nil {
		errs = append(errs, walkErr)
	}

	return sortedKeys(seen), sortedKeys(seenStepd), errors.Join(errs...)
}

func sortedKeys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for p := range m {
		out = append(out, p)
	}
	sort.Ints(out)
	return out
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

func isDir(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// Resolver maps an opaque SLUID back to a job ID. Implementations must
// answer from memory: the collector calls this on every sample, so a
// resolver that talks to the controller would put slurmctld on the scrape
// path.
type Resolver interface {
	ResolveSLUID(sluid string) (jobID string, err error)
}

// String aids log output.
func (a Allocation) String() string {
	id := a.JobID
	if id == "" {
		id = fmt.Sprintf("sluid:%s", a.Identifier)
	}
	return fmt.Sprintf("job=%s pids=%d", id, len(a.PIDs))
}
