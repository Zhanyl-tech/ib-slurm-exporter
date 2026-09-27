// Package slurm supplies job metadata (user, account, partition) and the
// SLUID → job ID map from squeue, cached in the background.
//
// Design constraints, each from the squeue man page
// (https://slurm.schedmd.com/squeue.html):
//
//   - "Do not run squeue or other Slurm client commands that send remote
//     procedure calls to slurmctld from loops in shell scripts or other
//     programs." One exporter per compute node is exactly such a loop, so
//     the call is node-scoped (--nodelist=localhost: "A node_name of
//     localhost is mapped to the current host name"), infrequent (never
//     more often than MinInterval), jittered so a DaemonSet rollout does
//     not line every node up, and entirely optional. Whether slurmctld
//     filters by node server-side, and so
//     whether this reduces controller work rather than just output size,
//     has not been verified.
//   - The Sluid format field is "The Slurm Lexicographically-sortable
//     Unique Identifier assigned to the job. This value remains constant
//     during the lifetime of the job", so one cached answer is good for the
//     job's whole life and a scrape never needs to ask the controller.
//   - JobID "will have a unique value for each element of job arrays", i.e.
//     the raw per-element ID — the same number cgroup directories use.
//
// Nothing here runs on the scrape path. A failed refresh keeps the previous
// cache, increments a counter and is logged at a bounded rate, so a broken
// squeue is visible instead of quietly producing empty labels.
package slurm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/collector"
)

// Squeue is a cached squeue reader. It implements collector.MetaSource,
// cgroup.Resolver and prometheus.Collector (for its own health metrics).
type Squeue struct {
	Path     string        // absolute path to squeue
	NodeList string        // passed to --nodelist; "localhost" by default
	Timeout  time.Duration // per call
	Logger   *slog.Logger

	// run executes a command; replaced in tests.
	run func(ctx context.Context, name string, args ...string) ([]byte, error)
	now func() time.Time

	mu          sync.RWMutex
	jobs        map[string]collector.JobMeta
	sluids      map[string]string
	lastSuccess time.Time
	errorsTotal float64
	noSluid     bool // this squeue refused the Sluid field by name
	noSluidAt   time.Time
	lastErrLog  time.Time
	failing     bool

	lastSuccessDesc *prometheus.Desc
	errorsDesc      *prometheus.Desc
	jobsDesc        *prometheus.Desc
}

// New returns a Squeue that has not refreshed yet.
func New(path, nodeList string, logger *slog.Logger) *Squeue {
	if nodeList == "" {
		nodeList = "localhost"
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Squeue{
		Path:     path,
		NodeList: nodeList,
		Timeout:  15 * time.Second,
		Logger:   logger,
		run:      runCommand,
		now:      time.Now,
		lastSuccessDesc: prometheus.NewDesc("ib_slurm_metadata_last_success_timestamp_seconds",
			"Unix time of the last successful squeue refresh; 0 if none has succeeded. Job labels are at most this old.",
			nil, nil),
		errorsDesc: prometheus.NewDesc("ib_slurm_metadata_refresh_errors_total",
			"squeue refreshes that failed. The previous cache is kept when one fails.",
			nil, nil),
		jobsDesc: prometheus.NewDesc("ib_slurm_metadata_jobs",
			"Jobs in the squeue metadata cache for this node.",
			nil, nil),
	}
}

func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	out, err := exec.CommandContext(ctx, name, args...).Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) && len(ee.Stderr) > 0 {
			return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(string(ee.Stderr)))
		}
	}
	return out, err
}

// Format fields. Sluid goes last so an squeue that does not know it can only
// ever add or drop a trailing column, never shift the others.
const (
	formatBase  = "JobID:|,UserName:|,Account:|,Partition:"
	formatSluid = "JobID:|,UserName:|,Account:|,Partition:|,Sluid:"
)

func (s *Squeue) args(withSluid bool) []string {
	f := formatBase
	if withSluid {
		f = formatSluid
	}
	return []string{"--noheader", "--nodelist=" + s.NodeList,
		"--states=RUNNING,COMPLETING", "--Format=" + f}
}

// Refresh runs squeue once and replaces the cache on success.
func (s *Squeue) Refresh(ctx context.Context) error {
	s.mu.RLock()
	// Retry the Sluid field hourly, so one unlucky transient failure cannot
	// switch it off for the life of the process.
	withSluid := !s.noSluid || s.now().Sub(s.noSluidAt) >= time.Hour
	s.mu.RUnlock()

	out, err := s.call(ctx, withSluid)
	if err != nil && withSluid && namesSluid(err) {
		// Sluid is new in 26.05. Per the slurm-25.05 source an older squeue
		// does not fail on it: parse_long_format (src/squeue/opts.c) logs
		// "Invalid job format specification: Sluid", prints an empty
		// column, and carries on, so the first call succeeds and simply
		// maps no SLUIDs. This path is for a squeue that does exit
		// non-zero over the field. It needs the error to name the field:
		// any other failure (a controller timeout, munge, ...) is an
		// ordinary one, counted below and retried with Sluid next time.
		// Falling back on those would switch SLUID lookups off for an hour
		// on one bad call, and call a failing controller twice.
		if out2, err2 := s.call(ctx, false); err2 == nil {
			s.mu.Lock()
			first := !s.noSluid
			s.noSluid, s.noSluidAt = true, s.now()
			s.mu.Unlock()
			if first {
				s.Logger.Info("squeue does not accept the Sluid field; SLUIDs will be resolved from slurmstepd titles only",
					"err", err)
			}
			out, err = out2, nil
		}
	} else if err == nil && withSluid {
		s.mu.Lock()
		s.noSluid = false
		s.mu.Unlock()
	}
	if err != nil {
		s.recordFailure(err)
		return err
	}

	jobs, sluids, bad := ParseSqueue(out)
	if bad > 0 {
		s.Logger.Warn("squeue output had unparseable lines; they were skipped", "lines", bad)
	}

	s.mu.Lock()
	s.jobs, s.sluids = jobs, sluids
	s.lastSuccess = s.now()
	recovered := s.failing
	s.failing = false
	s.mu.Unlock()
	if recovered {
		s.Logger.Info("squeue refresh recovered", "jobs", len(jobs))
	}
	return nil
}

// namesSluid reports whether a failed squeue call complained about the
// Sluid field itself (runCommand appends squeue's stderr to the error).
func namesSluid(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "sluid")
}

func (s *Squeue) call(ctx context.Context, withSluid bool) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	return s.run(ctx, s.Path, s.args(withSluid)...)
}

// recordFailure counts every failure and logs the first one and then at most
// one every ten minutes, so a dead controller does not flood the journal but
// is never silent.
func (s *Squeue) recordFailure(err error) {
	s.mu.Lock()
	s.errorsTotal++
	now := s.now()
	shouldLog := !s.failing || now.Sub(s.lastErrLog) >= 10*time.Minute
	s.failing = true
	if shouldLog {
		s.lastErrLog = now
	}
	last := s.lastSuccess
	s.mu.Unlock()
	if shouldLog {
		s.Logger.Warn("squeue refresh failed; keeping previous job metadata",
			"err", err, "path", s.Path, "last_success", last)
	}
}

// MinInterval is the shortest refresh interval Run accepts. Every compute
// node runs one exporter, so an interval of 0 (which an operator might pass
// expecting "disabled") would otherwise run squeue back to back on every
// node: the slurmctld RPC loop squeue(1) warns against.
const MinInterval = 10 * time.Second

// Run refreshes immediately, then every interval ±20% until ctx ends. An
// interval below MinInterval is raised to it.
func (s *Squeue) Run(ctx context.Context, interval time.Duration) {
	if interval < MinInterval {
		s.Logger.Warn("squeue interval below the minimum; using the minimum",
			"interval", interval, "minimum", MinInterval)
		interval = MinInterval
	}
	for {
		_ = s.Refresh(ctx) // failures are counted and logged in Refresh
		select {
		case <-ctx.Done():
			return
		case <-time.After(Jitter(interval, rand.Float64())):
		}
	}
}

// Jitter spreads interval by ±20%; u is uniform in [0,1).
func Jitter(interval time.Duration, u float64) time.Duration {
	return time.Duration(float64(interval) * (0.8 + 0.4*u))
}

var numericRe = regexp.MustCompile(`^\d+$`)

// ParseSqueue parses `--noheader --Format=JobID:|,UserName:|,Account:|,
// Partition:[|,Sluid:]` output. squeue pads each field to its minimum width
// and appends the "|" suffix, so fields are split on "|" and trimmed. Lines
// whose JobID is not numeric are counted as bad and skipped rather than
// guessed at.
func ParseSqueue(out []byte) (jobs map[string]collector.JobMeta, sluids map[string]string, bad int) {
	jobs = map[string]collector.JobMeta{}
	sluids = map[string]string{}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		f := strings.Split(line, "|")
		if len(f) < 4 {
			bad++
			continue
		}
		for i := range f {
			f[i] = strings.TrimSpace(f[i])
		}
		id := f[0]
		if !numericRe.MatchString(id) {
			bad++
			continue
		}
		jobs[id] = collector.JobMeta{User: f[1], Account: f[2], Partition: f[3]}
		if len(f) >= 5 && f[4] != "" {
			sluids[f[4]] = id
		}
	}
	return jobs, sluids, bad
}

// Meta implements collector.MetaSource.
func (s *Squeue) Meta(jobID string) (collector.JobMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.jobs[jobID]
	return m, ok
}

// ResolveSLUID implements cgroup.Resolver from the cache only. It never
// runs a command: a SLUID that is not cached yet stays unresolved until the
// next background refresh.
func (s *Squeue) ResolveSLUID(sluid string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if id, ok := s.sluids[sluid]; ok {
		return id, nil
	}
	return "", fmt.Errorf("sluid %q not in squeue cache", sluid)
}

// Describe implements prometheus.Collector.
func (s *Squeue) Describe(ch chan<- *prometheus.Desc) {
	ch <- s.lastSuccessDesc
	ch <- s.errorsDesc
	ch <- s.jobsDesc
}

// Collect implements prometheus.Collector.
func (s *Squeue) Collect(ch chan<- prometheus.Metric) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ts := 0.0
	if !s.lastSuccess.IsZero() {
		ts = float64(s.lastSuccess.UnixNano()) / 1e9
	}
	ch <- prometheus.MustNewConstMetric(s.lastSuccessDesc, prometheus.GaugeValue, ts)
	ch <- prometheus.MustNewConstMetric(s.errorsDesc, prometheus.CounterValue, s.errorsTotal)
	ch <- prometheus.MustNewConstMetric(s.jobsDesc, prometheus.GaugeValue, float64(len(s.jobs)))
}
