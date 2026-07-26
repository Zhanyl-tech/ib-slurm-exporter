// Command ib-slurm-exporter exports InfiniBand and RoCE counters correlated
// with the Slurm job that owns them.
//
// Runs per compute node. Reads only local sysfs, /proc and cgroups; job
// metadata comes from squeue.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/cgroup"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/collector"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/fixture"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/ib"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/procfd"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ib-slurm-exporter: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		listen     = flag.String("listen", ":9836", "metrics listen address")
		sysRoot    = flag.String("sys-root", "/sys/class/infiniband", "infiniband sysfs root")
		verbsRoot  = flag.String("verbs-root", "/sys/class/infiniband_verbs", "uverbs sysfs root")
		procRoot   = flag.String("proc-root", "/proc", "proc root")
		cgroupRoot = flag.String("cgroup-root", "/sys/fs/cgroup", "cgroup root")
		demo       = flag.Bool("demo", false, "serve a synthetic cluster; needs no InfiniBand hardware")
		demoLayout = flag.String("demo-layout", "v2", "demo cgroup layout: v1 | v2 | v2-sluid")
		once       = flag.Bool("once", false, "print the exposition once and exit")
		showVer    = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVer {
		fmt.Println(version)
		return nil
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	var resolver cgroup.Resolver
	var meta collector.MetaSource

	if *demo {
		dir, err := os.MkdirTemp("", "ib-slurm-demo-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)

		roots, err := fixture.Build(dir, fixture.Default(fixture.Layout(*demoLayout)))
		if err != nil {
			return fmt.Errorf("build demo fixture: %w", err)
		}
		*sysRoot, *verbsRoot = roots.Sys, roots.Verbs
		*procRoot, *cgroupRoot = roots.Proc, roots.Cgroup
		resolver = demoResolver{}
		meta = demoMeta{}

		logger.Info("demo mode — synthetic cluster, no hardware required",
			"layout", *demoLayout, "root", dir)
		logger.Info("expect: 918001 attributed on mlx5_0; 918002+918003 suppressed " +
			"(they share mlx5_1); 918004 attributed on mlx5_2 (RoCE)")
	} else {
		resolver = &scontrolResolver{}
		sq := &squeueMeta{}
		if err := sq.refresh(); err != nil {
			logger.Warn("squeue unavailable; series will carry job_id only", "err", err)
		}
		go sq.loop(30 * time.Second)
		meta = sq
	}

	scanner := cgroup.NewScanner(*cgroupRoot)
	c := collector.New(scanner, ib.NewReader(*sysRoot),
		procfd.NewMapper(*procRoot, *verbsRoot), meta, logger)
	c.SetResolver(resolver)

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), c)

	if *once {
		return dumpOnce(reg)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("listening", "addr", *listen, "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve", "err", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutCtx)
}

// dumpOnce renders the exposition to stdout — handy in CI and for a quick look.
func dumpOnce(reg *prometheus.Registry) error {
	families, err := reg.Gather()
	if err != nil {
		return err
	}
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "ib_slurm") {
			continue
		}
		for _, m := range f.GetMetric() {
			var labels []string
			for _, l := range m.GetLabel() {
				labels = append(labels, fmt.Sprintf("%s=%q", l.GetName(), l.GetValue()))
			}
			v := m.GetGauge().GetValue()
			if m.GetCounter() != nil {
				v = m.GetCounter().GetValue()
			}
			fmt.Printf("%s{%s} %g\n", f.GetName(), strings.Join(labels, ","), v)
		}
	}
	return nil
}

// ── Demo sources ───────────────────────────────────────────────────────────

type demoResolver struct{}

func (demoResolver) ResolveSLUID(s string) (string, error) {
	if id, ok := fixture.SLUIDToJob(s); ok {
		return id, nil
	}
	return "", fmt.Errorf("unknown sluid %q", s)
}

type demoMeta struct{}

func (demoMeta) Meta(jobID string) (collector.JobMeta, bool) {
	switch jobID {
	case "918001":
		return collector.JobMeta{User: "alice", Account: "research", Partition: "gpu"}, true
	case "918002":
		return collector.JobMeta{User: "bob", Account: "infra", Partition: "gpu"}, true
	case "918003":
		return collector.JobMeta{User: "carol", Account: "research", Partition: "gpu"}, true
	case "918004":
		return collector.JobMeta{User: "dave", Account: "trading", Partition: "roce"}, true
	}
	return collector.JobMeta{}, false
}

// ── Real sources ───────────────────────────────────────────────────────────

// squeueMeta caches job metadata, refreshed on an interval rather than per
// scrape so a Prometheus scrape never blocks on the controller.
type squeueMeta struct {
	mu   sync.RWMutex
	jobs map[string]collector.JobMeta
}

func (s *squeueMeta) Meta(jobID string) (collector.JobMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.jobs[jobID]
	return m, ok
}

func (s *squeueMeta) refresh() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, "squeue", "--noheader", "--states=RUNNING",
		"--Format=JobID:|,UserName:|,Account:|,Partition:").Output()
	if err != nil {
		return err
	}

	jobs := map[string]collector.JobMeta{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Split(line, "|")
		if len(f) < 4 {
			continue
		}
		for i := range f {
			f[i] = strings.TrimSpace(f[i])
		}
		jobs[f[0]] = collector.JobMeta{User: f[1], Account: f[2], Partition: f[3]}
	}

	s.mu.Lock()
	s.jobs = jobs
	s.mu.Unlock()
	return nil
}

func (s *squeueMeta) loop(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for range t.C {
		_ = s.refresh()
	}
}

// scontrolResolver maps a SLUID to a job id via the controller.
//
// Slurm 26.05 keys cgroup directories by SLUID, and only the controller knows
// the mapping. Unresolved entries are counted rather than guessed at.
type scontrolResolver struct{}

func (scontrolResolver) ResolveSLUID(sluid string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, "scontrol", "show", "job", sluid, "--oneliner").Output()
	if err != nil {
		return "", err
	}
	for _, field := range strings.Fields(string(out)) {
		if id, ok := strings.CutPrefix(field, "JobId="); ok {
			return id, nil
		}
	}
	return "", fmt.Errorf("no JobId in scontrol output for %q", sluid)
}
