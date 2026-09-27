// Command ib-slurm-exporter exports InfiniBand and RoCE counters correlated
// with the Slurm job that owns them.
//
// Runs per compute node. Reads local sysfs, /proc and cgroups on every
// sample; job metadata comes from a background, node-scoped squeue call that
// never runs on the scrape path, and can be switched off.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/cgroup"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/collector"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/fixture"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/ib"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/procfd"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/rdmares"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/slurm"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "ib-slurm-exporter: %v\n", err)
		os.Exit(1)
	}
}

type options struct {
	listen         string
	sysRoot        string
	verbsRoot      string
	procRoot       string
	cgroupRoot     string
	slurmScope     string
	ownership      string
	rdmaPath       string
	squeuePath     string
	squeueNodes    string
	squeueInterval time.Duration
	sampleInterval time.Duration
	noUserLabels   bool
	maxRequests    int
	demo           bool
	demoLayout     string
	once           bool
	showVersion    bool
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("ib-slurm-exporter", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.listen, "listen", ":9836", "metrics listen address; bind to a management interface where possible (the port serves job and user data unauthenticated)")
	fs.StringVar(&o.sysRoot, "sys-root", "/sys/class/infiniband", "infiniband sysfs root; --ownership=qp also reads IPoIB interfaces from its sibling net/ directory (/sys/class/net)")
	fs.StringVar(&o.verbsRoot, "verbs-root", "/sys/class/infiniband_verbs", "uverbs sysfs root")
	fs.StringVar(&o.procRoot, "proc-root", "/proc", "proc root")
	fs.StringVar(&o.cgroupRoot, "cgroup-root", "/sys/fs/cgroup", "cgroup root")
	fs.StringVar(&o.slurmScope, "slurm-scope", "", "slurmstepd scope directory relative to --cgroup-root (e.g. system.slice/slurmstepd.scope); empty = discover <slice>/*slurmstepd.scope")
	fs.StringVar(&o.ownership, "ownership", "fd", "how device users are found: fd (open uverbs descriptors, per HCA) | qp (RDMA resource tracking via iproute2 rdma, per port; sees kernel QPs created through ib_core and counts IPoIB interfaces, but not mlx5 DEVX QPs)")
	fs.StringVar(&o.rdmaPath, "rdma-path", "/usr/sbin/rdma", "absolute path to iproute2's rdma tool (--ownership=qp)")
	fs.StringVar(&o.squeuePath, "squeue-path", "/usr/bin/squeue", "absolute path to squeue; empty disables job metadata and SLUID lookups through the controller")
	fs.StringVar(&o.squeueNodes, "squeue-nodelist", "localhost", "value for squeue --nodelist; set to the Slurm NodeName if it differs from this host's name")
	fs.DurationVar(&o.squeueInterval, "squeue-interval", 60*time.Second, fmt.Sprintf("mean interval between squeue refreshes (±20%% jitter); at least %v (to switch squeue off, set --squeue-path=)", slurm.MinInterval))
	fs.DurationVar(&o.sampleInterval, "sample-interval", 15*time.Second, "background sampling interval for job attribution; 0 samples on scrape only")
	fs.BoolVar(&o.noUserLabels, "no-user-labels", false, "leave user/account/partition labels empty and drop ib_slurm_job_info")
	fs.IntVar(&o.maxRequests, "web.max-requests", 2, "maximum concurrent /metrics requests; further requests get 503")
	fs.BoolVar(&o.demo, "demo", false, "serve a synthetic node; needs no InfiniBand hardware")
	fs.StringVar(&o.demoLayout, "demo-layout", "v2", "demo cgroup layout: v1 | v2 | v2-sluid")
	fs.BoolVar(&o.once, "once", false, "print the ib_slurm_* metrics once in Prometheus text format and exit")
	fs.BoolVar(&o.showVersion, "version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	switch o.ownership {
	case "fd", "qp":
	default:
		return o, fmt.Errorf("--ownership must be fd or qp, got %q", o.ownership)
	}
	switch fixture.Layout(o.demoLayout) {
	case fixture.V1, fixture.V2, fixture.V2SLUID:
	default:
		return o, fmt.Errorf("--demo-layout must be v1, v2 or v2-sluid, got %q", o.demoLayout)
	}
	if o.squeuePath != "" && !filepath.IsAbs(o.squeuePath) {
		return o, fmt.Errorf("--squeue-path must be absolute (the exporter usually runs as root; a PATH lookup would run whatever is first on PATH), got %q", o.squeuePath)
	}
	// Every node runs an exporter, so a tiny interval is a slurmctld RPC
	// flood, and 0 in particular looks like "off" next to --sample-interval=0.
	if o.squeueInterval < slurm.MinInterval {
		return o, fmt.Errorf("--squeue-interval must be at least %v, got %v (to switch squeue off, set --squeue-path=)", slurm.MinInterval, o.squeueInterval)
	}
	if o.ownership == "qp" && !o.demo && !filepath.IsAbs(o.rdmaPath) {
		return o, fmt.Errorf("--rdma-path must be absolute, got %q", o.rdmaPath)
	}
	return o, nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	o, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if o.showVersion {
		fmt.Fprintln(stdout, version)
		return nil
	}

	// Logs go to stderr so --once output on stdout stays valid exposition.
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	cfg := collector.Config{
		Ownership:    collector.Ownership(o.ownership),
		NoUserLabels: o.noUserLabels,
		Logger:       logger,
	}
	reg := prometheus.NewRegistry()

	var squeue *slurm.Squeue
	var advance func() // demo only: move the synthetic counters

	if o.demo {
		dir, err := os.MkdirTemp("", "ib-slurm-demo-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)

		spec := fixture.Default(fixture.Layout(o.demoLayout))
		roots, err := fixture.Build(dir, spec)
		if err != nil {
			return fmt.Errorf("build demo fixture: %w", err)
		}
		o.sysRoot, o.verbsRoot = roots.Sys, roots.Verbs
		o.procRoot, o.cgroupRoot = roots.Proc, roots.Cgroup
		cfg.Resolver = demoResolver{}
		cfg.Meta = demoMeta{}
		if cfg.Ownership == collector.OwnershipQP {
			cfg.QPs = staticQPs(fixture.QPs(spec))
		}
		step := uint64(0)
		advance = func() {
			step++
			if err := fixture.Advance(roots, spec, 1); err != nil {
				logger.Warn("advance demo counters", "err", err)
			}
		}

		logger.Info("demo mode — synthetic node, no hardware required",
			"layout", o.demoLayout, "ownership", o.ownership, "root", dir)
		logger.Info("expect: 918001 attributed on mlx5_0; 918002+918003 suppressed " +
			"(they share mlx5_1); 918004 attributed on mlx5_2 (RoCE)")
	} else {
		if cfg.Ownership == collector.OwnershipQP {
			cfg.QPs = rdmares.Command{Path: o.rdmaPath}
		}
		if o.squeuePath != "" {
			squeue = slurm.New(o.squeuePath, o.squeueNodes, logger)
			cfg.Meta = squeue
			cfg.Resolver = squeue
			reg.MustRegister(squeue)
		} else {
			logger.Info("squeue disabled: series carry job_id only; SLUIDs resolve from slurmstepd titles only")
		}
	}

	scanner := cgroup.NewScanner(o.cgroupRoot)
	scanner.Scope = o.slurmScope
	cfg.Scanner = scanner
	cfg.Reader = ib.NewReader(o.sysRoot)
	cfg.Mapper = procfd.NewMapper(o.procRoot, o.verbsRoot)
	c := collector.New(cfg)
	reg.MustRegister(c)

	if o.once {
		if squeue != nil {
			if err := squeue.Refresh(ctx); err != nil {
				logger.Warn("squeue unavailable; series will carry job_id only", "err", err)
			}
		}
		if advance != nil {
			// Job series count increases seen while a job is a port's sole
			// user, so a single sample would show 0 everywhere. In the demo,
			// take a baseline, move the synthetic counters once, then report.
			c.Sample(ctx)
			advance()
		}
		return writeOnce(stdout, reg)
	}

	reg.MustRegister(collectors.NewGoCollector())

	if squeue != nil {
		go squeue.Run(ctx, o.squeueInterval)
	}
	if o.sampleInterval > 0 {
		go c.Run(ctx, o.sampleInterval)
	}
	if advance != nil {
		go func() {
			t := time.NewTicker(5 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					advance()
				}
			}
		}()
	}

	srv := &http.Server{
		Addr:              o.listen,
		Handler:           newMux(reg, o.maxRequests, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", o.listen, "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	select {
	case err := <-errc:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutCtx)
}

func newMux(reg *prometheus.Registry, maxRequests int, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		// Each scrape walks /proc and cgroups under one lock; bounding
		// concurrency keeps an unauthenticated client from queueing work.
		MaxRequestsInFlight: maxRequests,
		Timeout:             30 * time.Second,
		ErrorLog:            slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	return mux
}

// writeOnce renders the ib_slurm_* families in the Prometheus text format,
// with HELP and TYPE lines. The Go runtime metrics are left out so the output
// can go into node_exporter's textfile directory without colliding with
// node_exporter's own go_* series.
func writeOnce(w io.Writer, reg *prometheus.Registry) error {
	families, err := reg.Gather()
	if err != nil {
		return err
	}
	enc := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "ib_slurm_") {
			continue
		}
		if err := enc.Encode(f); err != nil {
			return err
		}
	}
	return nil
}

// ── Demo sources ───────────────────────────────────────────────────────────

// demoResolver stands in for the squeue SLUID cache.
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

type staticQPs []rdmares.QP

func (s staticQPs) QPs(context.Context) ([]rdmares.QP, error) { return s, nil }
