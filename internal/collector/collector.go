// Package collector joins Slurm cgroups to InfiniBand counters and exposes the
// result as Prometheus metrics.
//
// # The attribution problem
//
// PMA port counters (bytes, packets, link errors) are per port. The HCA
// counts packets; it does not record which process sent them. So when two
// jobs share a node and both use mlx5_0, there is no way to split
// port_xmit_data between them from these counters.
//
// Exporters that ignore this produce job-labelled series that are simply wrong,
// and wrong in the worst way: a busy neighbour's retries get attributed to a
// healthy job, so the metric is most misleading exactly when someone is using
// it to debug an incident.
//
// This exporter therefore emits two distinct metric families:
//
//	ib_slurm_job_*      Attributed to a job. Only emitted for a port on which
//	                    that job is the only user the exporter can see.
//	ib_slurm_device_*   Always emitted, never job-labelled. The ground truth.
//
// # What "only user" means, precisely
//
// It depends on the ownership mode, and neither mode is perfect:
//
//	fd (default)  A user is a Slurm job with a process holding an open
//	              uverbs descriptor on the HCA. Descriptors carry no port,
//	              so a job is treated as using every port of that HCA.
//	              Kernel RDMA consumers (IPoIB, NFS/RDMA, Lustre o2ib) and
//	              non-Slurm processes are invisible, so their traffic can
//	              land on a job that looks sole. NCCL opens every active HCA
//	              (src/transport/net_ib/init.cc; UCX not checked), so on
//	              shared nodes this mode effectively needs node-exclusive
//	              jobs to attribute anything.
//	qp            A user is the owner of a non-management queue pair on the
//	              port, from the kernel's RDMA resource tracking. Kernel-
//	              owned QPs and QPs owned by processes outside any job count
//	              as non-job users and suppress attribution. Resource
//	              tracking only lists QPs created through ib_core (verbs and
//	              rdma_cm users: NFS/RDMA, Lustre o2ib, iSER, non-enhanced
//	              IPoIB). Two kinds bypass it. mlx5 enhanced IPoIB creates
//	              its QP with a raw firmware command, so every IPoIB
//	              interface not "down" is counted from sysfs instead, as a
//	              non-job user of its port (package ib). DEVX QPs, created
//	              by userspace through mlx5 firmware commands, are not
//	              detected at all: a process using them looks like no user,
//	              and its traffic can land on a job that looks sole. UCX's
//	              default configuration asks for DEVX QPs on mlx5. Needs
//	              iproute2's rdma tool and has not been run on real hardware
//	              from this repository.
//
// # Fail safe
//
// Anything that makes the user set incomplete — a /proc/<pid>/fd that cannot
// be read, a cgroup subtree that cannot be read, a failed QP listing or
// IPoIB scan — suppresses all job attribution for that sample and is
// reported. An incomplete user set can make a shared port look exclusive,
// and a metric on the wrong job is worse than a metric on no job. (The DEVX
// gap above is the known exception: nothing reports it.)
//
// # Job counters are accumulated, not copied
//
// A job series is not the port's lifetime counter. It starts at 0 when the
// job is first seen as a port's sole user and only adds increases observed
// between two consecutive samples in which the job was sole user. So a
// neighbour's traffic during a shared period never lands in the job's
// series, even across the gap when attribution comes back, and a new job
// does not inherit earlier jobs' errors. Samples are taken on every scrape
// and on a background ticker (Run). The remaining blind spot: a neighbour
// that starts and finishes entirely between two samples cannot be seen.
//
// A job's accumulators are dropped when a clean cgroup scan no longer finds
// it. A scan that failed is not evidence that a job ended, so it prunes
// nothing, unless the job has been missing for longer than staleAfter
// (which bounds memory if an error persists).
package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/cgroup"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/ib"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/procfd"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/rdmares"
)

// JobMeta is the descriptive detail squeue knows and cgroups do not.
type JobMeta struct {
	User      string
	Account   string
	Partition string
}

// MetaSource supplies job metadata. Optional: without it the exporter still
// works, labelling series with job ID alone.
type MetaSource interface {
	Meta(jobID string) (JobMeta, bool)
}

// Ownership selects how device users are determined.
type Ownership string

const (
	OwnershipFD Ownership = "fd"
	OwnershipQP Ownership = "qp"
)

// Config wires a Collector.
type Config struct {
	Scanner *cgroup.Scanner
	Reader  *ib.Reader
	Mapper  *procfd.Mapper
	// Meta is optional.
	Meta MetaSource
	// Resolver is optional. It must answer from memory (see cgroup.Resolver).
	Resolver cgroup.Resolver
	// Ownership defaults to OwnershipFD.
	Ownership Ownership
	// QPs is required for OwnershipQP.
	QPs rdmares.Source
	// NoUserLabels leaves user, account and partition empty and drops
	// ib_slurm_job_info, for sites that do not want who-runs-what on an
	// unauthenticated port.
	NoUserLabels bool
	Logger       *slog.Logger
}

const ns = "ib_slurm"

type Collector struct {
	cfg Config

	mu sync.Mutex
	// sluidCache maps SLUID → job ID. A SLUID's job ID never changes, so an
	// entry lives until a clean scan no longer finds the SLUID's cgroup
	// directory (or it has been missing for staleAfter).
	sluidCache map[string]sluidEntry
	acc        map[accKey]*accState
	jobSeen    map[string]time.Time // job ID → last sample that found it
	gen        uint64
	logs       map[string]*logState
	lastScopes *string // scopes logged last, nil before the first sample
	now        func() time.Time

	// Descriptors
	jobCounter    *prometheus.Desc
	jobAttributed *prometheus.Desc
	jobInfo       *prometheus.Desc
	devCounter    *prometheus.Desc
	devSaturated  *prometheus.Desc
	devJobs       *prometheus.Desc
	devNonJob     *prometheus.Desc
	devState      *prometheus.Desc
	jobPIDs       *prometheus.Desc
	unattributed  *prometheus.Desc
	unresolved    *prometheus.Desc
	pidUnreadable *prometheus.Desc
	layoutInfo    *prometheus.Desc
	scopeFound    *prometheus.Desc
	scrapeError   *prometheus.Desc
}

// New returns a Collector. Scanner, Reader and Mapper are required.
func New(cfg Config) *Collector {
	if cfg.Ownership == "" {
		cfg.Ownership = OwnershipFD
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	jobLabels := []string{"job_id", "user", "account", "partition", "device", "port", "counter"}
	portLabels := []string{"device", "port"}
	devLabels := []string{"device", "port", "counter"}

	return &Collector{
		cfg:        cfg,
		sluidCache: map[string]sluidEntry{},
		acc:        map[accKey]*accState{},
		jobSeen:    map[string]time.Time{},
		logs:       map[string]*logState{},
		now:        time.Now,

		jobCounter: prometheus.NewDesc(ns+"_job_counter_total",
			"Increase of an InfiniBand counter observed while the job was the port's only visible user, "+
				"accumulated from 0 when the exporter first saw it so. Present only while the job is that sole user. "+
				"Same units as ib_slurm_device_counter_total.",
			jobLabels, nil),
		jobAttributed: prometheus.NewDesc(ns+"_job_device_attributed",
			"1 when the job's counters on this port are being attributed to it, 0 when the job uses the port but attribution is suppressed.",
			[]string{"job_id", "device", "port"}, nil),
		jobInfo: prometheus.NewDesc(ns+"_job_info",
			"Always 1. Job metadata from squeue, for joining with group_left.",
			[]string{"job_id", "user", "account", "partition"}, nil),
		devCounter: prometheus.NewDesc(ns+"_device_counter_total",
			"Raw InfiniBand/RoCE counter for a port, unattributed. Always present when readable. "+
				"port_xmit_data and port_rcv_data are in 4-octet units (multiply by 4 for bytes); others count events or packets.",
			devLabels, nil),
		devSaturated: prometheus.NewDesc(ns+"_device_counter_saturated",
			"1 when a fixed-width PMA counter is at its maximum (2^width-1) and further events are not visible to rate(). "+
				"Only emitted for counters whose width is known.",
			devLabels, nil),
		devJobs: prometheus.NewDesc(ns+"_device_jobs",
			"Slurm jobs (including unresolved SLUID allocations) using this port. 0 is a real zero; above 1 means per-job attribution is suppressed.",
			portLabels, nil),
		devNonJob: prometheus.NewDesc(ns+"_device_nonjob_users",
			"Non-job users of this port (qp ownership mode only): queue pairs owned by the kernel or by processes outside any Slurm job, "+
				"plus IPoIB interfaces on the port that are not down (their QPs can bypass RDMA resource tracking). Above 0 suppresses attribution.",
			portLabels, nil),
		devState: prometheus.NewDesc(ns+"_device_port_up",
			"1 when the port state is ACTIVE, 0 otherwise.",
			[]string{"device", "port", "link_layer", "rate"}, nil),
		jobPIDs: prometheus.NewDesc(ns+"_job_processes",
			"Processes found in the job's cgroup.",
			[]string{"job_id", "user", "account", "partition"}, nil),
		unattributed: prometheus.NewDesc(ns+"_unattributed_jobs",
			"Jobs using RDMA (or whose RDMA use could not be determined) that are attributed on no port.",
			nil, nil),
		unresolved: prometheus.NewDesc(ns+"_unresolved_sluid_allocations",
			"Cgroups keyed by SLUID (Slurm 26.05+) that could not be mapped to a job ID. They still count as device users.",
			nil, nil),
		pidUnreadable: prometheus.NewDesc(ns+"_pid_fd_unreadable",
			"Job processes whose /proc/<pid>/fd could not be read (fd ownership mode). Above 0 suppresses all job attribution.",
			nil, nil),
		layoutInfo: prometheus.NewDesc(ns+"_cgroup_layout_info",
			"Always 1. layout is the detected cgroup layout; scope is the Slurm cgroup root found, relative to --cgroup-root.",
			[]string{"layout", "scope"}, nil),
		scopeFound: prometheus.NewDesc(ns+"_cgroup_scope_found",
			"1 when a Slurm cgroup root (v2 slurmstepd scope or v1 <controller>/slurm) was found, even if it holds no jobs.",
			nil, nil),
		scrapeError: prometheus.NewDesc(ns+"_scrape_error",
			"1 when the last sample could not read this source (sysfs, cgroup, procfd, rdma).",
			[]string{"source"}, nil),
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.jobCounter, c.jobAttributed, c.jobInfo, c.devCounter, c.devSaturated,
		c.devJobs, c.devNonJob, c.devState, c.jobPIDs, c.unattributed,
		c.unresolved, c.pidUnreadable, c.layoutInfo, c.scopeFound, c.scrapeError,
	} {
		ch <- d
	}
}

// Run samples every interval until ctx ends, so job accumulators see
// ownership changes between scrapes.
func (c *Collector) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.Sample(ctx)
		}
	}
}

// Sample takes one sample without emitting anything, advancing the job
// accumulators. Run calls it on a ticker; --once uses it to set a baseline.
func (c *Collector) Sample(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sample(ctx)
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.sample(context.Background())
	c.emit(ch, s)
}

// ── Sampling ───────────────────────────────────────────────────────────────

type portKey struct {
	dev  string
	port int
}

// owner is a user of devices: a resolved job, or an unresolved SLUID
// allocation (which cannot be labelled but still makes a port shared).
type owner struct {
	key      string // job ID, or "sluid:<id>"
	jobID    string // empty when unresolved
	pids     []int
	paths    []string
	partial  bool             // cgroup subtree partly unreadable
	unknown  bool             // device use could not be fully determined
	usesDev  bool             // holds at least one HCA (fd mode)
	ports    map[portKey]bool // ports this owner uses
	attrib   map[portKey]bool // job only: sole user this sample
	pidCount int              // for ib_slurm_job_processes
}

type sample struct {
	ports     []ib.Counters
	byKey     map[portKey]ib.Counters
	scan      cgroup.Result
	owners    []*owner // sorted by key
	users     map[portKey]map[string]bool
	nonJob    map[portKey]int
	errs      map[string]error // by source
	unresolv  int
	unreadPID int
	gap       bool // user set incomplete: attribute nothing
	now       time.Time
}

// staleAfter bounds how long the exporter remembers a job (or SLUID) that
// only failed cgroup scans have missed. A clean scan that misses a job prunes
// it at once; a failed scan says nothing about which jobs ended, so it prunes
// nothing younger than this. Without the bound, a cgroup error that never
// clears would keep every ended job's accumulators forever.
const staleAfter = 10 * time.Minute

func (c *Collector) sample(ctx context.Context) *sample {
	s := &sample{
		byKey:  map[portKey]ib.Counters{},
		users:  map[portKey]map[string]bool{},
		nonJob: map[portKey]int{},
		errs:   map[string]error{},
		now:    c.now(),
	}
	c.gen++

	ports, err := c.cfg.Reader.ReadAll()
	if err != nil {
		s.errs["sysfs"] = err
	}
	s.ports = ports
	portsByDev := map[string][]int{}
	for _, p := range ports {
		k := portKey{p.Device, p.Port}
		s.byKey[k] = p
		portsByDev[p.Device] = append(portsByDev[p.Device], p.Port)
	}

	scan, err := c.cfg.Scanner.Scan()
	if err != nil {
		s.errs["cgroup"] = err
	}
	s.scan = scan
	c.logScopes(scan)

	s.owners = c.resolveOwners(scan.Allocations, s.errs["cgroup"] == nil, s.now)
	for _, o := range s.owners {
		if o.jobID == "" {
			s.unresolv++
		}
	}

	switch c.cfg.Ownership {
	case OwnershipQP:
		c.qpOwnership(ctx, s, portsByDev)
	default:
		c.fdOwnership(s, portsByDev)
	}

	// An unreadable sysfs means there are no counters to attribute; an
	// unreadable cgroup subtree means a job's processes may be missing.
	if s.errs["sysfs"] != nil || s.errs["cgroup"] != nil {
		s.gap = true
	}

	for _, o := range s.owners {
		if o.jobID == "" {
			continue
		}
		o.attrib = map[portKey]bool{}
		for pk := range o.ports {
			o.attrib[pk] = !s.gap && len(s.users[pk]) == 1 && s.nonJob[pk] == 0
		}
	}

	c.accumulate(s)
	c.logErrors(s)
	return s
}

// sluidEntry is a resolved SLUID and when a scan last found its cgroup.
type sluidEntry struct {
	jobID string
	seen  time.Time
}

// resolveOwners maps allocations to owners, resolving SLUIDs from the cache,
// the slurmstepd process title (local), and the resolver (squeue cache), in
// that order. Allocations resolving to the same job are merged. cleanScan
// reports that the cgroup scan had no error, so a SLUID it did not find has
// really gone.
func (c *Collector) resolveOwners(allocs []cgroup.Allocation, cleanScan bool, now time.Time) []*owner {
	present := map[string]bool{}
	byKey := map[string]*owner{}

	for _, a := range allocs {
		jobID := a.JobID
		if a.IsSLUID {
			present[a.Identifier] = true
			jobID = c.resolveSLUID(a, now)
		}
		key := jobID
		if key == "" {
			key = "sluid:" + a.Identifier
		}
		o := byKey[key]
		if o == nil {
			o = &owner{key: key, jobID: jobID, ports: map[portKey]bool{}}
			byKey[key] = o
		}
		o.pids = append(o.pids, a.PIDs...)
		o.paths = append(o.paths, a.Path)
		o.partial = o.partial || a.Incomplete
	}

	// A failed scan may have missed a live job's directory, and dropping its
	// SLUID would send it back through resolution (and the squeue cache)
	// for nothing.
	for sluid, e := range c.sluidCache {
		if !present[sluid] && (cleanScan || now.Sub(e.seen) > staleAfter) {
			delete(c.sluidCache, sluid)
		}
	}

	out := make([]*owner, 0, len(byKey))
	for _, o := range byKey {
		o.pids = dedupe(o.pids)
		o.pidCount = len(o.pids)
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

func (c *Collector) resolveSLUID(a cgroup.Allocation, now time.Time) string {
	if e, ok := c.sluidCache[a.Identifier]; ok {
		e.seen = now
		c.sluidCache[a.Identifier] = e
		return e.jobID
	}
	titleID, titleOK := c.cfg.Mapper.SlurmstepdJobID(a.StepdPIDs)
	var rid string
	if c.cfg.Resolver != nil {
		if id, err := c.cfg.Resolver.ResolveSLUID(a.Identifier); err == nil && id != "" {
			rid = id
		}
	}
	var id string
	switch {
	case titleOK && rid != "" && titleID != rid:
		c.logOnce("sluid-conflict:"+a.Identifier, titleID+"/"+rid, slog.LevelWarn,
			"slurmstepd title and squeue disagree about a SLUID; leaving it unresolved",
			"sluid", a.Identifier, "stepd_job", titleID, "squeue_job", rid)
		return ""
	case titleOK:
		id = titleID
	case rid != "":
		id = rid
	default:
		return ""
	}
	c.sluidCache[a.Identifier] = sluidEntry{jobID: id, seen: now}
	return id
}

// fdOwnership: users are owners with an open uverbs descriptor on a device,
// applied to every port of that device.
func (c *Collector) fdOwnership(s *sample, portsByDev map[string][]int) {
	var firstErr error
	for _, o := range s.owners {
		devs, st := c.cfg.Mapper.DevicesForPIDs(o.pids)
		unread := st.Unreadable
		if st.FirstErr != nil && firstErr == nil {
			firstErr = st.FirstErr
		}
		if len(st.Gone) > 0 {
			// A PID still listed in the cgroup but absent from /proc is
			// hidden (hidepid, another PID namespace), not exited.
			hidden, err := stillListed(o.paths, st.Gone)
			if err != nil {
				unread += len(st.Gone)
				if firstErr == nil {
					firstErr = err
				}
			} else if hidden > 0 {
				unread += hidden
				if firstErr == nil {
					firstErr = fmt.Errorf("%d job PIDs are listed in cgroup.procs but not visible in /proc (hidepid mount or different PID namespace?)", hidden)
				}
			}
		}
		o.unknown = unread > 0 || o.partial
		o.usesDev = len(devs) > 0
		s.unreadPID += unread
		for _, d := range devs {
			for _, p := range portsByDev[d] {
				pk := portKey{d, p}
				o.ports[pk] = true
				addUser(s.users, pk, o.key)
			}
		}
		if o.unknown {
			s.gap = true
		}
	}
	if s.unreadPID > 0 {
		s.errs["procfd"] = firstErr
	}
}

// qpOwnership: users are owners of non-management QPs on each port.
func (c *Collector) qpOwnership(ctx context.Context, s *sample, portsByDev map[string][]int) {
	pidOwner := map[int]*owner{}
	for _, o := range s.owners {
		for _, p := range o.pids {
			pidOwner[p] = o
		}
	}
	if c.cfg.QPs == nil {
		s.errs["rdma"] = errors.New("qp ownership mode without a QP source")
		s.gap = true
		return
	}
	qps, err := c.cfg.QPs.QPs(ctx)
	if err != nil {
		s.errs["rdma"] = err
		s.gap = true
		return
	}
	for _, q := range qps {
		if q.IsManagement() {
			continue
		}
		var pks []portKey
		if q.Port == 0 {
			// Not bound to a port yet: could end up on any of them.
			for _, p := range portsByDev[q.Device] {
				pks = append(pks, portKey{q.Device, p})
			}
		} else if _, ok := s.byKey[portKey{q.Device, q.Port}]; ok {
			pks = []portKey{{q.Device, q.Port}}
		}
		var o *owner
		if !q.Kernel {
			o = pidOwner[q.PID]
			if o == nil {
				if tgid, ok := c.cfg.Mapper.TGID(q.PID); ok {
					o = pidOwner[tgid]
				}
			}
		}
		for _, pk := range pks {
			if o == nil {
				s.nonJob[pk]++
				continue
			}
			o.ports[pk] = true
			addUser(s.users, pk, o.key)
		}
	}

	// mlx5 enhanced IPoIB creates its QP outside ib_core, so the listing
	// above cannot show it (see package ib). Count the interface instead.
	// A non-enhanced IPoIB interface is then counted twice, once as its
	// kernel QP and once here; only "above 0" matters.
	ifs, err := c.cfg.Reader.IPoIB()
	if err != nil {
		s.errs["rdma"] = fmt.Errorf("IPoIB interfaces: %w", err)
		s.gap = true
		return
	}
	for _, i := range ifs {
		if !i.MayCarryTraffic() {
			continue
		}
		for _, pk := range ipoibPorts(i, s.ports) {
			s.nonJob[pk]++
		}
	}
}

// ipoibPorts is the ports an IPoIB interface may be on: its own, or, when
// that is unknown, every candidate port (fail safe).
func ipoibPorts(i ib.IPoIBInterface, ports []ib.Counters) []portKey {
	var out []portKey
	for _, p := range ports {
		switch {
		case i.Device == "":
			// Not tied to an HCA: any port that is not Ethernet (RoCE).
			if !strings.EqualFold(p.LinkLayer, "Ethernet") {
				out = append(out, portKey{p.Device, p.Port})
			}
		case p.Device == i.Device && (i.Port == 0 || p.Port == i.Port):
			out = append(out, portKey{p.Device, p.Port})
		}
	}
	return out
}

func addUser(users map[portKey]map[string]bool, pk portKey, key string) {
	if users[pk] == nil {
		users[pk] = map[string]bool{}
	}
	users[pk][key] = true
}

// stillListed counts how many of pids are still in the cgroups at paths.
func stillListed(paths []string, pids []int) (int, error) {
	current := map[int]bool{}
	for _, p := range paths {
		ps, err := cgroup.ReadPIDs(p)
		if err != nil {
			return 0, err
		}
		for _, pid := range ps {
			current[pid] = true
		}
	}
	n := 0
	for _, pid := range pids {
		if current[pid] {
			n++
		}
	}
	return n, nil
}

// ── Accumulation ───────────────────────────────────────────────────────────

type accKey struct {
	job     string
	dev     string
	port    int
	counter string
}

type accState struct {
	value   uint64 // attributed increase so far
	last    uint64 // raw device value at the previous sample
	sole    bool   // job was sole user at the previous sample
	present bool   // counter was read at the previous sample
	gen     uint64
}

// accumulate adds, per (job, port, counter), the device's increase between
// two consecutive samples in which the job was the sole user. A decrease is
// treated the way Prometheus' rate() treats one — as a reset, adding the new
// raw value. Whatever the device counted between the previous sample and
// the reset (a subnet-manager clear of a PMA counter, or a 32-bit
// hw_counter wrap) is lost, so this can under-count; it never over-counts.
func (c *Collector) accumulate(s *sample) {
	liveJobs := map[string]bool{}
	for _, o := range s.owners {
		if o.jobID == "" {
			continue
		}
		liveJobs[o.jobID] = true
		c.jobSeen[o.jobID] = s.now
		for pk, sole := range o.attrib {
			p, ok := s.byKey[pk]
			if !ok {
				continue
			}
			for name, v := range p.Values {
				k := accKey{o.jobID, pk.dev, pk.port, name}
				st := c.acc[k]
				if st == nil {
					c.acc[k] = &accState{last: v, sole: sole, present: true, gen: c.gen}
					continue
				}
				if sole && st.sole && st.present {
					if v >= st.last {
						st.value += v - st.last
					} else {
						st.value += v
					}
				}
				st.last, st.sole, st.present, st.gen = v, sole, true, c.gen
			}
		}
	}
	// A job missing from a failed cgroup scan may still be running; dropping
	// its accumulators would restart its series at 0 mid-job.
	gone := func(job string) bool {
		return !liveJobs[job] && (s.errs["cgroup"] == nil || s.now.Sub(c.jobSeen[job]) > staleAfter)
	}
	for k, st := range c.acc {
		if st.gen == c.gen {
			continue
		}
		if gone(k.job) {
			delete(c.acc, k) // job gone: its series is gone too
			continue
		}
		// Not observed this sample (job left the port, counter vanished,
		// sysfs or cgroups unreadable): the next observation is a fresh
		// baseline.
		st.sole, st.present = false, false
	}
	for job := range c.jobSeen {
		if gone(job) {
			delete(c.jobSeen, job)
		}
	}
}

// ── Emission ───────────────────────────────────────────────────────────────

func (c *Collector) emit(ch chan<- prometheus.Metric, s *sample) {
	sources := []string{"sysfs", "cgroup"}
	if c.cfg.Ownership == OwnershipQP {
		sources = append(sources, "rdma")
	} else {
		sources = append(sources, "procfd")
	}
	for _, src := range sources {
		v := 0.0
		if s.errs[src] != nil {
			v = 1
		}
		ch <- prometheus.MustNewConstMetric(c.scrapeError, prometheus.GaugeValue, v, src)
	}

	scopeFound := 0.0
	if s.scan.ScopeFound {
		scopeFound = 1
	}
	ch <- prometheus.MustNewConstMetric(c.scopeFound, prometheus.GaugeValue, scopeFound)
	layout := s.scan.Layout
	if layout == "" {
		layout = cgroup.LayoutUnknown
	}
	if len(s.scan.Scopes) == 0 {
		ch <- prometheus.MustNewConstMetric(c.layoutInfo, prometheus.GaugeValue, 1, string(layout), "")
	}
	for _, sc := range s.scan.Scopes {
		ch <- prometheus.MustNewConstMetric(c.layoutInfo, prometheus.GaugeValue, 1, string(layout), sc)
	}

	// Device-level series are unconditional — they are the ground truth and
	// stay correct no matter how many jobs share a node.
	for _, p := range s.ports {
		port := strconv.Itoa(p.Port)
		pk := portKey{p.Device, p.Port}
		up := 0.0
		if p.IsActive() {
			up = 1
		}
		ch <- prometheus.MustNewConstMetric(c.devState, prometheus.GaugeValue, up,
			p.Device, port, p.LinkLayer, p.Rate)

		for name, v := range p.Values {
			ch <- prometheus.MustNewConstMetric(c.devCounter, prometheus.CounterValue,
				float64(v), p.Device, port, name)
			if sat, known := ib.Saturated(name, v); known {
				f := 0.0
				if sat {
					f = 1
				}
				ch <- prometheus.MustNewConstMetric(c.devSaturated, prometheus.GaugeValue,
					f, p.Device, port, name)
			}
		}

		// Every port read gets a device_jobs series, so "0 jobs" is a real
		// zero and not an absent series.
		ch <- prometheus.MustNewConstMetric(c.devJobs, prometheus.GaugeValue,
			float64(len(s.users[pk])), p.Device, port)
		if c.cfg.Ownership == OwnershipQP && s.errs["rdma"] == nil {
			ch <- prometheus.MustNewConstMetric(c.devNonJob, prometheus.GaugeValue,
				float64(s.nonJob[pk]), p.Device, port)
		}
	}

	ch <- prometheus.MustNewConstMetric(c.unresolved, prometheus.GaugeValue, float64(s.unresolv))
	if c.cfg.Ownership != OwnershipQP {
		ch <- prometheus.MustNewConstMetric(c.pidUnreadable, prometheus.GaugeValue, float64(s.unreadPID))
	}

	unattributed := 0
	for _, o := range s.owners {
		if o.jobID == "" {
			continue
		}
		meta := JobMeta{}
		if c.cfg.Meta != nil && !c.cfg.NoUserLabels {
			if m, ok := c.cfg.Meta.Meta(o.jobID); ok {
				meta = m
				ch <- prometheus.MustNewConstMetric(c.jobInfo, prometheus.GaugeValue, 1,
					o.jobID, meta.User, meta.Account, meta.Partition)
			}
		}

		ch <- prometheus.MustNewConstMetric(c.jobPIDs, prometheus.GaugeValue,
			float64(o.pidCount), o.jobID, meta.User, meta.Account, meta.Partition)

		attributedAny := false
		for pk, sole := range o.attrib {
			port := strconv.Itoa(pk.port)
			v := 0.0
			if sole {
				v = 1
				attributedAny = true
			}
			ch <- prometheus.MustNewConstMetric(c.jobAttributed, prometheus.GaugeValue, v,
				o.jobID, pk.dev, port)
			if !sole {
				continue
			}
			for name := range s.byKey[pk].Values {
				st := c.acc[accKey{o.jobID, pk.dev, pk.port, name}]
				if st == nil {
					continue
				}
				ch <- prometheus.MustNewConstMetric(c.jobCounter, prometheus.CounterValue,
					float64(st.value),
					o.jobID, meta.User, meta.Account, meta.Partition,
					pk.dev, port, name)
			}
		}

		usesRDMA := len(o.ports) > 0 || o.usesDev || o.unknown ||
			(c.cfg.Ownership == OwnershipQP && s.errs["rdma"] != nil)
		if usesRDMA && !attributedAny {
			unattributed++
		}
	}
	ch <- prometheus.MustNewConstMetric(c.unattributed, prometheus.GaugeValue, float64(unattributed))
}

// ── Logging ────────────────────────────────────────────────────────────────

type logState struct {
	cause string // what the line was about, digits folded (see cause)
	last  time.Time
	seen  uint64 // generation last seen failing
}

// logErrors logs each failing source when it starts failing, when its cause
// changes, and at most every ten minutes while one cause persists — the
// background sampler would otherwise log the same line every few seconds.
func (c *Collector) logErrors(s *sample) {
	for _, src := range []string{"sysfs", "cgroup", "procfd", "rdma"} {
		key := "err:" + src
		err := s.errs[src]
		if err == nil {
			if _, was := c.logs[key]; was {
				delete(c.logs, key)
				c.cfg.Logger.Info("sample source recovered", "source", src)
			}
			continue
		}
		hint := "job attribution is suppressed while this persists"
		if src == "procfd" && procfd.IsPermission(err) {
			// /proc/<pid>/fd is mode 0500 (DIR("fd", S_IRUSR|S_IXUSR) in
			// fs/proc/base.c) and proc_fd_permission (fs/proc/fd.c) only
			// runs generic_permission, which lets CAP_DAC_READ_SEARCH list
			// another user's directory; readlink on an entry then needs
			// ptrace access (call_proc_get_link), i.e. CAP_SYS_PTRACE.
			hint = "run as root, or with both CAP_DAC_READ_SEARCH (to list other users' /proc/<pid>/fd) and CAP_SYS_PTRACE (to read the links); " + hint
		}
		c.logOnce(key, cause(err.Error()), slog.LevelError, "sample source failed",
			"source", src, "err", err.Error(), "hint", hint)
	}
}

// cause folds digit runs so that the same failure on a different PID, job or
// count reads as the same cause. Without it, process churn under one
// persistent problem (EACCES on every job's fds) would change the message
// every few samples and defeat the rate limit; a different problem
// (EACCES, then hidden PIDs, then an unresolvable uverbs node) still reads
// as a new cause and is logged at once.
func cause(msg string) string {
	var b strings.Builder
	inDigits := false
	for _, r := range msg {
		if r >= '0' && r <= '9' {
			if !inDigits {
				b.WriteByte('#')
			}
			inDigits = true
			continue
		}
		inDigits = false
		b.WriteRune(r)
	}
	return b.String()
}

// logScopes says where Slurm's cgroups were found whenever that changes, so
// "no jobs" can be told apart from "looking in the wrong place" in the log
// as well as in ib_slurm_cgroup_scope_found.
func (c *Collector) logScopes(scan cgroup.Result) {
	key := strings.Join(scan.Scopes, ",")
	if c.lastScopes != nil && *c.lastScopes == key {
		return
	}
	c.lastScopes = &key
	if key == "" {
		c.cfg.Logger.Warn("no Slurm cgroup root found; check --cgroup-root and --slurm-scope",
			"cgroup_root", c.cfg.Scanner.Root, "slurm_scope", c.cfg.Scanner.Scope)
		return
	}
	c.cfg.Logger.Info("Slurm cgroup root found", "scopes", key, "layout", string(scan.Layout))
}

// logOnce logs msg under key unless the same cause was logged under key in
// the last ten minutes.
func (c *Collector) logOnce(key, cause string, level slog.Level, msg string, args ...any) {
	now := c.now()
	st := c.logs[key]
	if st != nil && st.cause == cause && now.Sub(st.last) < 10*time.Minute {
		st.seen = c.gen
		return
	}
	c.logs[key] = &logState{cause: cause, last: now, seen: c.gen}
	c.cfg.Logger.Log(context.Background(), level, msg, args...)
	// Forget keys that have not recurred recently so the map stays small.
	for k, v := range c.logs {
		if c.gen-v.seen > 1000 {
			delete(c.logs, k)
		}
	}
}

func dedupe(xs []int) []int {
	if len(xs) < 2 {
		return xs
	}
	sort.Ints(xs)
	out := xs[:1]
	for _, x := range xs[1:] {
		if x != out[len(out)-1] {
			out = append(out, x)
		}
	}
	return out
}
