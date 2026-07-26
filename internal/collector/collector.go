// Package collector joins Slurm cgroups to InfiniBand counters and exposes the
// result as Prometheus metrics.
//
// # The attribution problem
//
// InfiniBand counters are per-device. The HCA counts packets; it does not know
// which process sent them. So when two jobs share a node and both use mlx5_0,
// there is no way to split port_xmit_data between them — the information does
// not exist in the hardware.
//
// Exporters that ignore this produce job-labelled series that are simply wrong,
// and wrong in the worst way: a busy neighbour's retries get attributed to a
// healthy job, so the metric is most misleading exactly when someone is using
// it to debug an incident.
//
// This exporter therefore emits two distinct metric families:
//
//	ib_slurm_job_*      Attributed to a job. Only emitted when that job is the
//	                    device's sole user, which the exporter verifies.
//	ib_slurm_device_*   Always emitted, never job-labelled. The ground truth.
//
// and a third series, ib_slurm_device_jobs, giving the number of jobs on each
// device so a dashboard can show why attribution is missing.
package collector

import (
	"log/slog"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/cgroup"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/ib"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/procfd"
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

const ns = "ib_slurm"

type Collector struct {
	scanner *cgroup.Scanner
	reader  *ib.Reader
	mapper  *procfd.Mapper
	meta     MetaSource
	resolver cgroup.Resolver
	logger   *slog.Logger

	mu sync.Mutex

	// Descriptors
	jobCounter    *prometheus.Desc
	devCounter    *prometheus.Desc
	devJobs       *prometheus.Desc
	devState      *prometheus.Desc
	jobPIDs       *prometheus.Desc
	unattributed  *prometheus.Desc
	unresolved    *prometheus.Desc
	layoutInfo    *prometheus.Desc
	scrapeError   *prometheus.Desc
}

func New(scanner *cgroup.Scanner, reader *ib.Reader, mapper *procfd.Mapper,
	meta MetaSource, logger *slog.Logger) *Collector {
	jobLabels := []string{"job_id", "user", "account", "partition", "device", "port", "counter"}
	devLabels := []string{"device", "port", "counter"}

	return &Collector{
		scanner: scanner, reader: reader, mapper: mapper, meta: meta, logger: logger,

		jobCounter: prometheus.NewDesc(ns+"_job_counter_total",
			"InfiniBand counter attributed to a Slurm job. Only present when the job is the device's sole user.",
			jobLabels, nil),
		devCounter: prometheus.NewDesc(ns+"_device_counter_total",
			"InfiniBand counter for a port, unattributed. Always present.",
			devLabels, nil),
		devJobs: prometheus.NewDesc(ns+"_device_jobs",
			"Number of Slurm jobs holding this device. Above 1 means per-job attribution is suppressed.",
			[]string{"device", "port"}, nil),
		devState: prometheus.NewDesc(ns+"_device_port_up",
			"1 when the port state is ACTIVE, 0 otherwise.",
			[]string{"device", "port", "link_layer", "rate"}, nil),
		jobPIDs: prometheus.NewDesc(ns+"_job_processes",
			"Processes found in the job's cgroup.",
			[]string{"job_id", "user", "account", "partition"}, nil),
		unattributed: prometheus.NewDesc(ns+"_unattributed_jobs",
			"Jobs whose network activity could not be attributed because they share a device.",
			nil, nil),
		unresolved: prometheus.NewDesc(ns+"_unresolved_sluid_allocations",
			"Cgroups keyed by SLUID (Slurm 26.05+) that could not be mapped to a job ID.",
			nil, nil),
		layoutInfo: prometheus.NewDesc(ns+"_cgroup_layout_info",
			"Always 1. The label reports which cgroup layout was detected.",
			[]string{"layout"}, nil),
		scrapeError: prometheus.NewDesc(ns+"_scrape_error",
			"1 when the last scrape failed.", nil, nil),
	}
}

// SetResolver supplies the SLUID -> job id mapping. Without one, SLUID-keyed
// allocations are counted as unresolved rather than mislabelled.
func (c *Collector) SetResolver(r cgroup.Resolver) { c.resolver = r }

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.jobCounter
	ch <- c.devCounter
	ch <- c.devJobs
	ch <- c.devState
	ch <- c.jobPIDs
	ch <- c.unattributed
	ch <- c.unresolved
	ch <- c.layoutInfo
	ch <- c.scrapeError
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ports, err := c.reader.ReadAll()
	if err != nil {
		c.logger.Error("read infiniband counters", "err", err)
		ch <- prometheus.MustNewConstMetric(c.scrapeError, prometheus.GaugeValue, 1)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.scrapeError, prometheus.GaugeValue, 0)

	allocs, layout, err := c.scanner.Scan()
	if err != nil {
		c.logger.Error("scan cgroups", "err", err)
	}
	// SLUID-keyed cgroups (Slurm 26.05+) mean nothing until the controller
	// maps them back to a job.
	allocs = cgroup.Resolve(allocs, c.resolver)
	ch <- prometheus.MustNewConstMetric(c.layoutInfo, prometheus.GaugeValue, 1, string(layout))

	// Device-level series are unconditional — they are the ground truth and
	// stay correct no matter how many jobs share a node.
	for _, p := range ports {
		up := 0.0
		if p.IsActive() {
			up = 1
		}
		ch <- prometheus.MustNewConstMetric(c.devState, prometheus.GaugeValue, up,
			p.Device, itoa(p.Port), p.LinkLayer, p.Rate)

		for name, v := range p.Values {
			ch <- prometheus.MustNewConstMetric(c.devCounter, prometheus.CounterValue,
				float64(v), p.Device, itoa(p.Port), name)
		}
	}

	// Build device → jobs so sole ownership can be decided.
	type jobDevices struct {
		alloc   cgroup.Allocation
		devices []string
	}
	var jobs []jobDevices
	deviceUsers := map[string]int{}
	unresolved := 0

	for _, a := range allocs {
		if a.JobID == "" {
			// SLUID that could not be resolved. Counting it is more useful than
			// guessing a job ID for it.
			unresolved++
			continue
		}
		devs := c.mapper.DevicesForPIDs(a.PIDs)
		jobs = append(jobs, jobDevices{alloc: a, devices: devs})
		for _, d := range devs {
			deviceUsers[d]++
		}
	}
	ch <- prometheus.MustNewConstMetric(c.unresolved, prometheus.GaugeValue, float64(unresolved))

	// Ports indexed for lookup while emitting job series.
	portsByDevice := map[string][]ib.Counters{}
	for _, p := range ports {
		portsByDevice[p.Device] = append(portsByDevice[p.Device], p)
	}

	for dev, n := range deviceUsers {
		for _, p := range portsByDevice[dev] {
			ch <- prometheus.MustNewConstMetric(c.devJobs, prometheus.GaugeValue,
				float64(n), dev, itoa(p.Port))
		}
	}

	unattributed := 0
	for _, j := range jobs {
		meta := JobMeta{}
		if c.meta != nil {
			if m, ok := c.meta.Meta(j.alloc.JobID); ok {
				meta = m
			}
		}

		ch <- prometheus.MustNewConstMetric(c.jobPIDs, prometheus.GaugeValue,
			float64(len(j.alloc.PIDs)),
			j.alloc.JobID, meta.User, meta.Account, meta.Partition)

		attributedAny := false
		for _, dev := range j.devices {
			// The rule: attribute only where this job is the device's sole user.
			if deviceUsers[dev] > 1 {
				continue
			}
			attributedAny = true
			for _, p := range portsByDevice[dev] {
				for name, v := range p.Values {
					ch <- prometheus.MustNewConstMetric(c.jobCounter, prometheus.CounterValue,
						float64(v),
						j.alloc.JobID, meta.User, meta.Account, meta.Partition,
						dev, itoa(p.Port), name)
				}
			}
		}

		if len(j.devices) > 0 && !attributedAny {
			unattributed++
		}
	}

	ch <- prometheus.MustNewConstMetric(c.unattributed, prometheus.GaugeValue, float64(unattributed))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
