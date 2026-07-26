// Package fixture builds a synthetic /sys, /proc and cgroup tree.
//
// This exporter reads Linux-only interfaces, which normally means it can only
// be developed or reviewed on a GPU node with InfiniBand. Making every root
// injectable and generating a realistic tree here means the whole thing is
// testable on a laptop and demoable in one command — which is the difference
// between a repo people run and a repo people skim.
//
// The tree is deliberately awkward: two jobs share mlx5_1 so the attribution
// suppression path is exercised, and one allocation is keyed by SLUID so the
// Slurm 26.05 layout is too.
package fixture

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// Layout selects which cgroup hierarchy to generate.
type Layout string

const (
	V1      Layout = "v1"
	V2      Layout = "v2"
	V2SLUID Layout = "v2-sluid"
)

// Spec describes the cluster to synthesise.
type Spec struct {
	Layout Layout
	// Jobs maps job identifier -> PIDs.
	Jobs map[string][]int
	// PIDDevices maps PID -> uverbs index the process holds open.
	PIDDevices map[int][]int
	// Devices maps HCA name -> uverbs index.
	Devices map[string]int
	// Counters maps "device:port" -> counter name -> value.
	Counters map[string]map[string]uint64
	// RoCE marks devices whose link_layer is Ethernet.
	RoCE map[string]bool
}

// Default is a small cluster with the interesting cases baked in:
//
//	job 918001  → mlx5_0, sole user           → attributed
//	job 918002  → mlx5_1, shared with 918003  → suppressed
//	job 918003  → mlx5_1, shared with 918002  → suppressed
//	job 918004  → mlx5_2 (RoCE), sole user    → attributed, pause + CNP counters
func Default(layout Layout) Spec {
	return Spec{
		Layout: layout,
		Jobs: map[string][]int{
			"918001": {41001, 41002},
			"918002": {42001},
			"918003": {43001},
			"918004": {44001, 44002},
		},
		PIDDevices: map[int][]int{
			41001: {0}, 41002: {0},
			42001: {1},
			43001: {1},
			44001: {2}, 44002: {2},
		},
		Devices: map[string]int{"mlx5_0": 0, "mlx5_1": 1, "mlx5_2": 2},
		RoCE:    map[string]bool{"mlx5_2": true},
		Counters: map[string]map[string]uint64{
			// Healthy: volume, no retries.
			"mlx5_0:1": {
				"port_xmit_data": 8_812_004_331, "port_rcv_data": 8_798_221_004,
				"port_xmit_packets": 91_004_221, "port_rcv_packets": 90_881_004,
				"port_xmit_discards": 0, "port_rcv_errors": 0,
				"link_downed": 0, "link_error_recovery": 0,
				"symbol_error": 0, "local_link_integrity_errors": 0,
				"packet_seq_err": 0, "out_of_sequence": 0,
				"rnr_nak_retry_err": 0, "local_ack_timeout_err": 0,
				"out_of_buffer": 0,
			},
			// The interesting one: throughput looks fine, retries are climbing.
			// This is what a stalled all-reduce actually looks like.
			"mlx5_1:1": {
				"port_xmit_data": 4_410_882_100, "port_rcv_data": 1_204_118_882,
				"port_xmit_packets": 44_180_221, "port_rcv_packets": 12_004_881,
				"port_xmit_discards": 1_884, "port_rcv_errors": 42,
				"link_downed": 2, "link_error_recovery": 7,
				"symbol_error": 118, "local_link_integrity_errors": 9,
				"packet_seq_err": 48_221, "out_of_sequence": 47_889,
				"rnr_nak_retry_err": 12_004, "local_ack_timeout_err": 881,
				"out_of_buffer": 3_442,
			},
			// RoCE under congestion: pause frames and ECN marks.
			"mlx5_2:1": {
				"port_xmit_data": 6_004_118_002, "port_rcv_data": 5_998_004_112,
				"port_xmit_packets": 61_004_118, "port_rcv_packets": 60_884_002,
				"port_xmit_discards": 0, "port_rcv_errors": 0,
				"link_downed": 0, "link_error_recovery": 0,
				"rx_pause": 88_412, "tx_pause": 91_004,
				"rx_pause_duration": 4_118, "tx_pause_duration": 4_402,
				"np_cnp_sent": 12_884, "rp_cnp_handled": 12_701,
				"np_ecn_marked_roce_packets": 441_882,
				"packet_seq_err": 44, "out_of_sequence": 41,
			},
		},
	}
}

// Roots are the generated paths, to be passed to the readers.
type Roots struct {
	Base   string
	Sys    string // /sys/class/infiniband
	Verbs  string // /sys/class/infiniband_verbs
	Proc   string // /proc
	Cgroup string // /sys/fs/cgroup
}

// Build writes the tree under base and returns the roots.
func Build(base string, spec Spec) (Roots, error) {
	r := Roots{
		Base:   base,
		Sys:    filepath.Join(base, "sys", "class", "infiniband"),
		Verbs:  filepath.Join(base, "sys", "class", "infiniband_verbs"),
		Proc:   filepath.Join(base, "proc"),
		Cgroup: filepath.Join(base, "sys", "fs", "cgroup"),
	}

	if err := buildInfiniband(r, spec); err != nil {
		return r, err
	}
	if err := buildProc(r, spec); err != nil {
		return r, err
	}
	if err := buildCgroups(r, spec); err != nil {
		return r, err
	}
	return r, nil
}

func buildInfiniband(r Roots, spec Spec) error {
	for dev, uverbs := range spec.Devices {
		port := filepath.Join(r.Sys, dev, "ports", "1")
		if err := os.MkdirAll(filepath.Join(port, "counters"), 0o755); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(port, "hw_counters"), 0o755); err != nil {
			return err
		}

		linkLayer, rate := "InfiniBand", "200 Gb/sec (4X HDR)"
		if spec.RoCE[dev] {
			linkLayer, rate = "Ethernet", "100 Gb/sec (4X EDR)"
		}
		writeFile(filepath.Join(port, "state"), "4: ACTIVE")
		writeFile(filepath.Join(port, "rate"), rate)
		writeFile(filepath.Join(port, "link_layer"), linkLayer)

		// Split values across counters/ and hw_counters/ the way the kernel does.
		for name, v := range spec.Counters[dev+":1"] {
			sub := "hw_counters"
			switch name {
			case "port_xmit_data", "port_rcv_data", "port_xmit_packets", "port_rcv_packets",
				"port_xmit_discards", "port_rcv_errors", "link_downed",
				"link_error_recovery", "symbol_error", "local_link_integrity_errors":
				sub = "counters"
			}
			writeFile(filepath.Join(port, sub, name), strconv.FormatUint(v, 10))
		}

		// The uverbs → ibdev mapping the mapper resolves through.
		vdir := filepath.Join(r.Verbs, fmt.Sprintf("uverbs%d", uverbs))
		if err := os.MkdirAll(vdir, 0o755); err != nil {
			return err
		}
		writeFile(filepath.Join(vdir, "ibdev"), dev)
	}
	return nil
}

// buildProc fakes /proc/<pid>/fd with symlinks to uverbs nodes.
func buildProc(r Roots, spec Spec) error {
	devNodes := filepath.Join(r.Base, "dev", "infiniband")
	if err := os.MkdirAll(devNodes, 0o755); err != nil {
		return err
	}

	for pid, uverbsIdxs := range spec.PIDDevices {
		fdDir := filepath.Join(r.Proc, strconv.Itoa(pid), "fd")
		if err := os.MkdirAll(fdDir, 0o755); err != nil {
			return err
		}
		// Descriptors 0-2 are the usual std streams; RDMA handles come after.
		for i, idx := range uverbsIdxs {
			target := filepath.Join(devNodes, fmt.Sprintf("uverbs%d", idx))
			writeFile(target, "")
			link := filepath.Join(fdDir, strconv.Itoa(3+i))
			os.Remove(link)
			if err := os.Symlink(target, link); err != nil {
				return err
			}
		}
	}
	return nil
}

func buildCgroups(r Roots, spec Spec) error {
	for job, pids := range spec.Jobs {
		var jobDir string
		switch spec.Layout {
		case V1:
			jobDir = filepath.Join(r.Cgroup, "freezer", "slurm", "uid_1000", "job_"+job)
		case V2SLUID:
			// 26.05 keys the directory by SLUID rather than JobId.
			jobDir = filepath.Join(r.Cgroup, "system.slice", "slurmstepd.scope",
				"job_"+sluidFor(job))
		default:
			jobDir = filepath.Join(r.Cgroup, "system.slice", "slurmstepd.scope", "job_"+job)
		}

		// PIDs live in the step cgroups, not at the job level — an exporter
		// that only reads the job-level file finds nothing.
		stepDir := filepath.Join(jobDir, "step_0")
		if err := os.MkdirAll(stepDir, 0o755); err != nil {
			return err
		}
		writeFile(filepath.Join(jobDir, "cgroup.procs"), "")

		var body string
		for _, p := range pids {
			body += strconv.Itoa(p) + "\n"
		}
		writeFile(filepath.Join(stepDir, "cgroup.procs"), body)
	}
	return nil
}

// sluidFor produces a stable synthetic SLUID for a job id. The real format is
// SchedMD's; this only needs to be non-numeric so the SLUID branch is taken.
func sluidFor(jobID string) string {
	return "s" + jobID + "x7f2a"
}

// SLUIDToJob inverts sluidFor, standing in for a controller lookup.
func SLUIDToJob(sluid string) (string, bool) {
	if len(sluid) < 2 || sluid[0] != 's' {
		return "", false
	}
	rest := sluid[1:]
	idx := len(rest) - len("x7f2a")
	if idx <= 0 || rest[idx:] != "x7f2a" {
		return "", false
	}
	return rest[:idx], true
}

func writeFile(path, content string) {
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	_ = os.WriteFile(path, []byte(content), 0o644)
}
