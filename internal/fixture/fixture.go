// Package fixture builds a synthetic /sys, /proc and cgroup tree.
//
// This exporter reads Linux-only interfaces, which normally means it can only
// be developed or reviewed on a GPU node with InfiniBand. Making every root
// injectable and generating a tree here means the whole thing is testable on
// a laptop and demoable in one command.
//
// Everything here is synthetic. The directory shapes follow Slurm's and the
// kernel's documentation (cited below); the counter values, PIDs and SLUIDs
// are invented. A test passing against this tree shows the code agrees with
// the documentation as this package reads it — not that it matches a real
// node. Golden trees captured from real hardware would close that gap.
//
// The tree is deliberately awkward: two jobs share mlx5_1 so the attribution
// suppression path is exercised, and the v2-sluid layout names job
// directories by SLUID the way Slurm 26.05 does by default.
package fixture

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/ib"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/rdmares"
)

// Layout selects which cgroup hierarchy to generate.
type Layout string

const (
	// V1 is <root>/freezer/slurm/uid_<uid>/job_<jobid>/step_0/.
	V1 Layout = "v1"
	// V2 is <scope>/job_<jobid>/step_0/{slurm,user/task_0}/: Slurm before
	// 26.05, or 26.05+ with CgroupJobIdPaths=yes.
	V2 Layout = "v2"
	// V2SLUID is <scope>/<SLUID>/step_0/{slurm,user/task_0}/: the Slurm
	// 26.05 default. The job directory is the bare SLUID with no "job_"
	// prefix, per https://slurm.schedmd.com/cgroup.conf.html
	// (CgroupJobIdPaths) and the example tree in
	// https://slurm.schedmd.com/cgroup_v2.html.
	V2SLUID Layout = "v2-sluid"
)

// DefaultScope is where the v2 layouts put slurmstepd.scope.
const DefaultScope = "system.slice/slurmstepd.scope"

// Spec describes the node to synthesise.
type Spec struct {
	Layout Layout
	// Scope is the slurmstepd scope relative to the cgroup root (v2 only).
	// Empty means DefaultScope.
	Scope string
	// Jobs maps job ID -> PIDs of the job's user processes.
	Jobs map[string][]int
	// PIDDevices maps PID -> uverbs indexes the process holds open.
	PIDDevices map[int][]int
	// Devices maps HCA name -> uverbs index.
	Devices map[string]int
	// Ports maps HCA name -> number of ports. Missing means 1.
	Ports map[string]int
	// Counters maps "device:port" -> counter name -> value. Names are
	// written to counters/ or hw_counters/ the way the kernel splits them.
	Counters map[string]map[string]uint64
	// RoCE marks devices whose link_layer is Ethernet.
	RoCE map[string]bool
	// OmitStepd skips the slurmstepd processes (v2), so a SLUID cannot be
	// resolved from the process title.
	OmitStepd bool
	// IPoIB lists IPoIB network interfaces. They model mlx5 "enhanced"
	// IPoIB, whose QP is invisible to RDMA resource tracking, so QPs()
	// reports no QP for them.
	IPoIB []IPoIB
}

// IPoIB is a synthetic IPoIB interface. A parent (Parent empty) hangs off
// its HCA's device directory with dev_port = port - 1, the way
// ipoib_parent_init places it; a P_Key child names its parent and is a
// virtual device linked to it through iflink (ipoib_get_iflink).
type IPoIB struct {
	Name      string
	Device    string // parent only
	Port      int    // parent only
	Parent    string // child only
	OperState string // "up" when empty
}

// Default is a small node with the interesting cases baked in:
//
//	job 918001  → mlx5_0, sole user           → attributed
//	job 918002  → mlx5_1, shared with 918003  → suppressed
//	job 918003  → mlx5_1, shared with 918002  → suppressed
//	job 918004  → mlx5_2 (RoCE), sole user    → attributed, ECN/CNP counters
//
// ib_ipoib creates an interface on every InfiniBand port (ipoib_add_one), so
// mlx5_0 and mlx5_1 have ib0 and ib1. Both are administratively down, carry
// nothing, and do not affect attribution; tests bring one up.
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
		IPoIB: []IPoIB{
			{Name: "ib0", Device: "mlx5_0", Port: 1, OperState: "down"},
			{Name: "ib1", Device: "mlx5_1", Port: 1, OperState: "down"},
		},
		Counters: map[string]map[string]uint64{
			// Healthy: volume, no retries.
			"mlx5_0:1": {
				"port_xmit_data": 8_812_004_331, "port_rcv_data": 8_798_221_004,
				"port_xmit_packets": 91_004_221, "port_rcv_packets": 90_881_004,
				"port_xmit_discards": 0, "port_rcv_errors": 0,
				"link_downed": 0, "link_error_recovery": 0,
				"symbol_error": 0, "local_link_integrity_errors": 0,
				"port_xmit_wait": 1_204,
				"packet_seq_err": 0, "out_of_sequence": 0,
				"rnr_nak_retry_err": 0, "local_ack_timeout_err": 0,
				"out_of_buffer": 0,
			},
			// The interesting one: throughput looks fine, retries are climbing.
			// This is what a stalled all-reduce looks like.
			"mlx5_1:1": {
				"port_xmit_data": 4_410_882_100, "port_rcv_data": 1_204_118_882,
				"port_xmit_packets": 44_180_221, "port_rcv_packets": 12_004_881,
				"port_xmit_discards": 1_884, "port_rcv_errors": 42,
				"link_downed": 2, "link_error_recovery": 7,
				"symbol_error": 118, "local_link_integrity_errors": 9,
				"port_xmit_wait": 88_120_004,
				"packet_seq_err": 48_221, "out_of_sequence": 47_889,
				"rnr_nak_retry_err": 12_004, "local_ack_timeout_err": 881,
				"out_of_buffer": 3_442,
			},
			// RoCE under congestion: ECN marks and CNPs. (Pause frames are
			// netdev counters, not RDMA hw_counters, so they are not here.)
			"mlx5_2:1": {
				"port_xmit_data": 6_004_118_002, "port_rcv_data": 5_998_004_112,
				"port_xmit_packets": 61_004_118, "port_rcv_packets": 60_884_002,
				"port_xmit_discards": 0, "port_rcv_errors": 0,
				"link_downed": 0, "link_error_recovery": 0,
				"np_cnp_sent": 12_884, "rp_cnp_handled": 12_701,
				"rp_cnp_ignored":             3,
				"np_ecn_marked_roce_packets": 441_882,
				"roce_adp_retrans":           17,
				"packet_seq_err":             44, "out_of_sequence": 41,
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
	Net    string // /sys/class/net
}

// StepdPID is the synthetic PID of a job's step_0 slurmstepd.
func StepdPID(jobID string) int {
	n, _ := strconv.Atoi(jobID)
	return 500_000 + n%100_000
}

// InfinityPID is the synthetic PID of slurmstepd's infinity process.
const InfinityPID = 499_999

// Build writes the tree under base and returns the roots.
func Build(base string, spec Spec) (Roots, error) {
	r := Roots{
		Base:   base,
		Sys:    filepath.Join(base, "sys", "class", "infiniband"),
		Verbs:  filepath.Join(base, "sys", "class", "infiniband_verbs"),
		Proc:   filepath.Join(base, "proc"),
		Cgroup: filepath.Join(base, "sys", "fs", "cgroup"),
		Net:    filepath.Join(base, "sys", "class", "net"),
	}
	w := &writer{}
	buildInfiniband(w, r, spec)
	buildNet(w, r, spec)
	buildProc(w, r, spec)
	buildCgroups(w, r, spec)
	return r, w.err
}

// writer keeps the first error so the build functions stay readable.
type writer struct{ err error }

func (w *writer) mkdir(path string) {
	if w.err == nil {
		w.err = os.MkdirAll(path, 0o755)
	}
}

func (w *writer) file(path, content string) {
	w.mkdir(filepath.Dir(path))
	if w.err == nil {
		w.err = os.WriteFile(path, []byte(content), 0o644)
	}
}

func (w *writer) symlink(target, link string) {
	w.mkdir(filepath.Dir(link))
	if w.err != nil {
		return
	}
	if err := os.Remove(link); err != nil && !errors.Is(err, os.ErrNotExist) {
		w.err = err
		return
	}
	w.err = os.Symlink(target, link)
}

func ports(spec Spec, dev string) int {
	if n := spec.Ports[dev]; n > 0 {
		return n
	}
	return 1
}

func buildInfiniband(w *writer, r Roots, spec Spec) {
	for dev, uverbs := range spec.Devices {
		for p := 1; p <= ports(spec, dev); p++ {
			port := filepath.Join(r.Sys, dev, "ports", strconv.Itoa(p))
			w.mkdir(filepath.Join(port, "counters"))
			w.mkdir(filepath.Join(port, "hw_counters"))

			linkLayer, rate := "InfiniBand", "200 Gb/sec (4X HDR)"
			if spec.RoCE[dev] {
				linkLayer, rate = "Ethernet", "100 Gb/sec (4X EDR)"
			}
			w.file(filepath.Join(port, "state"), "4: ACTIVE")
			w.file(filepath.Join(port, "rate"), rate)
			w.file(filepath.Join(port, "link_layer"), linkLayer)

			for name, v := range spec.Counters[fmt.Sprintf("%s:%d", dev, p)] {
				w.file(CounterPath(r, dev, p, name), strconv.FormatUint(v, 10))
			}
		}

		// The uverbs → ibdev mapping the mapper resolves through.
		w.file(filepath.Join(r.Verbs, fmt.Sprintf("uverbs%d", uverbs), "ibdev"), dev)
	}
}

// DeviceDir is the synthetic parent device of an HCA, the directory
// /sys/class/infiniband/<dev>/device points at (a PCI function on a real
// host).
func DeviceDir(r Roots, dev string) string {
	return filepath.Join(r.Base, "sys", "devices", "pci-"+dev)
}

// NetDev writes one /sys/class/net entry: the attributes the exporter reads,
// in dir, and the class symlink to it.
func NetDev(r Roots, dir string, typ, devPort, ifindex, iflink int, operstate string) error {
	w := &writer{}
	netDev(w, r, dir, typ, devPort, ifindex, iflink, operstate)
	return w.err
}

func netDev(w *writer, r Roots, dir string, typ, devPort, ifindex, iflink int, operstate string) {
	for name, v := range map[string]string{
		"type":      strconv.Itoa(typ),
		"dev_port":  strconv.Itoa(devPort),
		"ifindex":   strconv.Itoa(ifindex),
		"iflink":    strconv.Itoa(iflink),
		"operstate": operstate,
	} {
		w.file(filepath.Join(dir, name), v+"\n")
	}
	w.symlink(dir, filepath.Join(r.Net, filepath.Base(dir)))
}

// buildNet gives every HCA a parent device and writes the network
// interfaces: loopback, an Ethernet netdev for each RoCE HCA (type 1,
// ARPHRD_ETHER), and spec.IPoIB (type 32, ARPHRD_INFINIBAND).
func buildNet(w *writer, r Roots, spec Spec) {
	devs := make([]string, 0, len(spec.Devices))
	for d := range spec.Devices {
		devs = append(devs, d)
	}
	sort.Strings(devs)

	virtual := filepath.Join(r.Base, "sys", "devices", "virtual", "net")
	netDev(w, r, filepath.Join(virtual, "lo"), 772, 0, 1, 1, "unknown")
	ifindex := 2
	for _, d := range devs {
		w.mkdir(DeviceDir(r, d))
		w.symlink(DeviceDir(r, d), filepath.Join(r.Sys, d, "device"))
		if spec.RoCE[d] {
			netDev(w, r, filepath.Join(DeviceDir(r, d), "net", "eth-"+d), 1, 0, ifindex, ifindex, "up")
			ifindex++
		}
	}
	index := map[string]int{}
	for _, children := range []bool{false, true} {
		for _, i := range spec.IPoIB {
			if (i.Parent != "") != children {
				continue
			}
			op := i.OperState
			if op == "" {
				op = "up"
			}
			if !children {
				netDev(w, r, filepath.Join(DeviceDir(r, i.Device), "net", i.Name), 32, i.Port-1, ifindex, ifindex, op)
			} else {
				netDev(w, r, filepath.Join(virtual, i.Name), 32, 0, ifindex, index[i.Parent], op)
			}
			index[i.Name] = ifindex
			ifindex++
		}
	}
}

// CounterPath is where the kernel exposes a counter: PMA counters under
// counters/, vendor counters under hw_counters/.
func CounterPath(r Roots, dev string, port int, name string) string {
	sub := "hw_counters"
	if ib.IsPortCounter(name) {
		sub = "counters"
	}
	return filepath.Join(r.Sys, dev, "ports", strconv.Itoa(port), sub, name)
}

// buildProc fakes /proc/<pid>/{fd,status,cmdline}. fd entries are symlinks
// to uverbs nodes, the way the kernel presents open character devices.
func buildProc(w *writer, r Roots, spec Spec) {
	devNodes := filepath.Join(r.Base, "dev", "infiniband")
	w.mkdir(devNodes)

	proc := func(pid int, comm, cmdline string, uverbs []int) {
		dir := filepath.Join(r.Proc, strconv.Itoa(pid))
		w.mkdir(filepath.Join(dir, "fd"))
		w.file(filepath.Join(dir, "status"),
			fmt.Sprintf("Name:\t%s\nTgid:\t%d\nPid:\t%d\n", comm, pid, pid))
		w.file(filepath.Join(dir, "cmdline"), cmdline)
		// Descriptors 0-2 are the usual std streams; RDMA handles come after.
		for i, idx := range uverbs {
			target := filepath.Join(devNodes, fmt.Sprintf("uverbs%d", idx))
			w.file(target, "")
			w.symlink(target, filepath.Join(dir, "fd", strconv.Itoa(3+i)))
		}
	}

	for _, pids := range spec.Jobs {
		for _, pid := range pids {
			proc(pid, "python", "python\x00train.py\x00", spec.PIDDevices[pid])
		}
	}
	if spec.Layout != V1 && !spec.OmitStepd {
		// slurmstepd rewrites its argv to this title; systemd-cgls shows it
		// in the cgroup_v2.html example tree as "slurmstepd: [3385.0]".
		for job := range spec.Jobs {
			proc(StepdPID(job), "slurmstepd", "slurmstepd: ["+job+".0]\x00", nil)
		}
		proc(InfinityPID, "slurmstepd", "/usr/sbin/slurmstepd\x00infinity\x00", nil)
	}
}

func buildCgroups(w *writer, r Roots, spec Spec) {
	scope := spec.Scope
	if scope == "" {
		scope = DefaultScope
	}
	scopeDir := filepath.Join(r.Cgroup, scope)

	if spec.Layout != V1 {
		// The scope exists (with system/) as soon as slurmd starts, even
		// with no jobs.
		w.file(filepath.Join(scopeDir, "system", "cgroup.procs"), strconv.Itoa(InfinityPID)+"\n")
	}

	for job, pids := range spec.Jobs {
		var body strings.Builder
		for _, p := range pids {
			body.WriteString(strconv.Itoa(p) + "\n")
		}

		switch spec.Layout {
		case V1:
			jobDir := filepath.Join(r.Cgroup, "freezer", "slurm", "uid_1000", "job_"+job)
			w.file(filepath.Join(jobDir, "cgroup.procs"), "")
			// PIDs live in the step cgroups, not at the job level — an
			// exporter that only reads the job-level file finds nothing.
			w.file(filepath.Join(jobDir, "step_0", "cgroup.procs"), body.String())
		default:
			name := "job_" + job
			if spec.Layout == V2SLUID {
				name = SLUIDFor(job)
			}
			jobDir := filepath.Join(scopeDir, name)
			w.file(filepath.Join(jobDir, "cgroup.procs"), "")
			stepDir := filepath.Join(jobDir, "step_0")
			stepd := ""
			if !spec.OmitStepd {
				stepd = strconv.Itoa(StepdPID(job)) + "\n"
			}
			w.file(filepath.Join(stepDir, "slurm", "cgroup.procs"), stepd)
			w.file(filepath.Join(stepDir, "user", "task_0", "cgroup.procs"), body.String())
		}
	}
}

// SLUIDFor produces a stable synthetic SLUID for a job ID. It is shaped like
// the documented examples ("s" plus 13 upper-case alphanumerics, e.g.
// sEKNKTV3WPV500), but SchedMD does not document the encoding and this is
// not it: a real SLUID does not contain the job ID. The "SYN" marker makes a
// synthetic one easy to spot.
func SLUIDFor(jobID string) string {
	s := "sSYN" + jobID + "X"
	for len(s) < 14 {
		s += "0"
	}
	return s
}

// SLUIDToJob inverts SLUIDFor, standing in for a controller lookup.
func SLUIDToJob(sluid string) (string, bool) {
	rest, ok := strings.CutPrefix(sluid, "sSYN")
	if !ok {
		return "", false
	}
	id, _, ok := strings.Cut(rest, "X")
	if !ok || id == "" {
		return "", false
	}
	return id, true
}

// Advance adds a deterministic increment to every non-zero counter in spec,
// so a long-running demo has rates to show. Fixed-width PMA counters stop at
// their maximum, the way the exporter assumes real ones do.
func Advance(r Roots, spec Spec, step uint64) error {
	keys := make([]string, 0, len(spec.Counters))
	for k := range spec.Counters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		dev, portStr, _ := strings.Cut(k, ":")
		port, _ := strconv.Atoi(portStr)
		for name, base := range spec.Counters[k] {
			if base == 0 {
				continue // healthy stays healthy
			}
			path := CounterPath(r, dev, port, name)
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
			if err != nil {
				return err
			}
			inc := base / 2000
			if inc == 0 {
				inc = 1
			}
			v += inc * step
			if w, ok := ib.Width(name); ok {
				if limit := uint64(1)<<w - 1; v > limit {
					v = limit
				}
			}
			if err := os.WriteFile(path, []byte(strconv.FormatUint(v, 10)), 0o644); err != nil {
				return err
			}
		}
	}
	return nil
}

// QPs synthesises what RDMA resource tracking would report for spec: the
// kernel's SMI/GSI management QPs on every port, and one RC QP on port 1 of
// each device a user process holds open. Synthetic, like the rest.
func QPs(spec Spec) []rdmares.QP {
	var out []rdmares.QP
	devs := make([]string, 0, len(spec.Devices))
	byIdx := map[int]string{}
	for d, idx := range spec.Devices {
		devs = append(devs, d)
		byIdx[idx] = d
	}
	sort.Strings(devs)
	for _, d := range devs {
		for p := 1; p <= ports(spec, d); p++ {
			out = append(out,
				rdmares.QP{Device: d, Port: p, LQPN: 0, Type: "SMI", State: "RTS", Kernel: true, Comm: "ib_core"},
				rdmares.QP{Device: d, Port: p, LQPN: 1, Type: "GSI", State: "RTS", Kernel: true, Comm: "ib_core"})
		}
	}
	pids := make([]int, 0, len(spec.PIDDevices))
	for pid := range spec.PIDDevices {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	lqpn := uint32(100)
	for _, pid := range pids {
		for _, idx := range spec.PIDDevices[pid] {
			out = append(out, rdmares.QP{Device: byIdx[idx], Port: 1, LQPN: lqpn,
				Type: "RC", State: "RTS", PID: pid, Comm: "python"})
			lqpn++
		}
	}
	return out
}
