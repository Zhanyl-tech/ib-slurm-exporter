// Package ib reads InfiniBand and RoCE counters from sysfs.
//
// Two counter families matter and they are not equivalent:
//
//	counters/      Standard IB PMA port counters — bytes, packets, link
//	               errors. Present on every HCA. Their widths are fixed by the
//	               PMA PortCounters layout, and most error counters are
//	               narrow: link_downed and link_error_recovery are 8-bit,
//	               local_link_integrity_errors and
//	               excessive_buffer_overrun_errors are 4-bit, symbol_error
//	               and port_rcv_errors are 16-bit (PORT_PMA_ATTR in
//	               drivers/infiniband/core/sysfs.c). Only the data and
//	               packet counters have 64-bit "extended" variants, and only
//	               on HCAs that advertise IB_PMA_CLASS_CAP_EXT_WIDTH.
//
//	               A narrow counter that reaches its maximum does not help
//	               rate(): if the HCA holds it there (OpenSM's perfmgr clears
//	               counters well before the maximum for this reason), rate()
//	               reads 0 from then on and an alert on it silently stops
//	               firing. So the exporter flags a counter at 2^width-1 as
//	               saturated instead of pretending rate() copes.
//
//	hw_counters/   Vendor (mlx5) counters. This is where the interesting
//	               signal lives for a slow all-reduce: packet_seq_err,
//	               out_of_sequence, rnr_nak_retry_err, and the RoCE congestion
//	               counters np_cnp_sent / rp_cnp_handled. The mlx5 Q counters
//	               are read as 32-bit values (be32_to_cpu in
//	               drivers/infiniband/hw/mlx5/counters.c), so they wrap;
//	               Prometheus treats a wrap as a counter reset, which
//	               under-counts by the part of the range lost at the wrap.
//
// A stalled collective almost never shows up as "throughput is low". It shows
// up as retries climbing on one HCA while the others are quiet.
package ib

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Counters is one snapshot of one port.
type Counters struct {
	Device string
	Port   int
	// Values keyed by counter name, spanning both counters/ and hw_counters/.
	Values map[string]uint64
	// Rate is the port's signalling rate as reported by sysfs, e.g. "200 Gb/sec".
	Rate string
	// State is "ACTIVE", "DOWN", etc. A down port with a job on it is the
	// finding, not a reason to skip collection.
	State string
	// LinkLayer distinguishes InfiniBand from Ethernet (RoCE); the useful
	// counters differ between them.
	LinkLayer string
}

// The counters worth exporting. Reading the whole directory would produce
// several hundred series per port, most of which nobody alerts on.
var (
	// Standard PMA port counters, read from ports/<n>/counters/. Names are
	// the sysfs file names declared with PORT_PMA_ATTR / PORT_PMA_ATTR_EXT in
	// drivers/infiniband/core/sysfs.c.
	portCounters = []string{
		"port_xmit_data", "port_rcv_data",
		"port_xmit_packets", "port_rcv_packets",
		"port_xmit_discards", "port_rcv_errors",
		"port_rcv_remote_physical_errors", "port_rcv_switch_relay_errors",
		"link_downed", "link_error_recovery",
		"local_link_integrity_errors", "symbol_error",
		"excessive_buffer_overrun_errors", "VL15_dropped",
		// "ticks during which the port had data to transmit but no data was
		// sent" (Documentation/ABI/stable/sysfs-class-infiniband) — the IB
		// congestion signal.
		"port_xmit_wait",
	}

	// mlx5 hardware counters, read from ports/<n>/hw_counters/. Every name
	// here appears in drivers/infiniband/hw/mlx5/counters.c. Whether a given
	// HCA/firmware exposes it depends on capability bits, which is why a
	// missing file is reported as absent rather than zero.
	//
	// RoCE pause (PFC) counters are deliberately not listed: mlx5 does not
	// expose them as RDMA hw_counters at all. They are netdev (ethtool)
	// counters such as rx_pause_ctrl_phy, per
	// https://docs.kernel.org/networking/device_drivers/ethernet/mellanox/mlx5/counters.html,
	// and would need a separate reader.
	hwCounters = []string{
		// Q counters (32-bit, wrap).
		"packet_seq_err", "out_of_sequence", "out_of_buffer",
		"rnr_nak_retry_err", "local_ack_timeout_err",
		"implied_nak_seq_err", "duplicate_request",
		"req_cqe_error", "resp_cqe_error",
		"req_remote_access_errors", "resp_remote_access_errors",
		"req_transport_retries_exceeded",
		"roce_adp_retrans",
		// Congestion counters (RoCE ECN/CNP).
		"np_cnp_sent", "rp_cnp_handled", "rp_cnp_ignored",
		"np_ecn_marked_roce_packets",
	}

	// pmaWidth is the field width in bits of each fixed-width PMA counter,
	// copied from the PORT_PMA_ATTR declarations in
	// drivers/infiniband/core/sysfs.c (torvalds/linux master, checked
	// 2026-09). port_xmit_data/port_rcv_data/port_{xmit,rcv}_packets are
	// omitted on purpose: they are 32-bit or 64-bit depending on whether the
	// HCA supports extended counters, which sysfs does not reveal.
	pmaWidth = map[string]uint{
		"symbol_error":                    16,
		"link_error_recovery":             8,
		"link_downed":                     8,
		"port_rcv_errors":                 16,
		"port_rcv_remote_physical_errors": 16,
		"port_rcv_switch_relay_errors":    16,
		"port_xmit_discards":              16,
		"local_link_integrity_errors":     4,
		"excessive_buffer_overrun_errors": 4,
		"VL15_dropped":                    16,
		"port_xmit_wait":                  32,
	}
)

// AllCounterNames is the union, in a stable order. Used to validate that the
// shipped dashboard only queries counters this exporter can emit.
func AllCounterNames() []string {
	out := make([]string, 0, len(portCounters)+len(hwCounters))
	out = append(out, portCounters...)
	out = append(out, hwCounters...)
	return out
}

// IsPortCounter reports whether name lives under counters/ (PMA) rather than
// hw_counters/.
func IsPortCounter(name string) bool {
	for _, n := range portCounters {
		if n == name {
			return true
		}
	}
	return false
}

// Width returns the fixed field width of a PMA counter, if it has one.
func Width(name string) (uint, bool) {
	w, ok := pmaWidth[name]
	return w, ok
}

// Saturated reports whether a fixed-width counter is at the largest value its
// field can hold. The second result is false when the width is unknown, in
// which case nothing can be said either way.
func Saturated(name string, v uint64) (sat, known bool) {
	w, ok := pmaWidth[name]
	if !ok {
		return false, false
	}
	return v == (uint64(1)<<w)-1, true
}

// Reader reads from a sysfs root. Root is injectable so the package can be
// tested against a synthetic tree on any OS.
type Reader struct {
	Root string
	// NetRoot is /sys/class/net, read only for IPoIB interfaces (IPoIB).
	NetRoot string
}

// NewReader returns a Reader for root, with NetRoot set to root's sibling
// "net" directory: /sys/class/infiniband gives /sys/class/net, and a tree
// mounted elsewhere (a container's /host/sys, a test fixture) keeps the same
// shape, so one flag moves both.
func NewReader(root string) *Reader {
	if root == "" {
		root = "/sys/class/infiniband"
	}
	return &Reader{Root: root, NetRoot: filepath.Join(filepath.Dir(root), "net")}
}

// Devices lists HCAs present on the host.
func (r *Reader) Devices() ([]string, error) {
	entries, err := os.ReadDir(r.Root)
	if err != nil {
		return nil, fmt.Errorf("list infiniband devices: %w", err)
	}
	var out []string
	for _, e := range entries {
		// Devices appear as symlinks, so IsDir() is false; accept both.
		out = append(out, e.Name())
	}
	return out, nil
}

// Ports lists port numbers for a device.
func (r *Reader) Ports(device string) ([]int, error) {
	base := filepath.Join(r.Root, device, "ports")
	entries, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	var out []int
	for _, e := range entries {
		if n, err := strconv.Atoi(e.Name()); err == nil {
			out = append(out, n)
		}
	}
	return out, nil
}

// Read returns a snapshot for one port. Missing counters are skipped rather
// than defaulted to zero: a counter this kernel does not expose and a counter
// that is genuinely zero are different facts, and conflating them invents data.
func (r *Reader) Read(device string, port int) (Counters, error) {
	portDir := filepath.Join(r.Root, device, "ports", strconv.Itoa(port))

	if _, err := os.Stat(portDir); err != nil {
		return Counters{}, fmt.Errorf("port %s:%d: %w", device, port, err)
	}

	c := Counters{
		Device:    device,
		Port:      port,
		Values:    make(map[string]uint64, len(portCounters)+len(hwCounters)),
		Rate:      readString(filepath.Join(portDir, "rate")),
		State:     normalizeState(readString(filepath.Join(portDir, "state"))),
		LinkLayer: readString(filepath.Join(portDir, "link_layer")),
	}

	for _, name := range portCounters {
		if v, ok := readUint(filepath.Join(portDir, "counters", name)); ok {
			c.Values[name] = v
		}
	}
	for _, name := range hwCounters {
		if v, ok := readUint(filepath.Join(portDir, "hw_counters", name)); ok {
			c.Values[name] = v
		}
	}

	return c, nil
}

// ReadAll snapshots every port on every device.
func (r *Reader) ReadAll() ([]Counters, error) {
	devices, err := r.Devices()
	if err != nil {
		return nil, err
	}
	var out []Counters
	for _, d := range devices {
		ports, err := r.Ports(d)
		if err != nil {
			continue
		}
		for _, p := range ports {
			c, err := r.Read(d, p)
			if err != nil {
				continue
			}
			out = append(out, c)
		}
	}
	return out, nil
}

func readString(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func readUint(path string) (uint64, bool) {
	s := readString(path)
	if s == "" {
		return 0, false
	}
	// Some counters read as "N/A" when unsupported by the firmware.
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// normalizeState turns sysfs's "4: ACTIVE" into "ACTIVE".
func normalizeState(raw string) string {
	if _, after, found := strings.Cut(raw, ":"); found {
		return strings.TrimSpace(after)
	}
	return raw
}

// IsActive reports whether the port is up.
func (c Counters) IsActive() bool {
	return strings.EqualFold(c.State, "ACTIVE")
}
