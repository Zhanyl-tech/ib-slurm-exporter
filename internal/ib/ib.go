// Package ib reads InfiniBand and RoCE counters from sysfs.
//
// Two counter families matter and they are not equivalent:
//
//   counters/      Standard IB port counters — bytes, packets, link errors.
//                  Present on every HCA. 32-bit on older hardware, so they
//                  wrap; the collector reports raw values and lets Prometheus'
//                  rate() handle resets rather than trying to be clever here.
//
//   hw_counters/   Vendor (mlx5) counters. This is where the interesting
//                  signal lives for a slow all-reduce: packet_seq_err,
//                  out_of_sequence, rnr_nak_retry_err, and the RoCE congestion
//                  counters np_cnp_sent / rp_cnp_handled.
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
	// Standard port counters.
	portCounters = []string{
		"port_xmit_data", "port_rcv_data",
		"port_xmit_packets", "port_rcv_packets",
		"port_xmit_discards", "port_rcv_errors",
		"port_rcv_remote_physical_errors", "port_rcv_switch_relay_errors",
		"link_downed", "link_error_recovery",
		"local_link_integrity_errors", "symbol_error",
		"excessive_buffer_overrun_errors", "VL15_dropped",
	}

	// mlx5 hardware counters — the retry and congestion signal.
	hwCounters = []string{
		"packet_seq_err", "out_of_sequence", "out_of_buffer",
		"rnr_nak_retry_err", "local_ack_timeout_err",
		"implied_nak_seq_err", "duplicate_request",
		"req_cqe_error", "resp_cqe_error",
		"req_remote_access_errors", "resp_remote_access_errors",
		"rx_pause", "tx_pause", "rx_pause_duration", "tx_pause_duration",
		"np_cnp_sent", "rp_cnp_handled", "np_ecn_marked_roce_packets",
	}
)

// AllCounterNames is the union, in a stable order, for metric registration.
func AllCounterNames() []string {
	out := make([]string, 0, len(portCounters)+len(hwCounters))
	out = append(out, portCounters...)
	out = append(out, hwCounters...)
	return out
}

// IsErrorCounter reports whether a counter indicates a fault rather than
// volume. Used to decide what belongs on an alert versus a dashboard.
func IsErrorCounter(name string) bool {
	switch name {
	case "port_xmit_data", "port_rcv_data", "port_xmit_packets", "port_rcv_packets",
		"rx_pause_duration", "tx_pause_duration", "np_cnp_sent", "rp_cnp_handled":
		return false
	}
	return true
}

// Reader reads from a sysfs root. Root is injectable so the package can be
// tested against a synthetic tree on any OS.
type Reader struct {
	Root string
}

func NewReader(root string) *Reader {
	if root == "" {
		root = "/sys/class/infiniband"
	}
	return &Reader{Root: root}
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

// IsRoCE reports whether the port is Ethernet-backed.
func (c Counters) IsRoCE() bool {
	return strings.EqualFold(c.LinkLayer, "Ethernet")
}

// IsActive reports whether the port is up.
func (c Counters) IsActive() bool {
	return strings.EqualFold(c.State, "ACTIVE")
}
