// Package rdmares reads RDMA queue-pair ownership from the kernel's RDMA
// resource tracking, via iproute2's `rdma -j resource show qp`.
//
// Why it exists: the default ownership signal (an open uverbs descriptor)
// cannot see two things that matter for attribution.
//
//   - Kernel RDMA consumers — NFS/RDMA, Lustre o2ib, iSER, IPoIB — create
//     queue pairs with no process and no descriptor. rdma_netlink.h says of
//     RDMA_NLDEV_ATTR_RES_PID: "in case of kernel origin, PID won't exist".
//   - A descriptor carries no port, and NCCL opens every active HCA whether
//     or not it sends on it. A QP is bound to one port and only exists where
//     traffic can flow.
//
// Resource tracking reports each QP's device, port, type and either its
// owning PID or (for kernel objects) a kernel name, so users can be counted
// per port, kernel users included.
//
// What it cannot see: only QPs created through ib_core are tracked
// (create_qp in drivers/infiniband/core/verbs.c calls rdma_restrack_add).
// Two kinds of QP bypass that:
//
//   - mlx5 enhanced IPoIB. On an HCA with the ipoib_enhanced_offloads
//     capability, mlx5_core creates the IPoIB QP with a raw firmware
//     command (drivers/net/ethernet/mellanox/mlx5/core/ipoib/ipoib.c), with
//     no restrack entry. The collector counts IPoIB interfaces from sysfs
//     to cover this (package ib). Non-enhanced IPoIB creates its QP with
//     ib_create_qp when the interface registers, so it is listed, as a
//     kernel QP named "ib_ipoib".
//   - DEVX. mlx5 lets userspace create QPs through firmware commands
//     (drivers/infiniband/hw/mlx5/devx.c, no restrack call). UCX's IB
//     memory-domain config defaults MLX5_DEVX to "try" and
//     MLX5_DEVX_OBJECTS to "rcqp,rcsrq,dct,dcsrq,dci,cq"
//     (src/uct/ib/base/ib_md.c), so a UCX job on mlx5 may have no QP here
//     at all. Nothing in this package detects that. Whether DEVX succeeds
//     on a given HCA, firmware and kernel has not been checked.
//
// The JSON keys parsed here ("ifname", "port", "lqpn", "type", "state",
// "pid", "comm") are the ones printed by rdma/res-qp.c and rdma/res.c in
// iproute2 (print_link, res_qp_line, print_comm). The QP type strings come
// from qp_types_to_str in the same source. None of this has been run
// against a real HCA from this repository; the parser is tested against
// output written to that source's format.
package rdmares

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"time"
)

// QP is one queue pair as reported by resource tracking.
type QP struct {
	Device string
	// Port is 0 when the QP is not bound to a port yet (rdma prints these
	// as "link mlx5_4/-"). Callers should treat such a QP as possibly on
	// any port of the device.
	Port  int
	LQPN  uint32
	Type  string
	State string
	// PID is the owning process in the reader's PID namespace. Zero with
	// Kernel set means a kernel-owned QP.
	PID    int
	Kernel bool
	// Comm is the process name, or for a kernel QP the name of the module
	// that called ib_create_qp (its KBUILD_MODNAME): "ib_core" for the
	// SMI/GSI QPs, "ib_ipoib" for non-enhanced IPoIB, "rdma_cm" for QPs made
	// with rdma_create_qp (NFS/RDMA among them).
	Comm string
}

// IsManagement reports QP0/QP1 (SMI/GSI). The kernel creates them for
// subnet management and general services, not on behalf of a job, so they say
// nothing about who is using the port and must not count as a user.
func (q QP) IsManagement() bool {
	return q.Type == "SMI" || q.Type == "GSI"
}

type rawQP struct {
	IfName string  `json:"ifname"`
	Port   *int    `json:"port"`
	LQPN   uint32  `json:"lqpn"`
	Type   string  `json:"type"`
	State  string  `json:"state"`
	PID    *int    `json:"pid"`
	Comm   *string `json:"comm"`
}

// Parse reads `rdma -j resource show qp` output. It accepts one JSON array
// of QP objects, several arrays back to back, or bare objects, because
// iproute2's JSON framing has varied between versions and a parser that
// guesses wrong would silently return no QPs.
func Parse(r io.Reader) ([]QP, error) {
	dec := json.NewDecoder(r)
	var out []QP
	for {
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("parse rdma resource output: %w", err)
		}
		v = bytes.TrimSpace(v)
		var raws []rawQP
		switch {
		case len(v) > 0 && v[0] == '[':
			if err := json.Unmarshal(v, &raws); err != nil {
				return nil, fmt.Errorf("parse rdma resource output: %w", err)
			}
		case len(v) > 0 && v[0] == '{':
			var one rawQP
			if err := json.Unmarshal(v, &one); err != nil {
				return nil, fmt.Errorf("parse rdma resource output: %w", err)
			}
			raws = append(raws, one)
		default:
			return nil, fmt.Errorf("parse rdma resource output: unexpected JSON value %.20q", v)
		}
		for _, rq := range raws {
			if rq.IfName == "" {
				return nil, fmt.Errorf("parse rdma resource output: QP without ifname")
			}
			q := QP{Device: rq.IfName, LQPN: rq.LQPN, Type: rq.Type, State: rq.State}
			if rq.Port != nil {
				q.Port = *rq.Port
			}
			if rq.Comm != nil {
				q.Comm = *rq.Comm
			}
			// print_comm and res_qp_line: a user QP carries "pid"; a kernel
			// QP carries only "comm" (its kernel name).
			if rq.PID != nil {
				q.PID = *rq.PID
			} else {
				q.Kernel = true
			}
			out = append(out, q)
		}
	}
	return out, nil
}

// Source lists QPs.
type Source interface {
	QPs(ctx context.Context) ([]QP, error)
}

// Command runs the iproute2 `rdma` tool.
type Command struct {
	// Path to the rdma binary. Absolute, so a root exporter never resolves
	// a binary through PATH.
	Path    string
	Timeout time.Duration
}

// QPs runs `rdma -j resource show qp` and parses it.
func (c Command) QPs(ctx context.Context) ([]QP, error) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, c.Path, "-j", "resource", "show", "qp").Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) && len(ee.Stderr) > 0 {
			return nil, fmt.Errorf("%s -j resource show qp: %w: %s", c.Path, err, bytes.TrimSpace(ee.Stderr))
		}
		return nil, fmt.Errorf("%s -j resource show qp: %w", c.Path, err)
	}
	return Parse(bytes.NewReader(out))
}
