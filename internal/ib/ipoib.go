package ib

// IPoIB interfaces, for --ownership=qp.
//
// Why the QP-ownership mode needs them: RDMA resource tracking (what
// `rdma resource show qp` lists) only sees queue pairs created through
// ib_core, where create_qp calls rdma_restrack_add
// (drivers/infiniband/core/verbs.c). mlx5 "enhanced" IPoIB does not go
// through there. When the HCA advertises the ipoib_enhanced_offloads
// capability, ipoib_intf_init gets its netdev from rdma_init_netdev
// (drivers/infiniband/ulp/ipoib/ipoib_main.c), and mlx5_core creates the
// underlay UD QP with a raw firmware command (MLX5_CMD_OP_CREATE_QP in
// drivers/net/ethernet/mellanox/mlx5/core/ipoib/ipoib.c), which has no
// restrack call. Traffic over such an interface (TCP over ib0, NFS or
// storage over IPoIB) therefore has no QP a listing could show, and would
// land on a job that looks like the port's only user.
//
// The interface itself is visible in sysfs, so it is counted instead. Where
// it lives, per the kernel source (not checked on a live node):
//
//   - ipoib_parent_init does SET_NETDEV_DEV(dev, ca->dev.parent) and sets
//     dev_port = port - 1. The driver core links a class device's "device"
//     entry to its parent and puts class devices under a glue directory
//     named after the class (drivers/base/core.c), so a parent interface
//     appears as /sys/class/infiniband/<hca>/device/net/<if>, with
//     dev_port telling the port.
//   - P_Key child interfaces are virtual devices (no SET_NETDEV_DEV in
//     ipoib_vlan.c) whose iflink is the parent's ifindex
//     (ipoib_get_iflink), so they are found in /sys/class/net by iflink.
//   - IPoIB interfaces have type ARPHRD_INFINIBAND (32, ipoib_setup_common
//     and include/uapi/linux/if_arp.h); a RoCE port's Ethernet netdev, in
//     the same device/net directory, does not.

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// arphrdInfiniband is ARPHRD_INFINIBAND from include/uapi/linux/if_arp.h,
// the value IPoIB interfaces report in /sys/class/net/<if>/type.
const arphrdInfiniband = 32

// IPoIBInterface is one IPoIB network interface.
type IPoIBInterface struct {
	Name string
	// Device is the HCA the interface is bound to. Empty when it could not
	// be tied to one (a child whose parent was not found); callers should
	// then assume it may be on any InfiniBand port.
	Device string
	// Port is 0 when dev_port could not be read; callers should then assume
	// any port of Device.
	Port int
	// OperState is /sys/class/net/<if>/operstate, "" when unreadable.
	OperState string
}

// MayCarryTraffic reports whether the interface can be moving packets.
// Documentation/networking/operstates.rst: "down" means the interface "is
// unable to transfer data on L1, f.e. ethernet is not plugged or interface
// is ADMIN down", and "lowerlayerdown" is what an interface stacked on a
// down one (such as a P_Key child) shows. Every other value, including an
// unreadable one, is treated as possibly carrying traffic.
func (i IPoIBInterface) MayCarryTraffic() bool {
	return i.OperState != "down" && i.OperState != "lowerlayerdown"
}

// IPoIB lists the IPoIB interfaces bound to this host's HCAs, parents and
// P_Key children. An error means some interface could not be inspected, so
// the list may be incomplete.
func (r *Reader) IPoIB() ([]IPoIBInterface, error) {
	devices, err := r.Devices()
	if err != nil {
		return nil, err
	}
	var out []IPoIBInterface
	var errs []error
	parents := map[string]bool{}           // by name
	byIndex := map[string]IPoIBInterface{} // parents by ifindex

	for _, d := range devices {
		dir := filepath.Join(r.Root, d, "device", "net")
		entries, err := os.ReadDir(dir)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue // no netdev on this HCA
			}
			errs = append(errs, fmt.Errorf("list network interfaces of %s: %w", d, err))
			continue
		}
		for _, e := range entries {
			p := filepath.Join(dir, e.Name())
			ok, err := isIPoIB(p)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if !ok {
				continue
			}
			i := IPoIBInterface{Name: e.Name(), Device: d, OperState: readString(filepath.Join(p, "operstate"))}
			if dp, ok := readUint(filepath.Join(p, "dev_port")); ok {
				i.Port = int(dp) + 1
			}
			out = append(out, i)
			parents[i.Name] = true
			if idx := readString(filepath.Join(p, "ifindex")); idx != "" {
				byIndex[idx] = i
			}
		}
	}

	// A child cannot exist without its parent, so with no parent there is
	// nothing more to find (and no reason to walk every veth on the host).
	if len(parents) == 0 {
		return out, errors.Join(errs...)
	}
	entries, err := os.ReadDir(r.NetRoot)
	if err != nil {
		errs = append(errs, fmt.Errorf("list %s: %w", r.NetRoot, err))
		return out, errors.Join(errs...)
	}
	for _, e := range entries {
		if parents[e.Name()] || e.Type().IsRegular() { // bonding_masters is a file
			continue
		}
		p := filepath.Join(r.NetRoot, e.Name())
		ok, err := isIPoIB(p)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !ok {
			continue
		}
		i := IPoIBInterface{Name: e.Name(), OperState: readString(filepath.Join(p, "operstate"))}
		if par, found := byIndex[readString(filepath.Join(p, "iflink"))]; found {
			i.Device, i.Port = par.Device, par.Port
		}
		out = append(out, i)
	}
	return out, errors.Join(errs...)
}

// isIPoIB reads <iface>/type. An interface that disappears while being read
// is not an error; one whose type cannot be read otherwise is.
func isIPoIB(iface string) (bool, error) {
	b, err := os.ReadFile(filepath.Join(iface, "type"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("read %s: %w", filepath.Join(iface, "type"), err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return false, fmt.Errorf("read %s: %w", filepath.Join(iface, "type"), err)
	}
	return n == arphrdInfiniband, nil
}
