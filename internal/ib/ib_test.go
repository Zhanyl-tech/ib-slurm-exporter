package ib_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/fixture"
	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/ib"
)

// All trees here are synthetic (see package fixture).

func TestWidthsMatchKernelPMAAttributes(t *testing.T) {
	// Spot-check against PORT_PMA_ATTR in drivers/infiniband/core/sysfs.c.
	for name, want := range map[string]uint{
		"symbol_error":                16,
		"link_error_recovery":         8,
		"link_downed":                 8,
		"local_link_integrity_errors": 4,
		"port_xmit_wait":              32,
	} {
		if got, ok := ib.Width(name); !ok || got != want {
			t.Errorf("Width(%s) = %d,%v; want %d", name, got, ok, want)
		}
	}
	// Data/packet counters are 32- or 64-bit depending on the HCA.
	for _, name := range []string{"port_xmit_data", "port_rcv_packets", "packet_seq_err"} {
		if _, ok := ib.Width(name); ok {
			t.Errorf("%s has no fixed width and must not claim one", name)
		}
	}
}

func TestSaturated(t *testing.T) {
	cases := []struct {
		name       string
		v          uint64
		sat, known bool
	}{
		{"link_downed", 255, true, true},
		{"link_downed", 254, false, true},
		{"local_link_integrity_errors", 15, true, true},
		{"symbol_error", 65535, true, true},
		{"symbol_error", 0, false, true},
		{"port_xmit_data", 1<<32 - 1, false, false},
	}
	for _, c := range cases {
		sat, known := ib.Saturated(c.name, c.v)
		if sat != c.sat || known != c.known {
			t.Errorf("Saturated(%s,%d) = %v,%v; want %v,%v", c.name, c.v, sat, known, c.sat, c.known)
		}
	}
}

func TestSaturatedCounterReadsTheSameEveryTime(t *testing.T) {
	// A saturated 8-bit link_downed stays at 255, so rate() over it is 0
	// for good. The exporter cannot fix that; it can say so.
	spec := fixture.Default(fixture.V2)
	spec.Counters["mlx5_1:1"]["link_downed"] = 255
	roots, err := fixture.Build(t.TempDir(), spec)
	if err != nil {
		t.Fatal(err)
	}
	r := ib.NewReader(roots.Sys)
	for i := 0; i < 2; i++ {
		c, err := r.Read("mlx5_1", 1)
		if err != nil {
			t.Fatal(err)
		}
		if sat, _ := ib.Saturated("link_downed", c.Values["link_downed"]); !sat {
			t.Fatalf("read %d: link_downed=%d should be flagged saturated", i, c.Values["link_downed"])
		}
	}
}

func TestMissingCountersAreAbsentNotZero(t *testing.T) {
	// A counter this kernel does not expose and a counter that is genuinely
	// zero are different facts. Defaulting the former to 0 invents data.
	roots, err := fixture.Build(t.TempDir(), fixture.Default(fixture.V2))
	if err != nil {
		t.Fatal(err)
	}
	c, err := ib.NewReader(roots.Sys).Read("mlx5_0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := c.Values["np_cnp_sent"]; present {
		t.Error("mlx5_0 has no np_cnp_sent in the fixture; it must be absent, not 0")
	}
	if v, present := c.Values["port_xmit_discards"]; !present || v != 0 {
		t.Error("a real zero must be present with value 0")
	}
}

func TestUnparseableCounterIsAbsent(t *testing.T) {
	roots, err := fixture.Build(t.TempDir(), fixture.Default(fixture.V2))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.CounterPath(roots, "mlx5_0", 1, "packet_seq_err"), []byte("N/A\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c, _ := ib.NewReader(roots.Sys).Read("mlx5_0", 1)
	if _, present := c.Values["packet_seq_err"]; present {
		t.Error(`"N/A" must be absent, not 0`)
	}
}

func TestRoCEPortCarriesCongestionCounters(t *testing.T) {
	roots, err := fixture.Build(t.TempDir(), fixture.Default(fixture.V2))
	if err != nil {
		t.Fatal(err)
	}
	c, err := ib.NewReader(roots.Sys).Read("mlx5_2", 1)
	if err != nil {
		t.Fatal(err)
	}
	if c.LinkLayer != "Ethernet" {
		t.Errorf("mlx5_2 link_layer = %q, want Ethernet", c.LinkLayer)
	}
	if !c.IsActive() {
		t.Error("port state '4: ACTIVE' should parse as active")
	}
	if c.Values["np_ecn_marked_roce_packets"] != 441882 {
		t.Errorf("ECN marks = %d", c.Values["np_ecn_marked_roce_packets"])
	}
	if c.Values["rp_cnp_ignored"] != 3 || c.Values["roce_adp_retrans"] != 17 {
		t.Errorf("rp_cnp_ignored/roce_adp_retrans not read: %v", c.Values)
	}
}

func TestPauseCountersAreNotRead(t *testing.T) {
	// mlx5 has no pause counters under hw_counters/ (they are netdev
	// counters). Even if a file with that name appears, it is not collected.
	roots, err := fixture.Build(t.TempDir(), fixture.Default(fixture.V2))
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(roots.Sys, "mlx5_2", "ports", "1", "hw_counters", "rx_pause")
	if err := os.WriteFile(p, []byte("88412"), 0o644); err != nil {
		t.Fatal(err)
	}
	c, _ := ib.NewReader(roots.Sys).Read("mlx5_2", 1)
	if _, present := c.Values["rx_pause"]; present {
		t.Error("rx_pause is not an mlx5 RDMA hw_counter and must not be exported")
	}
	for _, n := range ib.AllCounterNames() {
		if n == "rx_pause" || n == "tx_pause" || n == "rx_pause_duration" || n == "tx_pause_duration" {
			t.Errorf("%s is listed but does not exist in mlx5 hw_counters", n)
		}
	}
}

func TestPortXmitWaitIsCollected(t *testing.T) {
	roots, err := fixture.Build(t.TempDir(), fixture.Default(fixture.V2))
	if err != nil {
		t.Fatal(err)
	}
	c, _ := ib.NewReader(roots.Sys).Read("mlx5_1", 1)
	if c.Values["port_xmit_wait"] != 88120004 {
		t.Errorf("port_xmit_wait = %d", c.Values["port_xmit_wait"])
	}
}

func TestCounterListsAreConsistent(t *testing.T) {
	seen := map[string]bool{}
	for _, n := range ib.AllCounterNames() {
		if seen[n] {
			t.Errorf("%s listed twice", n)
		}
		seen[n] = true
		if _, fixed := ib.Width(n); fixed && !ib.IsPortCounter(n) {
			t.Errorf("%s has a PMA width but is not a port counter", n)
		}
	}
}

func TestMissingRootIsAnError(t *testing.T) {
	if _, err := ib.NewReader(filepath.Join(t.TempDir(), "nope")).ReadAll(); err == nil {
		t.Fatal("an unreadable sysfs root must be an error, not an empty node")
	}
}

func TestMultiPortDevice(t *testing.T) {
	spec := fixture.Default(fixture.V2)
	spec.Ports = map[string]int{"mlx5_0": 2}
	spec.Counters["mlx5_0:2"] = map[string]uint64{"port_xmit_data": 777}
	roots, err := fixture.Build(t.TempDir(), spec)
	if err != nil {
		t.Fatal(err)
	}
	all, err := ib.NewReader(roots.Sys).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range all {
		if c.Device == "mlx5_0" && c.Port == 2 && c.Values["port_xmit_data"] == 777 {
			found = true
		}
	}
	if !found {
		t.Fatal("port 2 of mlx5_0 not read")
	}
}

// ── IPoIB interfaces ───────────────────────────────────────────────────────

func ipoibByName(t *testing.T, r *ib.Reader) map[string]ib.IPoIBInterface {
	t.Helper()
	ifs, err := r.IPoIB()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]ib.IPoIBInterface{}
	for _, i := range ifs {
		out[i.Name] = i
	}
	return out
}

func TestIPoIBParentsAndChildrenAreFound(t *testing.T) {
	// Layout per the kernel source (ipoib_parent_init, ipoib_get_iflink,
	// drivers/base/core.c), not captured from a node.
	spec := fixture.Default(fixture.V2)
	spec.IPoIB = append(spec.IPoIB, fixture.IPoIB{Name: "ib0.8001", Parent: "ib0", OperState: "up"})
	roots, err := fixture.Build(t.TempDir(), spec)
	if err != nil {
		t.Fatal(err)
	}
	// /sys/class/net also holds bonding_masters, a plain file.
	if err := os.WriteFile(filepath.Join(roots.Net, "bonding_masters"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	r := ib.NewReader(roots.Sys)
	if r.NetRoot != roots.Net {
		t.Fatalf("NetRoot = %s, want the sibling of the infiniband root %s", r.NetRoot, roots.Net)
	}
	got := ipoibByName(t, r)
	want := map[string]ib.IPoIBInterface{
		"ib0":      {Name: "ib0", Device: "mlx5_0", Port: 1, OperState: "down"},
		"ib1":      {Name: "ib1", Device: "mlx5_1", Port: 1, OperState: "down"},
		"ib0.8001": {Name: "ib0.8001", Device: "mlx5_0", Port: 1, OperState: "up"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v (the RoCE Ethernet netdev and lo are not IPoIB)", got, want)
	}
	for n, w := range want {
		if got[n] != w {
			t.Errorf("%s = %+v, want %+v", n, got[n], w)
		}
	}
}

func TestIPoIBDevPortIsThePortMinusOne(t *testing.T) {
	spec := fixture.Default(fixture.V2)
	spec.Ports = map[string]int{"mlx5_0": 2}
	spec.IPoIB = []fixture.IPoIB{{Name: "ib5", Device: "mlx5_0", Port: 2}}
	roots, err := fixture.Build(t.TempDir(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if got := ipoibByName(t, ib.NewReader(roots.Sys))["ib5"]; got.Port != 2 || got.Device != "mlx5_0" {
		t.Fatalf("got %+v, want mlx5_0 port 2", got)
	}
}

func TestIPoIBInterfaceWithUnknownParentIsKeptUntied(t *testing.T) {
	// An IPoIB interface that cannot be tied to an HCA is still reported,
	// with no device, so the caller can assume the worst.
	roots, err := fixture.Build(t.TempDir(), fixture.Default(fixture.V2))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(roots.Base, "sys", "devices", "virtual", "net", "ib9.8001")
	if err := fixture.NetDev(roots, dir, 32, 0, 90, 89, "up"); err != nil {
		t.Fatal(err)
	}
	got := ipoibByName(t, ib.NewReader(roots.Sys))["ib9.8001"]
	if got.Name == "" || got.Device != "" || got.Port != 0 {
		t.Fatalf("got %+v, want an untied interface", got)
	}
}

func TestIPoIBWithoutParentsDoesNotReadNetClass(t *testing.T) {
	spec := fixture.Default(fixture.V2)
	spec.IPoIB = nil
	roots, err := fixture.Build(t.TempDir(), spec)
	if err != nil {
		t.Fatal(err)
	}
	r := ib.NewReader(roots.Sys)
	r.NetRoot = filepath.Join(t.TempDir(), "missing")
	if ifs, err := r.IPoIB(); err != nil || len(ifs) != 0 {
		t.Fatalf("no IPoIB parent, so no child can exist: %v, %v", ifs, err)
	}
}

func TestIPoIBUnreadableDeviceNetIsAnError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	roots, err := fixture.Build(t.TempDir(), fixture.Default(fixture.V2))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(fixture.DeviceDir(roots, "mlx5_1"), "net")
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	if _, err := ib.NewReader(roots.Sys).IPoIB(); err == nil {
		t.Fatal("an interface list that cannot be read is incomplete evidence")
	}
}

func TestIPoIBMayCarryTraffic(t *testing.T) {
	// Documentation/networking/operstates.rst: "down" cannot transfer data;
	// "lowerlayerdown" is a stacked interface on a down one. Anything else,
	// including an unreadable state, may carry traffic.
	for state, want := range map[string]bool{
		"down": false, "lowerlayerdown": false,
		"up": true, "unknown": true, "dormant": true, "": true,
	} {
		if got := (ib.IPoIBInterface{OperState: state}).MayCarryTraffic(); got != want {
			t.Errorf("%q: %v, want %v", state, got, want)
		}
	}
}
