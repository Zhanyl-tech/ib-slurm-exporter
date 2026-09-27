package rdmares_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/rdmares"
)

// The JSON below is hand-written to the key names printed by iproute2's
// rdma/res-qp.c and rdma/res.c. It is not captured from a real HCA. Kernel
// QP names are the KBUILD_MODNAME of the module calling ib_create_qp, read
// from the kernel source: ib_core (MAD QPs), ib_ipoib (non-enhanced IPoIB),
// rdma_cm (anything made with rdma_create_qp, e.g. NFS/RDMA).
const sample = `[
 {"ifindex":0,"ifname":"mlx5_0","port":1,"lqpn":0,"type":"SMI","state":"RTS","sq-psn":0,"comm":"ib_core"},
 {"ifindex":0,"ifname":"mlx5_0","port":1,"lqpn":1,"type":"GSI","state":"RTS","sq-psn":0,"comm":"ib_core"},
 {"ifindex":0,"ifname":"mlx5_0","port":1,"lqpn":277,"type":"UD","state":"RTS","sq-psn":1,"comm":"ib_ipoib"},
 {"ifindex":0,"ifname":"mlx5_0","port":1,"lqpn":1234,"rqpn":881,"type":"RC","state":"RTS","rq-psn":5,"sq-psn":9,"path-mig-state":"MIGRATED","pdn":3,"pid":41001,"comm":"python"},
 {"ifindex":1,"ifname":"mlx5_1","lqpn":1300,"type":"RC","state":"RESET","sq-psn":0,"pdn":4,"pid":42001,"comm":"python"}
]`

func TestParse(t *testing.T) {
	qps, err := rdmares.Parse(strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	if len(qps) != 5 {
		t.Fatalf("want 5 QPs, got %d", len(qps))
	}
	if !qps[0].IsManagement() || !qps[1].IsManagement() || qps[2].IsManagement() {
		t.Error("SMI/GSI are management QPs; UD is not")
	}
	if !qps[2].Kernel || qps[2].Comm != "ib_ipoib" || qps[2].PID != 0 {
		t.Errorf("a QP without pid is kernel-owned: %+v", qps[2])
	}
	if qps[3].Kernel || qps[3].PID != 41001 || qps[3].Port != 1 || qps[3].Device != "mlx5_0" || qps[3].Type != "RC" {
		t.Errorf("user QP misparsed: %+v", qps[3])
	}
	if qps[4].Port != 0 {
		t.Errorf("a QP with no port attribute must have Port 0, got %d", qps[4].Port)
	}
}

func TestParseAcceptsSeveralArraysAndBareObjects(t *testing.T) {
	in := `[{"ifname":"mlx5_0","port":1,"lqpn":5,"type":"RC","pid":1}]
[{"ifname":"mlx5_1","port":1,"lqpn":6,"type":"RC","pid":2}]
{"ifname":"mlx5_2","port":1,"lqpn":7,"type":"RC","comm":"rdma_cm"}`
	qps, err := rdmares.Parse(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(qps) != 3 || !qps[2].Kernel {
		t.Fatalf("got %+v", qps)
	}
}

func TestParseEmpty(t *testing.T) {
	for _, in := range []string{"", "[]", "\n"} {
		qps, err := rdmares.Parse(strings.NewReader(in))
		if err != nil || len(qps) != 0 {
			t.Errorf("%q: got %v, %v", in, qps, err)
		}
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	// A parser that returns zero QPs on unexpected input would make every
	// port look unused. Errors must surface so the collector fails safe.
	for _, in := range []string{
		"link mlx5_0/1 lqpn 1 type GSI state RTS", // text output, not -j
		`[{"port":1}]`, // no ifname
		`"just a string"`,
		`[{"ifname":"mlx5_0",`,
	} {
		if _, err := rdmares.Parse(strings.NewReader(in)); err == nil {
			t.Errorf("%q: want an error", in)
		}
	}
}

func TestCommand(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script stand-in")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "rdma")
	body := "#!/bin/sh\n" +
		`[ "$*" = "-j resource show qp" ] || { echo "unexpected args: $*" >&2; exit 2; }` + "\n" +
		"cat <<'EOF'\n" + sample + "\nEOF\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	qps, err := rdmares.Command{Path: script}.QPs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(qps) != 5 {
		t.Fatalf("got %d QPs", len(qps))
	}

	failing := filepath.Join(dir, "rdma-fail")
	if err := os.WriteFile(failing, []byte("#!/bin/sh\necho 'Operation not permitted' >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err = rdmares.Command{Path: failing}.QPs(context.Background())
	if err == nil || !strings.Contains(err.Error(), "Operation not permitted") {
		t.Fatalf("want stderr in the error, got %v", err)
	}
	if _, err := (rdmares.Command{Path: filepath.Join(dir, "missing")}).QPs(context.Background()); err == nil {
		t.Fatal("missing binary must be an error")
	}
}
