package slurm

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Zhanyl-tech/ib-slurm-exporter/internal/collector"
)

// Output shapes follow the --Format rules in squeue(1): each field padded to
// its minimum width (20 when none is given) and followed by its suffix. The
// values are invented.

func TestParseSqueue(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		jobs   map[string]collector.JobMeta
		sluids map[string]string
		bad    int
	}{
		{
			name: "padded, with Sluid",
			in: "918001              |alice               |research            |gpu                 |sEKNKTV3WPV500      \n" +
				"918002              |bob                 |infra               |gpu                 |sFNDM35NQ39R00      \n",
			jobs: map[string]collector.JobMeta{
				"918001": {User: "alice", Account: "research", Partition: "gpu"},
				"918002": {User: "bob", Account: "infra", Partition: "gpu"},
			},
			sluids: map[string]string{"sEKNKTV3WPV500": "918001", "sFNDM35NQ39R00": "918002"},
		},
		{
			name:   "pre-26.05, no Sluid column",
			in:     "918001              |alice               |research            |gpu                 \n",
			jobs:   map[string]collector.JobMeta{"918001": {User: "alice", Account: "research", Partition: "gpu"}},
			sluids: map[string]string{},
		},
		{
			name:   "Sluid column present but empty",
			in:     "918001|alice|research|gpu|\n",
			jobs:   map[string]collector.JobMeta{"918001": {User: "alice", Account: "research", Partition: "gpu"}},
			sluids: map[string]string{},
		},
		{
			name:   "empty output",
			in:     "",
			jobs:   map[string]collector.JobMeta{},
			sluids: map[string]string{},
		},
		{
			name: "malformed and non-numeric lines are skipped, not guessed",
			in: "918001|alice|research|gpu|sA\n" +
				"garbage\n" +
				"123_4|bob|infra|gpu|sB\n" +
				"   \n",
			jobs:   map[string]collector.JobMeta{"918001": {User: "alice", Account: "research", Partition: "gpu"}},
			sluids: map[string]string{"sA": "918001"},
			bad:    2,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			jobs, sluids, bad := ParseSqueue([]byte(c.in))
			if bad != c.bad {
				t.Errorf("bad = %d, want %d", bad, c.bad)
			}
			if len(jobs) != len(c.jobs) {
				t.Fatalf("jobs = %v, want %v", jobs, c.jobs)
			}
			for k, v := range c.jobs {
				if jobs[k] != v {
					t.Errorf("jobs[%s] = %+v, want %+v", k, jobs[k], v)
				}
			}
			if len(sluids) != len(c.sluids) {
				t.Fatalf("sluids = %v, want %v", sluids, c.sluids)
			}
			for k, v := range c.sluids {
				if sluids[k] != v {
					t.Errorf("sluids[%s] = %q, want %q", k, sluids[k], v)
				}
			}
		})
	}
}

type fakeRun struct {
	mu    sync.Mutex
	calls [][]string
	fn    func(args []string) ([]byte, error)
}

func (f *fakeRun) run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.mu.Lock()
	f.calls = append(f.calls, append([]string{name}, args...))
	f.mu.Unlock()
	return f.fn(args)
}

func (f *fakeRun) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func newTest(fn func(args []string) ([]byte, error)) (*Squeue, *fakeRun, *bytes.Buffer) {
	var logs bytes.Buffer
	s := New("/usr/bin/squeue", "", slog.New(slog.NewTextHandler(&logs, nil)))
	f := &fakeRun{fn: fn}
	s.run = f.run
	return s, f, &logs
}

func hasSluid(args []string) bool {
	for _, a := range args {
		if strings.Contains(a, "Sluid") {
			return true
		}
	}
	return false
}

func TestRefreshIsNodeScopedAndAsksForSluid(t *testing.T) {
	s, f, _ := newTest(func([]string) ([]byte, error) {
		return []byte("918001|alice|research|gpu|sA\n"), nil
	})
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	args := strings.Join(f.calls[0], " ")
	if f.calls[0][0] != "/usr/bin/squeue" {
		t.Errorf("squeue must be run by absolute path, got %q", f.calls[0][0])
	}
	if !strings.Contains(args, "--nodelist=localhost") {
		t.Errorf("squeue must be node-scoped: %s", args)
	}
	if !hasSluid(f.calls[0]) {
		t.Errorf("squeue must ask for Sluid: %s", args)
	}
	if id, err := s.ResolveSLUID("sA"); err != nil || id != "918001" {
		t.Errorf("ResolveSLUID = %q, %v", id, err)
	}
	if m, ok := s.Meta("918001"); !ok || m.User != "alice" {
		t.Errorf("Meta = %+v, %v", m, ok)
	}
}

func TestOldSqueueWithoutSluidFallsBack(t *testing.T) {
	s, f, logs := newTest(func(args []string) ([]byte, error) {
		if hasSluid(args) {
			return nil, errors.New("exit status 1: Invalid job format specification: Sluid")
		}
		return []byte("918001|alice|research|gpu\n"), nil
	})
	for i := 0; i < 3; i++ {
		if err := s.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := s.Meta("918001"); !ok {
		t.Fatal("metadata must still load without Sluid")
	}
	// First refresh: Sluid attempt + fallback. Later ones skip Sluid.
	if f.count() != 4 {
		t.Errorf("want 4 squeue calls (1 probe + 3 without Sluid), got %d", f.count())
	}
	if n := strings.Count(logs.String(), "does not accept the Sluid field"); n != 1 {
		t.Errorf("fallback should be logged once, got %d\n%s", n, logs.String())
	}
	if errorsTotal(s) != 0 {
		t.Error("a successful fallback is not a refresh error")
	}
}

func TestPre2605SqueueEmptySluidColumnNeedsNoFallback(t *testing.T) {
	// What the slurm-25.05 source says an older squeue does with an unknown
	// --Format field: parse_long_format logs "Invalid job format
	// specification" to stderr and prints an empty column, and the call
	// succeeds. (Read from the source, not run.) Metadata loads, no SLUID
	// is mapped, and there is no second call and no error.
	s, f, logs := newTest(func([]string) ([]byte, error) {
		return []byte("918001              |alice               |research            |gpu                 |\n"), nil
	})
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Meta("918001"); !ok {
		t.Fatal("metadata must load")
	}
	if f.count() != 1 || errorsTotal(s) != 0 || strings.Contains(logs.String(), "Sluid field") {
		t.Errorf("calls=%d errors=%v logs=%s", f.count(), errorsTotal(s), logs.String())
	}
}

func TestTransientSluidFailureIsAnOrdinaryFailure(t *testing.T) {
	// Audit reproduction: a controller that timed out on the Sluid query and
	// answered the plain one switched SLUID lookups off for an hour, logged
	// "does not accept the Sluid field", and counted no error.
	fail := false
	s, f, logs := newTest(func(args []string) ([]byte, error) {
		if fail {
			return nil, errors.New("signal: killed")
		}
		return []byte("918001|alice|research|gpu|sA\n"), nil
	})
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	fail = true
	if err := s.Refresh(context.Background()); err == nil {
		t.Fatal("a failed Sluid query is a failed refresh")
	}
	if f.count() != 2 {
		t.Errorf("an error that does not name the field must not trigger the fallback call: %d calls", f.count())
	}
	if errorsTotal(s) != 1 {
		t.Errorf("errors_total = %v, want 1", errorsTotal(s))
	}
	if strings.Contains(logs.String(), "does not accept the Sluid field") {
		t.Error("a timeout is not squeue refusing the field")
	}
	if id, err := s.ResolveSLUID("sA"); err != nil || id != "918001" {
		t.Errorf("previous SLUID cache must be kept: %q, %v", id, err)
	}
	fail = false
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !hasSluid(f.calls[len(f.calls)-1]) {
		t.Error("the next refresh must ask for Sluid again")
	}
}

func TestFailureKeepsCacheCountsAndIsRateLimited(t *testing.T) {
	fail := false
	s, _, logs := newTest(func([]string) ([]byte, error) {
		if fail {
			return nil, errors.New("slurm_load_jobs error: Unable to contact slurm controller")
		}
		return []byte("918001|alice|research|gpu|sA\n"), nil
	})
	now := time.Unix(1_800_000_000, 0)
	s.now = func() time.Time { return now }

	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	fail = true
	for i := 0; i < 5; i++ {
		now = now.Add(time.Minute)
		if err := s.Refresh(context.Background()); err == nil {
			t.Fatal("want error")
		}
	}
	if _, ok := s.Meta("918001"); !ok {
		t.Error("a failed refresh must keep the previous cache")
	}
	if got := errorsTotal(s); got != 5 {
		t.Errorf("errors_total = %v, want 5", got)
	}
	if n := strings.Count(logs.String(), "squeue refresh failed"); n != 1 {
		t.Errorf("5 failures within 5 minutes should log once, got %d", n)
	}
	now = now.Add(11 * time.Minute)
	_ = s.Refresh(context.Background())
	if n := strings.Count(logs.String(), "squeue refresh failed"); n != 2 {
		t.Errorf("a persisting failure should log again after 10 minutes, got %d", n)
	}
	fail = false
	_ = s.Refresh(context.Background())
	if !strings.Contains(logs.String(), "recovered") {
		t.Error("recovery should be logged")
	}
}

func TestResolveNeverRunsACommand(t *testing.T) {
	s, f, _ := newTest(func([]string) ([]byte, error) {
		return []byte("918001|alice|research|gpu|sA\n"), nil
	})
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := f.count()
	for i := 0; i < 100; i++ {
		_, _ = s.ResolveSLUID("sA")
		_, _ = s.ResolveSLUID("sUnknown")
		_, _ = s.Meta("918001")
	}
	if f.count() != before {
		t.Fatalf("lookups must be served from cache; squeue ran %d more times", f.count()-before)
	}
}

func TestLastSuccessIsZeroUntilARefreshSucceeds(t *testing.T) {
	s, _, _ := newTest(func([]string) ([]byte, error) { return nil, errors.New("down") })
	_ = s.Refresh(context.Background())
	if n := testutil.CollectAndCount(s); n != 3 {
		t.Fatalf("want 3 metadata series, got %d", n)
	}
	want := `
# HELP ib_slurm_metadata_last_success_timestamp_seconds Unix time of the last successful squeue refresh; 0 if none has succeeded. Job labels are at most this old.
# TYPE ib_slurm_metadata_last_success_timestamp_seconds gauge
ib_slurm_metadata_last_success_timestamp_seconds 0
`
	if err := testutil.CollectAndCompare(s, strings.NewReader(want), "ib_slurm_metadata_last_success_timestamp_seconds"); err != nil {
		t.Fatal(err)
	}
}

func TestJitterBounds(t *testing.T) {
	if got := Jitter(time.Minute, 0); got != 48*time.Second {
		t.Errorf("lower bound = %v", got)
	}
	if got := Jitter(time.Minute, 0.999999); got >= 72*time.Second || got < 71*time.Second {
		t.Errorf("upper bound = %v", got)
	}
}

func TestRunStopsWithContext(t *testing.T) {
	s, f, _ := newTest(func([]string) ([]byte, error) { return []byte(""), nil })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx, time.Hour); close(done) }()
	deadline := time.Now().Add(2 * time.Second)
	for f.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
	if f.count() < 1 {
		t.Fatal("Run should refresh immediately")
	}
}

func TestRunNeverLoopsFasterThanTheMinimum(t *testing.T) {
	// Audit reproduction: an interval of 0 ran squeue in a tight loop, which
	// on a real cluster means every node at once.
	s, f, logs := newTest(func([]string) ([]byte, error) { return []byte(""), nil })
	for _, interval := range []time.Duration{0, -time.Second, time.Millisecond} {
		before := f.count()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { s.Run(ctx, interval); close(done) }()
		deadline := time.Now().Add(2 * time.Second)
		for f.count() == before && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
		cancel()
		<-done
		if n := f.count() - before; n != 1 {
			t.Errorf("interval %v: %d squeue calls in ~100ms, want 1 (the immediate refresh)", interval, n)
		}
	}
	if !strings.Contains(logs.String(), "below the minimum") {
		t.Error("raising the interval should be logged")
	}
}

// errorsTotal reads the refresh-error counter.
func errorsTotal(s *Squeue) float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.errorsTotal
}
