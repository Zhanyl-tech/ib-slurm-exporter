# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Fixes from an external audit whose findings were independently re-verified.
Everything below was tested against synthetic `/sys`, `/proc` and cgroup
trees built from the kernel and Slurm documentation. **None of it has been run
on InfiniBand hardware, a Slurm 26.05 node, or a Kubernetes cluster.**

### Fixed

- **Slurm 26.05 default layout found no jobs.** On 26.05 the job cgroup is the
  bare SLUID (`slurmstepd.scope/sEKNKTV3WPV500/`) unless `CgroupJobIdPaths=yes`
  (cgroup.conf(5), cgroup_v2.html, 26.05 CHANGELOG). 0.1.0 matched only
  `job_*`, and its fixture used an invented `job_<SLUID>` shape, so the tests
  agreed with the bug. Job directories are now recognised as `job_<id>` or as
  any directory with `step_*` children; the scope's `system/` directory (the
  slurmstepd infinity process) is no longer scanned as a job root. The
  fixture now follows the documented `<SLUID>/step_0/{slurm,user/task_0}`
  shape, and a test uses the documentation's own example name.
- **SLUID resolution put slurmctld on the scrape path.** It ran
  `scontrol show job` per allocation per scrape, under the collector lock.
  SLUIDs now resolve from a cache (a SLUID's job ID never changes), then from
  the local slurmstepd process title (`slurmstepd: [<jobid>.<step>]`, as shown
  in cgroup_v2.html), then from the background squeue cache (`Sluid` field).
  If the title and squeue disagree, the SLUID stays unresolved. The scontrol
  resolver is removed.
- **An unresolved SLUID allocation could make a neighbour look like a sole
  user.** Unresolved allocations were dropped before counting device users, so
  a job sharing an HCA with one could be charged its traffic. They now count
  as users. (Found while fixing the above; not in the audit.)
- **An unreadable `/proc/<pid>/fd` failed silently.** It looked like "no
  RDMA", contrary to what the README claimed. Unreadable descriptors, PIDs
  listed in a cgroup but hidden from `/proc` (hidepid, another PID namespace),
  unresolvable uverbs nodes and unreadable cgroup subtrees are now reported
  (`ib_slurm_pid_fd_unreadable`, `ib_slurm_scrape_error{source=...}`) and
  suppress **all** job attribution for that sample, because an incomplete user
  set can make a shared port look exclusive. The docs and the log hint now say
  that a non-root exporter needs **both** `CAP_DAC_READ_SEARCH` (the fd
  directory is mode 0500) and `CAP_SYS_PTRACE` (to read its links); an earlier
  draft named only `CAP_SYS_PTRACE`. That is read from `fs/proc/fd.c` and
  `fs/proc/base.c`, not tested on a live kernel.
- **A failed cgroup scan looked like jobs ending.** Jobs missing from a scan
  that returned an error lost their accumulators (so a job series restarted
  from 0 mid-job once the error cleared) and their cached SLUIDs. State is now dropped only after
  a clean scan misses the job, or after 10 minutes missing. On cgroup v1, a
  `uid_<uid>` directory removed between listing and reading (Slurm removes it
  with the user's last job) is a job ending, not a scan error.
- **Regaining sole ownership charged the job for the shared period.** Job
  series were the port's lifetime value, so `rate()` across a gap included the
  neighbour's traffic, and a new job inherited old errors. Job series now
  accumulate only increases seen between two consecutive samples in which the
  job was sole user, starting at 0, sampled on every scrape and on a
  background ticker (`--sample-interval`). A neighbour that comes and goes
  entirely between two samples is still invisible.
- **Attribution state was under-reported.** Idle ports now get
  `ib_slurm_device_jobs 0` instead of no series; `ib_slurm_job_device_attributed`
  shows partial attribution per port; `ib_slurm_cgroup_scope_found`
  separates an idle node from a wrong `--cgroup-root`.
- **Scope path was hard-coded.** The slurmstepd scope is now discovered under
  `<slice>/` and `<slice>/<slice>/` (covers `CgroupSlice` and multiple-slurmd
  `<nodename>_slurmstepd.scope`), or set with `--slurm-scope`, and reported as
  a label on `ib_slurm_cgroup_layout_info`.
- **squeue polled the whole cluster in lock-step and hid failures.** It is now
  `--nodelist=localhost` (configurable), every `--squeue-interval` (default 60 s,
  minimum 10 s; `0` is refused rather than running squeue back to back)
  ±20% jitter, run by absolute path only, and optional. Failures keep the
  previous cache, count in `ib_slurm_metadata_refresh_errors_total`, and are
  logged at most every ten minutes. The old-format fallback for the `Sluid`
  field is taken only when squeue's error names the field; a timeout on the
  `Sluid` query is an ordinary failure, not "unsupported for an hour". Per the
  slurm-25.05 source, an older squeue prints an empty column for an unknown
  field instead of failing. Whether `--nodelist` reduces slurmctld work
  (rather than only output) is unverified.
- **Counter documentation was wrong.** PMA error counters are 4-, 8- and
  16-bit (`PORT_PMA_ATTR` in `drivers/infiniband/core/sysfs.c`), not "32-bit on
  older hardware", and a saturated one makes `rate()` read 0.
  `ib_slurm_device_counter_saturated` flags it. The mlx5 Q counters are
  32-bit and wrap; that is now documented.
- **`--once` was not exposition format** and mixed log lines into stdout. It
  now prints the `ib_slurm_*` families in Prometheus text format with HELP and
  TYPE, and all logs go to stderr.
- **The CI demo job could not fail**: `make demo-layouts` ended in `done; echo`.
  `make demo-check` (`scripts/demo-check.sh`) now asserts the documented lines
  for every layout and ownership mode and fails on a non-zero exit.
- `go.sum` was incomplete (`go mod tidy`); four files were not gofmt-formatted.

### Changed (breaking)

- `ib_slurm_job_counter_total` is now the accumulated sole-user increase
  described above, not the port's lifetime counter.
- `ib_slurm_scrape_error` has a `source` label (`sysfs`, `cgroup`, `procfd`,
  `rdma`); `ib_slurm_cgroup_layout_info` has a `scope` label;
  `ib_slurm_device_jobs` is per port and includes unresolved SLUID allocations.
- Removed `rx_pause`, `tx_pause`, `rx_pause_duration`, `tx_pause_duration`:
  mlx5 has no such RDMA `hw_counters` (they are netdev/ethtool counters), so
  they could never appear on real hardware.
- Removed the unused `ib.IsErrorCounter` and `Counters.IsRoCE`.
- `--squeue-path` defaults to `/usr/bin/squeue` and must be absolute; the
  metadata interval is a flag with default 60 s (was a fixed 30 s).

### Added

- `--ownership=qp`: device users from the kernel's RDMA resource tracking
  (`rdma -j resource show qp`), per port, counting kernel-owned and non-Slurm
  QPs as non-job users (`ib_slurm_device_nonjob_users`) and ignoring SMI/GSI.
  Resource tracking only lists QPs created through ib_core, so mlx5 enhanced
  IPoIB (whose QP mlx5_core creates with a firmware command) is covered by
  counting every IPoIB interface that is not down, found in sysfs, as a
  non-job user of its port. DEVX QPs (UCX's default on mlx5) are not detected;
  that is documented as the known fail-open case. An earlier draft said IPoIB
  was counted through its QP. Parser keys and QP type strings are taken from
  iproute2's source, the IPoIB and DEVX behaviour from kernel and UCX source;
  none of it has been run against an HCA.
- Counters `port_xmit_wait`, `req_transport_retries_exceeded`,
  `rp_cnp_ignored`, `roce_adp_retrans` (names from the kernel sources).
- `ib_slurm_job_info`, `ib_slurm_metadata_last_success_timestamp_seconds`,
  `ib_slurm_metadata_jobs`.
- Flags `--slurm-scope`, `--squeue-nodelist`, `--squeue-interval`,
  `--sample-interval`, `--no-user-labels`, `--web.max-requests` (the handler
  also has a 30 s timeout).
- Tests for every package, including failure paths (EACCES on `/proc` and on
  cgroups, hidden PIDs, unresolvable SLUIDs, QP listing failure, squeue
  failure and fallback), promlint (only the two known naming problems are
  allowed), `--once` output parsed as exposition, the HTTP in-flight limit,
  and a check that every dashboard query uses real metric names, selector,
  `by()`, `label_values()` and legend labels, and counter names. Key
  regressions were mutation-checked (the fix reverted in a scratch copy, a
  test seen failing), but not every fix was: an audit found that reverting the
  cgroup part of the fail-safe broke no test, so tests for an unreadable SLUID
  job directory (`fd` mode) and an unreadable cgroup in `qp` mode were added
  and that revert is now caught. The fixes from that audit round (cgroup-error
  pruning, the v1 `uid_*` race, IPoIB counting, the squeue interval floor and
  `Sluid` probe, cause-based re-logging) were each reverted in a scratch copy
  and a test failed; a selector-label typo and an aggregation without `by`
  injected into the dashboard were caught the same way.
- CI: gofmt, `go mod tidy -diff`, staticcheck, read-only `permissions`,
  actions pinned by commit SHA, the demo check, and a container build job
  (`image`; it has not run yet, so the Dockerfile has never been built). A
  tag-triggered release workflow for static Linux binaries (not yet run).
- `deploy/systemd`, `deploy/kubernetes/daemonset.yaml` and a `Dockerfile` —
  none run on a real host or cluster.
- Dashboard: `$node` variable, `instance` kept in every per-series
  aggregation (the six attribution-health stats are totals across the
  selected nodes, and say so), a device-level error panel, `or vector(0)` on
  counts, and panels for the new health metrics.
- Error logging re-logs a failing source when its cause changes (digits such
  as PIDs are ignored, so process churn does not defeat the ten-minute rate
  limit), as its comment already claimed.

### Documentation

- The opening scenario is labelled as illustrative with hypothetical numbers.
- "The only durable link" / "nothing connects a Slurm job ID to a network
  device" replaced with what the exporter does: per-HCA (or per-port)
  attribution on the compute node with shared-device suppression.
- "That information does not exist in the hardware" narrowed to PMA port
  counters; the README now explains that mlx5 Q counters can be bound per PID
  or QP (rdma-statistic(8)) and that this exporter does not use that yet.
- Counter units and widths, the ×4 on the data counters, security exposure,
  the default-port clash, related work, and an explicit not-run-on-hardware
  status. The dashboard is described as name-checked in CI, not as verified
  against a live target.
- Corrections from a second audit: a counter reset is no longer called
  "exact for a clear" (increments between the last sample and the clear are
  lost); the join-chain diagram no longer says flatly that two jobs on one
  HCA "cannot be split" (Q counters could be, through counter binding); the
  Dockerfile is no longer described as built and smoke-tested (the CI job
  that would do it has not run); two dashboard panel descriptions now match
  what the panels show.

### Not done

Listed so they are not mistaken for done: TLS/basic auth (keeps the dependency
surface to `client_golang`), per-PID/QP Q-counter binding, netdev pause
counters, splitting `*_counter_total` into typed families and moving metadata
labels to `ib_slurm_job_info` only (breaking; planned before 1.0), deriving
metadata without squeue, golden trees from real hardware.

## [0.1.0]

First public release.

Attribute InfiniBand/RoCE fabric counters to the Slurm job responsible.

- Core tool implemented and covered by tests.
- `make demo` (or equivalent) runs against a synthetic backend, no special
  hardware required.
- CI runs the test suite on pushes to `main` and on pull requests.
  *(Corrected: this line originally said "on every push".)*

This is a `0.x` release: the behaviour is tested and the safety properties are
asserted, but flags and metric names may still change before `1.0.0`.

Known defects found later (fixed under Unreleased): it found no jobs on a
default Slurm 26.05 node, and it failed silently when it could not read other
users' `/proc/<pid>/fd`.

[Unreleased]: https://github.com/Zhanyl-tech/ib-slurm-exporter/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Zhanyl-tech/ib-slurm-exporter/releases/tag/v0.1.0
