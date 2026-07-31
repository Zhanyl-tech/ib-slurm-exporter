# ib-slurm-exporter

Correlate InfiniBand and RoCE counters with the Slurm job that owns them, so a
slow multi-node training run can be traced to the fabric instead of guessed at.

```
make demo
```

No InfiniBand, no cluster, no Linux required. Builds a synthetic `/sys`,
`/proc` and cgroup tree and serves real metrics off it.

---

## The problem

A 64-node training run drops from 4,200 to 900 samples/sec. The GPUs look busy.
Slurm says the job is `RUNNING`. Your Slurm exporter reports job count, state,
and runtime — none of which move.

The actual cause is one HCA retransmitting: `packet_seq_err` climbing on
`mlx5_1`, which stalls the all-reduce and idles all 512 GPUs behind it.

Your fabric monitoring can see that counter. It just cannot tell you which job
owns it, because nothing connects a Slurm job ID to a network device.

## How the join works

Neither Slurm nor the HCA knows about the other. The bridge is a file
descriptor:

```
Slurm cgroup                      /proc                     sysfs
─────────────                     ─────                     ─────
job_918001/step_0/cgroup.procs
        │
        └─▶ PID 41001
                  │
                  └─▶ /proc/41001/fd/3 ──▶ /dev/infiniband/uverbs0
                                                      │
                    /sys/class/infiniband_verbs/uverbs0/ibdev ──▶ "mlx5_0"
                                                                      │
                        /sys/class/infiniband/mlx5_0/ports/1/hw_counters/
                                                                      │
                                                              packet_seq_err
```

Any process doing RDMA holds an open descriptor on a uverbs device. That is the
only durable link between a Slurm job and an HCA.

The last hop goes through sysfs deliberately. `uverbs3` does **not** have to
mean `mlx5_3` — the numbering is independent, and a host where they happen to
match is exactly the host where a shortcut looks correct and is wrong elsewhere.

## The honest part: what cannot be attributed

**InfiniBand counters are per-device. The HCA counts packets; it does not know
which process sent them.**

So when two jobs share a node and both use `mlx5_1`, there is no way to split
`port_xmit_data` between them. That information does not exist in the hardware.

Exporters that ignore this emit job-labelled series that are wrong in the worst
possible way: a noisy neighbour's retries land on a healthy job, so the metric
misleads you precisely when you are using it to debug an incident.

This exporter emits two families instead:

| Family | Labelled with a job? | When |
| --- | --- | --- |
| `ib_slurm_job_*` | yes | **Only** when the job is the device's sole user |
| `ib_slurm_device_*` | no | Always. Ground truth. |

Plus `ib_slurm_device_jobs`, so a dashboard can show *why* attribution is
missing rather than leaving a mysterious gap.

Suppressing attribution never loses data. In the demo, `mlx5_1` is shared by two
jobs and its 48,221 `packet_seq_err` are still exported — just at device level,
where they are true.

## Slurm 26.05 changed the cgroup layout

From the 26.05 release notes:

> cgroup/v2 directory structures are now keyed off of SLUID and not the JobId.

Anything doing `strconv.Atoi` on the segment after `job_` now gets a parse error
or, worse, a number that is not a job ID. It does not error at runtime — the
series just stop appearing, or appear against the wrong job.

Three layouts are handled:

| Layout | Path |
| --- | --- |
| cgroup v1 | `/sys/fs/cgroup/<ctrl>/slurm/uid_<uid>/job_<jobid>/step_<n>/` |
| cgroup v2 | `/sys/fs/cgroup/system.slice/slurmstepd.scope/job_<jobid>/step_<n>/` |
| cgroup v2, 26.05+ | `…/slurmstepd.scope/job_<SLUID>/step_<n>/` |

The identifier is treated as opaque. Non-numeric ones are flagged as SLUIDs and
resolved through `scontrol`; anything that fails to resolve is **counted in
`ib_slurm_unresolved_sluid_allocations`, never guessed at**. A metric on the
wrong job is worse than a metric on no job.

> The SLUID path is implemented from the release notes and has **not** been
> validated against a live 26.05 cluster. If you run one, I would like to know
> what the real directory names look like.

One more thing that bites: PIDs live in the *step* cgroups, not the job-level
one. An exporter reading only `job_*/cgroup.procs` finds nothing and reports an
empty cluster.

## Metrics

| Metric | Type | Labels |
| --- | --- | --- |
| `ib_slurm_job_counter_total` | counter | `job_id`, `user`, `account`, `partition`, `device`, `port`, `counter` |
| `ib_slurm_device_counter_total` | counter | `device`, `port`, `counter` |
| `ib_slurm_device_jobs` | gauge | `device`, `port` |
| `ib_slurm_device_port_up` | gauge | `device`, `port`, `link_layer`, `rate` |
| `ib_slurm_job_processes` | gauge | `job_id`, `user`, `account`, `partition` |
| `ib_slurm_unattributed_jobs` | gauge | — |
| `ib_slurm_unresolved_sluid_allocations` | gauge | — |
| `ib_slurm_cgroup_layout_info` | gauge | `layout` |
| `ib_slurm_scrape_error` | gauge | — |

### Counters collected

**Port counters** — `port_xmit_data`, `port_rcv_data`, packets, discards,
`port_rcv_errors`, `link_downed`, `link_error_recovery`, `symbol_error`,
`local_link_integrity_errors`.

**mlx5 hardware counters** — this is where a stalled collective shows up:
`packet_seq_err`, `out_of_sequence`, `rnr_nak_retry_err`,
`local_ack_timeout_err`, `out_of_buffer`, `req_cqe_error`, `resp_cqe_error`.

**RoCE congestion** — `rx_pause`, `tx_pause`, `np_cnp_sent`, `rp_cnp_handled`,
`np_ecn_marked_roce_packets`.

Counters the kernel does not expose are **absent**, not zero. A counter this
firmware lacks and a counter that is genuinely zero are different facts.

### The query worth alerting on

```promql
rate(ib_slurm_job_counter_total{counter="packet_seq_err"}[5m]) > 100
```

A run whose throughput is fine but whose retries are climbing is the case this
exporter exists to catch. For shared nodes, the same query against
`ib_slurm_device_counter_total` still finds the fabric problem — you just have
to look at `ib_slurm_device_jobs` to see who is on it.

### Dashboard

A ready-to-import Grafana dashboard is in
[`deploy/grafana-dashboard.json`](deploy/grafana-dashboard.json) — attribution
health first, then per-job errors, throughput, and device state, in the order
you actually debug a slow multi-node job. Every query matches the metric and
counter names above, so it works against a live target with no editing.

## Deployment

One instance per compute node — a DaemonSet or a systemd unit. Reads only local
sysfs, `/proc` and cgroups; job metadata comes from `squeue`, cached on an
interval so a Prometheus scrape never blocks on the controller.

```bash
ib-slurm-exporter --listen :9836
ib-slurm-exporter --once            # dump the exposition and exit
ib-slurm-exporter --demo            # synthetic cluster, no hardware
```

Needs read access to other users' `/proc/<pid>/fd`, which in practice means
running as root or with `CAP_SYS_PTRACE`. Without it the PID→device mapping
comes back empty and every job lands in `ib_slurm_unattributed_jobs` — the
failure is visible rather than silent, by design.

## Limitations

- **Shared devices are never job-attributed.** By design; see above.
- **mlx5-centric.** The hardware counter names are Mellanox/NVIDIA. Other
  vendors expose different `hw_counters`; port counters are standard.
- **No per-QP breakdown.** Counters are per port, so a job using several queue
  pairs on one HCA appears as one series.
- **SLUID support is unvalidated** against a live 26.05 cluster.
- **Counters wrap.** 32-bit on older HCAs. Raw values are exported and `rate()`
  handles resets; no attempt is made to be clever about it here.

## Development

```bash
make test      # against the synthetic tree — no hardware needed
make demo      # serve the synthetic cluster on :9836
make demo-once # dump the exposition
```

Every root — sysfs, proc, verbs, cgroup — is injectable. That is what makes a
Linux-only exporter testable on a laptop, and it is why `make demo` works
without an HCA.

## The set

Part of a set of tools covering the lifecycle of a GPU allocation, each built on
the same rule — never act on absent evidence:

- **ib-slurm-exporter** — this repo. Fabric problems attributed to the job.
- **[gpu-reaper](https://github.com/Zhanyl-tech/gpu-reaper)** — wasted GPUs during a job.
- **[epilog-gpu-validator](https://github.com/Zhanyl-tech/epilog-gpu-validator)** — GPU hardware faults between jobs.
- **[slurm-scheduler-lab](https://github.com/Zhanyl-tech/slurm-scheduler-lab)** — the scheduling policy behind it all.

## License

MIT
