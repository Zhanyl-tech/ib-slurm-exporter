# Deployment artifacts

None of these has been run on a real host or cluster yet; each file says what
has and has not been checked.

| File | What it is | Checked |
| --- | --- | --- |
| `grafana-dashboard.json` | Grafana dashboard | metric names, selector and `by()` label names, `label_values()`, legends and counter names against the source, by `TestDashboardQueriesMatchTheExporter`; not imported into a live Grafana |
| `systemd/ib-slurm-exporter.service` | hardened unit, non-root with `CAP_DAC_READ_SEARCH` and `CAP_SYS_PTRACE` | not run (`systemd-analyze verify` not run either) |
| `kubernetes/daemonset.yaml` | DaemonSet, `hostPID`, read-only `/sys` | YAML syntax only |
| `../Dockerfile` | distroless image | never built: the CI `image` job is configured to build it and run `--demo --once` in it, but has not run yet |

## Grafana dashboard

`grafana-dashboard.json` — per-job InfiniBand/RoCE fabric health, built on this
exporter's metrics.

Import it in Grafana under **Dashboards → New → Import**, upload the JSON, and
pick your Prometheus data source when prompted.

The panels are ordered the way you debug a slow multi-node job:

1. **Attribution health** first — is the exporter able to pin fabric activity
   to jobs right now? Unattributed jobs, unresolved SLUID allocations, scrape
   errors, unreadable job processes, shared ports, saturated counters. If these
   are lit, the per-job panels below are incomplete *and the dashboard says so*
   rather than showing misleading zeros.
2. **Per-job errors** — `packet_seq_err`, `out_of_sequence`,
   `rnr_nak_retry_err` and the receive-side errors, counted only while the job
   was the port's sole user. In the scenario this tool is built for they are
   the leading indicator; that ordering is the design intent, not something
   measured here.
3. **Per-job throughput** — tx/rx bandwidth (the data counters ×4, since they
   are in 4-octet units), transmit discards and `port_xmit_wait`.
4. **Device level** — the same error counters for every port whether or not a
   job could be attributed, jobs per port, port state, and the detected cgroup
   layout and scope.

Every per-series panel aggregates `by (instance, …)`, and a `$node` variable
filters by node, so the node with the failing HCA stays identifiable. The six
attribution-health stats are totals across the selected nodes; narrow `$node`
to see which one is affected. `$job_id` comes from
`ib_slurm_job_device_attributed`, so jobs whose attribution is suppressed are
still selectable.
