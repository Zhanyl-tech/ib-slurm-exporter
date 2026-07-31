# Grafana dashboard

`grafana-dashboard.json` — per-job InfiniBand/RoCE fabric health, built directly
on this exporter's metrics.

Import it in Grafana under **Dashboards → New → Import**, upload the JSON, and
pick your Prometheus data source when prompted.

The panels are ordered the way you actually debug a slow multi-node job:

1. **Attribution health** first — is the exporter even able to pin fabric
   activity to jobs right now? Unattributed jobs, unresolved SLUID allocations,
   scrape errors, shared devices. If these are lit, the per-job panels below are
   incomplete *and the dashboard says so* rather than showing misleading zeros.
2. **Per-job errors** — `packet_seq_err`, `out_of_sequence`, `rnr_nak_retry_err`
   and the receive-side errors are the leading indicator. They rise before any
   throughput chart dips, which is the whole point: *"the training job was never
   slow — the fabric was."*
3. **Per-job throughput** — tx/rx bandwidth and transmit discards, to confirm
   the errors are actually costing the job.
4. **Device level** — port state and detected cgroup layout, always present even
   when a device is shared and per-job attribution is suppressed.

Every query, label, and counter name matches the exporter source
(`internal/collector/collector.go`, `internal/ib/ib.go`), so it works against a
live target with no editing. The `$job_id` and `$device` template variables are
populated from the metrics themselves.
