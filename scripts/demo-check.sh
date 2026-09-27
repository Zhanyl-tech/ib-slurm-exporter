#!/bin/sh
# Asserts what the README promises about the demo, for every cgroup layout
# and ownership mode. The previous `make demo-layouts` piped into grep inside
# a for-loop ending in `echo`, so it exited 0 even when the binary crashed.
# This script exits non-zero on the first broken promise.
#
# Usage: scripts/demo-check.sh [path/to/ib-slurm-exporter]
set -eu

bin=${1:-bin/ib-slurm-exporter}
fail=0

expect_line() { # output, exact line, context
	if ! printf '%s\n' "$1" | grep -qxF -- "$2"; then
		echo "FAIL [$3]: missing line: $2" >&2
		fail=1
	fi
}

for layout in v1 v2 v2-sluid; do
	case $layout in
	v1) want_layout=cgroup-v1 scope=freezer/slurm ;;
	v2) want_layout=cgroup-v2 scope=system.slice/slurmstepd.scope ;;
	v2-sluid) want_layout=cgroup-v2-sluid scope=system.slice/slurmstepd.scope ;;
	esac
	for own in fd qp; do
		ctx="$layout/$own"
		if ! out=$("$bin" --demo --demo-layout "$layout" --ownership "$own" --once 2>/dev/null); then
			echo "FAIL [$ctx]: exporter exited non-zero" >&2
			exit 1
		fi

		expect_line "$out" "ib_slurm_cgroup_layout_info{layout=\"$want_layout\",scope=\"$scope\"} 1" "$ctx"
		expect_line "$out" 'ib_slurm_cgroup_scope_found 1' "$ctx"
		expect_line "$out" 'ib_slurm_unattributed_jobs 2' "$ctx"
		expect_line "$out" 'ib_slurm_unresolved_sluid_allocations 0' "$ctx"
		expect_line "$out" 'ib_slurm_device_jobs{device="mlx5_1",port="1"} 2' "$ctx"
		expect_line "$out" 'ib_slurm_job_device_attributed{device="mlx5_0",job_id="918001",port="1"} 1' "$ctx"
		expect_line "$out" 'ib_slurm_job_device_attributed{device="mlx5_1",job_id="918002",port="1"} 0' "$ctx"
		expect_line "$out" 'ib_slurm_scrape_error{source="sysfs"} 0' "$ctx"
		expect_line "$out" 'ib_slurm_scrape_error{source="cgroup"} 0' "$ctx"

		# The rule the whole exporter rests on: a shared port is never
		# attributed to a job.
		if printf '%s\n' "$out" | grep -q '^ib_slurm_job_counter_total{.*device="mlx5_1"'; then
			echo "FAIL [$ctx]: shared mlx5_1 was attributed to a job" >&2
			fail=1
		fi
		# Logs belong on stderr; stdout must be pure exposition.
		if printf '%s\n' "$out" | grep -q 'level='; then
			echo "FAIL [$ctx]: log lines on stdout" >&2
			fail=1
		fi

		[ "$fail" -eq 0 ] && echo "ok   [$ctx]"
	done
done

exit "$fail"
