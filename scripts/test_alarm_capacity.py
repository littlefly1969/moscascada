#!/usr/bin/env python3
"""VS-005 T15 / VS-006 §7: tests of the alarm capacity model (scripts/alarm_capacity.py)."""
import io
import json
import math
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import alarm_capacity as m  # noqa: E402

RAW = """# campaign
kind=alarm-storm label=x sink=drain interval_us=0 events_s=150000 storm60=PASS safety=PASS
kind=alarm-storm label=y sink=drain interval_us=0 events_s=90000 storm60=PASS safety=PASS
kind=alarm-storm label=z sink=drain interval_us=0 events_s=5 storm60=FAIL safety=PASS
kind=alarm-storm label=t sink=drain interval_us=1000 events_s=7 storm60=PASS safety=PASS
kind=alarm-storm label=s sink=stopped interval_us=0 events_s=999999 storm60=FAIL safety=PASS
kind=alarms-core case=storm n=1 ops_s=1 events_s=2500000 p50_ns=1
kind=alarms-cold rules=100 per_rule_b=192.7
kind=alarms-cold rules=1000 per_rule_b=150.3
"""

JOURNAL = """# vs006
kind=journal label=E0_disk durability=FULL fs=/dev/vda:ext4 events_per_op=0 events=0 commit_max=1024 ops_per_s=120000 events_per_s=0 fsync_per_s=110 bytes_per_event=0.0 bytes_per_op=60.0 event_table_bytes=n/a write_amplification=2.0 checkpoint_pause_us_p99=900 checkpoint_write_us_max=5000
kind=journal label=E1_disk durability=FULL fs=/dev/vda:ext4 events_per_op=1 events=1000 commit_max=1024 ops_per_s=100000 events_per_s=100000 fsync_per_s=100 bytes_per_event=150.0 bytes_per_op=150.0 event_table_bytes=50000 write_amplification=2.5 checkpoint_pause_us_p99=1000 checkpoint_write_us_max=6000
kind=journal label=E1_disk_rep durability=FULL fs=/dev/vda:ext4 events_per_op=1 events=1000 commit_max=1024 ops_per_s=90000 events_per_s=90000 fsync_per_s=90 bytes_per_event=150.0 bytes_per_op=150.0 event_table_bytes=50000 write_amplification=2.5
kind=journal label=E100_disk durability=FULL fs=/dev/vda:ext4 events_per_op=100 events=100000 commit_max=1024 ops_per_s=3000 events_per_s=300000 fsync_per_s=30 bytes_per_event=60.0 bytes_per_op=6000.0 event_table_bytes=4000000 write_amplification=3.0
kind=journal label=E1_tmpfs durability=FULL fs=tmpfs:tmpfs events_per_op=1 events=1000 commit_max=1024 ops_per_s=900000 events_per_s=900000 fsync_per_s=900 bytes_per_event=150.0 bytes_per_op=150.0 event_table_bytes=50000 write_amplification=2.5
kind=journal label=E1_NORMAL durability=NORMAL_NOT_DURABLE fs=/dev/vda:ext4 events_per_op=1 events=1000 commit_max=1024 ops_per_s=800000 events_per_s=800000 fsync_per_s=0 bytes_per_event=150.0 bytes_per_op=150.0 event_table_bytes=50000 write_amplification=2.5
kind=journal label=E1_c1 durability=FULL fs=/dev/vda:ext4 events_per_op=1 events=1000 commit_max=1 ops_per_s=500 events_per_s=500 fsync_per_s=500 bytes_per_event=150.0 bytes_per_op=150.0 event_table_bytes=50000 write_amplification=9.0
kind=journal-recovery label=E1_disk total_ops=1000000 recover_ms=850.0 replayed_ops=1000
kind=alarm-storm label=J1 sink=journal interval_us=0 events_s=70000 released_s=60000 storm60=PASS safety=PASS
kind=alarm-storm label=J3_slow sink=journal interval_us=0 events_s=7000 released_s=500 storm60=PASS safety=PASS
"""


def run(*args):
    buf = io.StringIO()
    with redirect_stdout(buf):
        o = m.main(list(args) + ["--json"])
    json.loads(buf.getvalue())  # valid JSON, no bare Infinity
    return o


class Model(unittest.TestCase):
    def setUp(self):
        fd, self.raw = tempfile.mkstemp()
        with os.fdopen(fd, "w") as f:
            f.write(RAW)

    def tearDown(self):
        os.unlink(self.raw)

    def test_levels_are_separated_and_measured_values_parsed(self):
        o = run("--bench-results", self.raw, "--outbox-slots", "1000", "--event-rate", "10")
        a = o["level_A_measured"]
        self.assertEqual(a["kernel_events_s_process_drain_pass"], 90000.0)  # min of PASS drains only
        self.assertEqual(a["kernel_events_s_in_memory_max"], 2500000.0)
        self.assertEqual(a["per_rule_bytes_measured"], 192.7)
        self.assertEqual(a["event_record_bytes"], 48)
        self.assertEqual(o["level_B_assumed"]["durable_rate_per_s_vs005"], 0.0)

    def test_mu_zero_time_to_full_and_storm(self):
        o = run("--outbox-slots", "1000", "--event-rate", "10", "--storm-seconds", "60", "--reserve", "100")
        s = o["level_C_computed"]["vs005_mu_0"]
        self.assertEqual(o["level_C_computed"]["usable_slots"], 900)
        self.assertAlmostEqual(s["time_to_full_s"], 90.0)
        self.assertEqual(s["required_slots_for_storm"], 100 + 600)
        self.assertEqual(s["storm_verdict"], "PASS")
        self.assertEqual(s["backlog_drain_time_s"], 0.0)
        self.assertNotIn("vs006_projection", o["level_C_computed"])
        o = run("--outbox-slots", "1000", "--event-rate", "20", "--storm-seconds", "60")
        self.assertEqual(o["level_C_computed"]["vs005_mu_0"]["storm_verdict"], "FAIL")  # 1200 > 1000

    def test_boundary_exact_capacity_passes(self):
        o = run("--outbox-slots", "600", "--event-rate", "10", "--storm-seconds", "60")
        self.assertEqual(o["level_C_computed"]["vs005_mu_0"]["storm_verdict"], "PASS")
        o = run("--outbox-slots", "599", "--event-rate", "10", "--storm-seconds", "60")
        self.assertEqual(o["level_C_computed"]["vs005_mu_0"]["storm_verdict"], "FAIL")

    def test_projection_with_durable_rate_is_labelled(self):
        o = run("--outbox-slots", "1000", "--event-rate", "10", "--durable-rate", "15", "--initial-backlog", "500")
        p = o["level_C_computed"]["vs006_projection"]
        self.assertEqual(p["net_growth_per_s"], -5.0)
        self.assertTrue(math.isinf(p["time_to_full_s"]))
        self.assertAlmostEqual(p["backlog_drain_time_s"], 100.0)  # 500 / (15 - 10)
        v = o["level_C_computed"]["vs005_mu_0"]
        self.assertTrue(math.isinf(v["backlog_drain_time_s"]), "VS-005 never drains")
        self.assertTrue(any("PROJECTION" in w for w in o["warnings"]))

    def test_ram_budget_limits_slots(self):
        o = run("--ram-budget-mib", "1", "--rules", "0", "--event-rate", "1")
        self.assertEqual(o["level_C_computed"]["slots"], (1 << 20) // 48)
        o = run("--ram-budget-mib", "1", "--outbox-slots", "100", "--event-rate", "1")
        self.assertEqual(o["level_C_computed"]["slots"], 100)
        o = run("--bench-results", self.raw, "--ram-budget-mib", "1", "--rules", "1000", "--event-rate", "1")
        self.assertEqual(o["level_C_computed"]["slots"], int(((1 << 20) - 192.7 * 1000) // 48))

    def test_burst_profile_uses_max_prefix_sum(self):
        self.assertEqual(m.max_prefix_excess([5, 5, -100, 50], 0), 10.0)
        self.assertEqual(m.max_prefix_excess([100, 0, 0, 100], 10), 160.0)  # 90-10-10+90
        self.assertEqual(m.max_prefix_excess([1, 1], 5), 0.0)
        fd, prof = tempfile.mkstemp()
        with os.fdopen(fd, "w") as f:
            f.write("100, 0, 0, 100\n")
        try:
            o = run("--outbox-slots", "180", "--event-rate", "1", "--durable-rate", "10", "--burst-profile", prof,
                    "--reserve", "10")
            p = o["level_C_computed"]["vs006_projection"]
            self.assertEqual(p["required_slots_for_burst"], 170)
            self.assertEqual(p["burst_verdict"], "PASS")
            v = o["level_C_computed"]["vs005_mu_0"]
            self.assertEqual(v["required_slots_for_burst"], 210)
            self.assertEqual(v["burst_verdict"], "FAIL")
        finally:
            os.unlink(prof)

    def test_warnings(self):
        o = run("--bench-results", self.raw, "--outbox-slots", "10", "--event-rate", "100000", "--reserve", "11")
        w = " ".join(o["warnings"])
        self.assertIn("exceeds the measured sustained kernel rate", w)
        self.assertIn("first transition is refused", w)
        self.assertIn("NON PRODUCTION-READY", o["limitation"])

    def test_vs006_journal_level_a_excludes_tmpfs_normal_and_uses_the_conservative_cell(self):
        fd, raw = tempfile.mkstemp()
        with os.fdopen(fd, "w") as f:
            f.write(JOURNAL)
        try:
            o = run("--bench-results", raw, "--outbox-slots", "65536", "--event-rate", "1000", "--events-per-op", "10",
                    "--disk-free-mib", "1280", "--min-disk-free-mib", "256", "--storm-seconds", "600")
        finally:
            os.unlink(raw)
        a = o["level_A_measured"]
        # events_per_op=10: the closest measured fan-out not above it is E1; the slowest repetition.
        self.assertEqual(a["journal_cell_used"], "E1_disk_rep")
        self.assertEqual(a["journal_events_s_in_process"], 90000.0)
        self.assertEqual(sorted(a["journal_cells_ignored_not_durable_or_tmpfs"]), ["E1_NORMAL", "E1_tmpfs"])
        # mu = min(process storm PASS (slow/stall excluded), in-process) = 60000.
        self.assertEqual(a["journal_events_s_process_storm_pass"], 60000.0)
        self.assertEqual(a["mu_events_s_measured"], 60000.0)
        self.assertEqual(a["recovery_ms_worst"], 850.0)
        c = o["level_C_computed"]
        self.assertTrue(math.isinf(c["vs006_journal_measured"]["time_to_full_s"]))  # 1000 < 60000
        self.assertEqual(c["vs006_journal_measured"]["storm_verdict"], "PASS")
        self.assertEqual(c["vs005_mu_0"]["storm_verdict"], "FAIL")  # 600 000 events > 65 536 slots with mu = 0
        j = c["journal"]
        # ops_rate = 1000 / 10 = 100/s; growth = 100*60 + 1000*50 = 56000 B/s.
        self.assertAlmostEqual(j["disk_growth_bytes_s"], 56000.0)
        self.assertAlmostEqual(j["time_to_disk_floor_s"], 1024 * (1 << 20) / 56000.0)
        self.assertTrue(math.isinf(j["queue_time_to_full_s"]))

    def test_vs006_rates_above_the_journal_warn(self):
        fd, raw = tempfile.mkstemp()
        with os.fdopen(fd, "w") as f:
            f.write(JOURNAL)
        try:
            o = run("--bench-results", raw, "--outbox-slots", "1000", "--event-rate", "70000", "--events-per-op", "0",
                    "--ops-rate", "130000", "--journal-queue-operations", "10000")
        finally:
            os.unlink(raw)
        w = " ".join(o["warnings"])
        self.assertIn("exceeds the measured durable journal rate", w)
        self.assertIn("JOURNAL_QUEUE_FULL", w)
        self.assertAlmostEqual(o["level_C_computed"]["journal"]["queue_time_to_full_s"], 10000 / 10000.0)
        s = o["level_C_computed"]["vs006_journal_measured"]
        self.assertAlmostEqual(s["time_to_full_s"], 1000 / 10000.0)

    def test_invalid_inputs_are_refused(self):
        for bad in (["--event-rate", "-1", "--outbox-slots", "1"], ["--event-rate", "1"],
                    ["--event-rate", "1", "--outbox-slots", "1", "--reserve", "-1"]):
            with self.assertRaises(SystemExit):
                with redirect_stdout(io.StringIO()):
                    import contextlib
                    with contextlib.redirect_stderr(io.StringIO()):
                        m.main(bad)


if __name__ == "__main__":
    unittest.main(verbosity=1)
