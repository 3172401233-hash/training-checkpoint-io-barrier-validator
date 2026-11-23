"""Unit tests for the checkpoint I/O barrier validator.

Covers: asynchronous writes lost in the page cache, missing directory
sync, rank crash before/after persistence, single-rank completion not
counting as a global barrier, and the happy path.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from checkpoint_barrier import Event as E, SimulatedStorage, validate

CKPT = "ckpt-1"
SHARD0 = "checkpoints/ckpt-1/shard-0.bin"
SHARD1 = "checkpoints/ckpt-1/shard-1.bin"
SHARDS = [SHARD0, SHARD1]


def fully_persisted_events():
    return [
        E.write_file(0, SHARD0, b"w0"),
        E.write_file(1, SHARD1, b"w1"),
        E.fsync(0, SHARD0),
        E.fsync(1, SHARD1),
        E.dir_sync(0, "checkpoints/ckpt-1"),
        E.rank_complete(0),
        E.rank_complete(1),
        E.manifest_commit(CKPT, SHARDS),
    ]


class StorageSemanticsTest(unittest.TestCase):
    def test_write_without_fsync_lost_on_crash(self):
        storage = SimulatedStorage()
        storage.write(SHARD0, b"w0")
        storage.crash()
        self.assertIsNone(storage.read(SHARD0))
        self.assertEqual(storage.visible_files(), [])

    def test_fsync_without_dir_sync_not_visible(self):
        storage = SimulatedStorage()
        storage.write(SHARD0, b"w0")
        storage.fsync(SHARD0)
        storage.crash()
        self.assertTrue(storage.content_durable(SHARD0))
        self.assertFalse(storage.is_visible(SHARD0))

    def test_fsync_and_dir_sync_visible(self):
        storage = SimulatedStorage()
        storage.write(SHARD0, b"w0")
        storage.fsync(SHARD0)
        storage.dir_sync("checkpoints/ckpt-1")
        storage.crash()
        self.assertEqual(storage.read(SHARD0), b"w0")


class AsyncWriteTest(unittest.TestCase):
    def test_async_write_not_persisted_before_crash(self):
        events = fully_persisted_events()
        # rank 1 never fsyncs: shard-1 stays in the page cache.
        events = [e for e in events
                  if not (e.kind == "fsync" and e.path == SHARD1)]
        events.append(E.rank_crash(1))
        report = validate(events, expected_ranks={0, 1})
        self.assertNotIn(CKPT, report.publishable_checkpoints)
        self.assertIn(SHARD1, report.unpersisted_files)
        self.assertNotIn(SHARD0, report.unpersisted_files)
        self.assertTrue(any("rewrite %s" % SHARD1 in step
                            for step in report.retry_steps))
        self.assertTrue(any("re-commit manifest" in step
                            for step in report.retry_steps))


class MissingDirSyncTest(unittest.TestCase):
    def test_missing_dir_sync_blocks_publish(self):
        events = [e for e in fully_persisted_events()
                  if e.kind != "dir_sync"]
        events.append(E.rank_crash(0))
        report = validate(events, expected_ranks={0, 1})
        self.assertEqual(report.publishable_checkpoints, [])
        self.assertEqual(report.unpersisted_files, sorted(SHARDS))
        self.assertTrue(any("fsync directory" in step
                            for step in report.retry_steps))


class RankCrashTest(unittest.TestCase):
    def test_rank_crash_before_fsync_loses_shard(self):
        events = [
            E.write_file(0, SHARD0, b"w0"),
            E.write_file(1, SHARD1, b"w1"),
            E.fsync(0, SHARD0),
            E.dir_sync(0, "checkpoints/ckpt-1"),
            E.rank_crash(1, note="crashed before fsync"),
            E.rank_complete(0),
            E.manifest_commit(CKPT, SHARDS),
        ]
        report = validate(events, expected_ranks={0, 1})
        self.assertEqual(report.publishable_checkpoints, [])
        self.assertIn(SHARD1, report.unpersisted_files)
        self.assertIn(SHARD0, report.visible_after_recovery)

    def test_crash_after_full_persistence_keeps_checkpoint(self):
        events = fully_persisted_events()
        events.append(E.rank_crash(0))
        report = validate(events, expected_ranks={0, 1})
        self.assertEqual(report.publishable_checkpoints, [CKPT])
        self.assertEqual(report.unpersisted_files, [])
        self.assertEqual(report.retry_steps, [])


class GlobalBarrierTest(unittest.TestCase):
    def test_single_rank_complete_is_not_a_barrier(self):
        events = [
            E.write_file(0, SHARD0, b"w0"),
            E.fsync(0, SHARD0),
            E.dir_sync(0, "checkpoints/ckpt-1"),
            E.rank_complete(0),
            # rank 1 never completes; manifest committed anyway.
            E.manifest_commit(CKPT, [SHARD0]),
        ]
        report = validate(events, expected_ranks={0, 1})
        self.assertEqual(report.publishable_checkpoints, [])
        self.assertTrue(any("global barrier" in v for v in report.violations))

    def test_manifest_before_dependency_durable_is_violation(self):
        events = [
            E.write_file(0, SHARD0, b"w0"),
            E.rank_complete(0),
            E.rank_complete(1),
            E.manifest_commit(CKPT, [SHARD0]),
            E.fsync(0, SHARD0),
            E.dir_sync(0, "checkpoints/ckpt-1"),
        ]
        report = validate(events, expected_ranks={0, 1})
        self.assertEqual(report.publishable_checkpoints, [])
        self.assertTrue(any("before" in v and "durable" in v
                            for v in report.violations))


class HappyPathTest(unittest.TestCase):
    def test_fully_persisted_checkpoint_is_publishable(self):
        report = validate(fully_persisted_events(), expected_ranks={0, 1})
        self.assertEqual(report.publishable_checkpoints, [CKPT])
        self.assertEqual(report.unpersisted_files, [])
        self.assertEqual(report.violations, [])


if __name__ == "__main__":
    unittest.main()
