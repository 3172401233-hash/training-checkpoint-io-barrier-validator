"""Demo CLI: replay a buggy and a fixed checkpoint scenario.

    python -m checkpoint_barrier

Prints a JSON report with publishable checkpoints, files that were
never persisted, and the retry steps required. All storage is a local
in-memory simulation.
"""

import json

from .events import Event as E
from .validator import validate


def buggy_scenario():
    """Async multi-file write, manifest published too early, then crash."""
    return [
        E.write_file(0, "checkpoints/ckpt-1/shard-0.bin", b"weights-0"),
        E.write_file(1, "checkpoints/ckpt-1/shard-1.bin", b"weights-1"),
        E.fsync(0, "checkpoints/ckpt-1/shard-0.bin"),
        # rank 1's shard is still only in the page cache; no dir sync
        # for the checkpoint directory happened at all.
        E.rank_complete(0),
        E.rank_complete(1),
        E.manifest_commit("ckpt-1", [
            "checkpoints/ckpt-1/shard-0.bin",
            "checkpoints/ckpt-1/shard-1.bin",
        ]),
        E.rank_crash(1, note="process exited before flushing page cache"),
    ]


def fixed_scenario():
    """Every shard fsync'd, directory synced, barrier, then manifest."""
    return [
        E.write_file(0, "checkpoints/ckpt-2/shard-0.bin", b"weights-0"),
        E.write_file(1, "checkpoints/ckpt-2/shard-1.bin", b"weights-1"),
        E.fsync(0, "checkpoints/ckpt-2/shard-0.bin"),
        E.fsync(1, "checkpoints/ckpt-2/shard-1.bin"),
        E.dir_sync(0, "checkpoints/ckpt-2"),
        E.rank_complete(0),
        E.rank_complete(1),
        E.manifest_commit("ckpt-2", [
            "checkpoints/ckpt-2/shard-0.bin",
            "checkpoints/ckpt-2/shard-1.bin",
        ]),
        E.rank_crash(0, note="crash after full persistence"),
    ]


def main():
    report = {
        "buggy_scenario": validate(buggy_scenario(), expected_ranks={0, 1}).as_dict(),
        "fixed_scenario": validate(fixed_scenario(), expected_ranks={0, 1}).as_dict(),
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
