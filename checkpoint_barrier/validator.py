"""Replay a checkpoint event log and enforce durability ordering.

Invariants checked
------------------
1. Manifest publication must happen *after* every dependency file is
   durable (content fsync'd AND parent directory synced). A manifest
   committed earlier is a barrier violation.
2. A single rank reporting ``rank_complete`` is NOT a global barrier:
   the manifest may only be committed once *all* expected ranks have
   completed.
3. After a crash, recovery only sees files whose content and directory
   entry were both persisted. A checkpoint is publishable only if its
   manifest and every file it lists are all visible after recovery.

The report lists publishable checkpoints, files that were never
persisted, and the retry steps needed to make the checkpoint
publishable.
"""

import posixpath
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from .storage import SimulatedStorage

MANIFEST_NAME = "manifest.json"


def manifest_path(checkpoint):
    # Manifests live in their own directory so that the manifest
    # commit's directory sync cannot accidentally persist shard
    # directory entries.
    return posixpath.join("manifests", checkpoint, MANIFEST_NAME)


@dataclass
class CheckpointState:
    checkpoint: str
    files: tuple = ()
    committed: bool = False
    deps_durable_at_commit: bool = False
    all_ranks_complete_at_commit: bool = False
    violations: List[str] = field(default_factory=list)


@dataclass
class BarrierReport:
    publishable_checkpoints: List[str] = field(default_factory=list)
    unpersisted_files: List[str] = field(default_factory=list)
    retry_steps: List[str] = field(default_factory=list)
    violations: List[str] = field(default_factory=list)
    visible_after_recovery: List[str] = field(default_factory=list)

    def as_dict(self):
        return {
            "publishable_checkpoints": list(self.publishable_checkpoints),
            "unpersisted_files": list(self.unpersisted_files),
            "retry_steps": list(self.retry_steps),
            "violations": list(self.violations),
            "visible_after_recovery": list(self.visible_after_recovery),
        }


def validate(events, expected_ranks, storage=None):
    """Replay ``events`` and return a :class:`BarrierReport`.

    ``expected_ranks`` is the set of ranks that must all complete
    before any manifest commit counts as a global barrier.
    """
    storage = storage or SimulatedStorage()
    expected = set(expected_ranks)
    completed = set()
    crashed = set()
    checkpoints = {}   # type: Dict[str, CheckpointState]
    order = []         # checkpoint ids in commit order
    report = BarrierReport()

    for event in events:
        kind = event.kind
        if kind == "write_file":
            storage.write(event.path, event.data)
        elif kind == "fsync":
            storage.fsync(event.path)
        elif kind == "dir_sync":
            storage.dir_sync(event.path)
        elif kind == "rank_complete":
            completed.add(event.rank)
        elif kind == "rank_crash":
            crashed.add(event.rank)
            completed.discard(event.rank)
            storage.crash()
        elif kind == "retry":
            # A retry rewrites the file; it still needs fsync + dir sync
            # events afterwards to become durable.
            if event.path is not None and not storage.is_visible(event.path):
                storage.write(event.path, b"")
        elif kind == "manifest_commit":
            state = CheckpointState(event.checkpoint, files=event.files,
                                    committed=True)
            # Dependency durability is evaluated *before* the manifest
            # commit performs its own fsync/dir-sync: the manifest must
            # be published strictly after all dependencies are durable.
            missing = [f for f in event.files if not storage.is_visible(f)]
            mpath = manifest_path(event.checkpoint)
            body = ("\n".join(event.files) + "\n").encode("utf-8")
            storage.write(mpath, body)
            # The framework is expected to fsync the manifest and its
            # directory as part of the commit protocol.
            storage.fsync(mpath)
            storage.dir_sync(posixpath.dirname(mpath))

            state.deps_durable_at_commit = not missing
            for f in missing:
                state.violations.append(
                    "manifest %s committed before %s was durable" % (event.checkpoint, f))
            pending_ranks = expected - completed
            state.all_ranks_complete_at_commit = not pending_ranks
            if pending_ranks:
                state.violations.append(
                    "manifest %s committed without global barrier; "
                    "ranks not complete: %s"
                    % (event.checkpoint, sorted(pending_ranks)))
            checkpoints[event.checkpoint] = state
            order.append(event.checkpoint)
        else:
            raise ValueError("unknown event kind: %r" % kind)

    # -- recovery view -------------------------------------------------
    report.visible_after_recovery = storage.visible_files()

    for ckpt_id in order:
        state = checkpoints[ckpt_id]
        report.violations.extend(state.violations)
        mpath = manifest_path(ckpt_id)
        if not state.committed:
            continue
        if not (state.deps_durable_at_commit
                and state.all_ranks_complete_at_commit):
            continue
        if not storage.is_visible(mpath):
            continue
        if all(storage.is_visible(f) for f in state.files):
            report.publishable_checkpoints.append(ckpt_id)

    # -- unpersisted files & retry plan --------------------------------
    referenced = []   # type: List[str]
    for ckpt_id in order:
        for f in checkpoints[ckpt_id].files:
            if f not in referenced:
                referenced.append(f)
    lost = [f for f in referenced if not storage.is_visible(f)]
    report.unpersisted_files = sorted(lost)

    synced_dirs = set()
    for f in lost:
        directory = posixpath.dirname(f)
        report.retry_steps.append("rewrite %s and fsync it" % f)
        if directory not in synced_dirs:
            synced_dirs.add(directory)
            report.retry_steps.append("fsync directory %s" % directory)
    if lost:
        for ckpt_id in order:
            state = checkpoints[ckpt_id]
            if any(f in lost for f in state.files):
                report.retry_steps.append(
                    "re-commit manifest for checkpoint %s only after all "
                    "dependency files are durable" % ckpt_id)

    return report
