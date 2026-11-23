"""Event types for the checkpoint I/O barrier simulation.

An event log is an ordered list of :class:`Event` records describing
what a training framework did while writing a checkpoint:

- ``write_file``     rank wrote bytes to a checkpoint shard (page cache only)
- ``fsync``          rank flushed one file to durable storage
- ``dir_sync``       rank fsync'd a directory so new entries survive crash
- ``manifest_commit``the framework published the checkpoint manifest
- ``rank_complete``  a rank signalled it finished its shard
- ``rank_crash``     a rank (or the whole job) crashed; page cache is lost
- ``retry``          the framework retried persisting a file

The validator replays these events in order against simulated local
storage and checks the durability ordering invariants.
"""

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class Event:
    kind: str
    rank: Optional[int] = None
    path: Optional[str] = None
    data: bytes = b""
    checkpoint: Optional[str] = None
    files: Tuple[str, ...] = ()
    note: str = ""

    # -- constructors -------------------------------------------------
    @staticmethod
    def write_file(rank, path, data):
        return Event("write_file", rank=rank, path=path, data=data)

    @staticmethod
    def fsync(rank, path):
        return Event("fsync", rank=rank, path=path)

    @staticmethod
    def dir_sync(rank, directory):
        return Event("dir_sync", rank=rank, path=directory)

    @staticmethod
    def manifest_commit(checkpoint, files, rank=0):
        return Event("manifest_commit", rank=rank,
                     checkpoint=checkpoint, files=tuple(files))

    @staticmethod
    def rank_complete(rank):
        return Event("rank_complete", rank=rank)

    @staticmethod
    def rank_crash(rank, note=""):
        return Event("rank_crash", rank=rank, note=note)

    @staticmethod
    def retry(rank, path, note=""):
        return Event("retry", rank=rank, path=path, note=note)


EVENT_KINDS = (
    "write_file", "fsync", "dir_sync", "manifest_commit",
    "rank_complete", "rank_crash", "retry",
)
