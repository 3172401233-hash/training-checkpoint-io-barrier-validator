"""Checkpoint I/O barrier validator (local storage simulation).

Replays a checkpoint event log (file writes, fsync, directory syncs,
manifest commits, rank completions, rank crashes, retries) against a
simulated local storage layer and determines which checkpoint files
are visible after recovery and which checkpoints are publishable.

All storage is simulated locally in memory; no real distributed
filesystem or object store is involved.
"""

from .events import Event
from .storage import SimulatedStorage
from .validator import validate, BarrierReport

__all__ = ["Event", "SimulatedStorage", "validate", "BarrierReport"]

__version__ = "0.1.0"
