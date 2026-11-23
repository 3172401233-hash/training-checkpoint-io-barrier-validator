"""Simulated local storage with page-cache semantics.

This models, in memory, the durability rules of a local POSIX
filesystem:

* ``write`` only places data in the page cache; a crash loses it.
* ``fsync(path)`` makes the file *content* durable, but the directory
  entry (the file's name) is still not guaranteed to survive a crash.
* ``dir_sync(dir)`` (fsync on the directory) makes all names created
  in that directory since the last sync durable.
* On ``crash()`` everything still pending in the page cache is lost:
  un-fsync'd content vanishes (new files disappear entirely, rewritten
  files revert to their last durable content) and un-synced directory
  entries are dropped.

Everything is a local simulation -- no real disk, filesystem, or
network storage is touched.
"""

import posixpath


def _split(path):
    directory, name = posixpath.split(path)
    return directory or "/", name


class SimulatedStorage(object):
    def __init__(self):
        self._durable_content = {}   # path -> bytes, survives crash
        self._pending_content = {}   # path -> bytes, page cache only
        self._durable_entries = {}   # dir -> set(names), survives crash
        self._pending_entries = {}   # dir -> set(names), page cache only

    # -- operations ---------------------------------------------------
    def write(self, path, data):
        self._pending_content[path] = data
        directory, name = _split(path)
        if name not in self._durable_entries.get(directory, ()):
            self._pending_entries.setdefault(directory, set()).add(name)

    def fsync(self, path):
        """Flush pending content of one file to durable storage."""
        if path in self._pending_content:
            self._durable_content[path] = self._pending_content.pop(path)
            return True
        return path in self._durable_content

    def dir_sync(self, directory):
        """Flush pending directory entries of one directory."""
        pending = self._pending_entries.pop(directory, set())
        self._durable_entries.setdefault(directory, set()).update(pending)

    def crash(self):
        """Simulate a process/machine crash: page cache is lost."""
        self._pending_content.clear()
        self._pending_entries.clear()

    # -- queries --------------------------------------------------------
    def content_durable(self, path):
        """File content has been fsync'd (ignores directory entry)."""
        return path in self._durable_content and path not in self._pending_content

    def entry_durable(self, path):
        """The file's name is recorded in a synced directory."""
        directory, name = _split(path)
        return name in self._durable_entries.get(directory, ())

    def is_visible(self, path):
        """Visible after recovery: durable content AND durable name."""
        return self.content_durable(path) and self.entry_durable(path)

    def visible_files(self):
        return sorted(p for p in self._durable_content if self.is_visible(p))

    def read(self, path):
        """Content as seen after recovery (None if not visible)."""
        if not self.is_visible(path):
            return None
        return self._durable_content[path]
