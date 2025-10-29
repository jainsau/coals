"""
Coals: A Plasma-like in-memory object store using Python's multiprocessing.shared_memory.

This is a small educational prototype — not production-ready.

Features:
- Put/get semantics using SharedMemory segments (zero-copy access by name)
- "seal" an object to mark it immutable
- reference counting stored in a Manager dict (simple cross-process visibility)

Limitations (explicit):
- No eviction, no persistence, no security, no sophisticated memory pooling
- Metadata is kept in a multiprocessing.Manager dict (works for cross-process within the same host)
- Objects are stored as raw bytes; we show how to store Arrow buffers or pickled data, but here we use bytes
"""

from multiprocessing import Manager
from multiprocessing.shared_memory import SharedMemory
import uuid
from typing import Dict, Any, Tuple, Optional


class ObjectExists(Exception):
    """Object already exists in store"""
    pass


class ObjectNotSealed(Exception):
    """Object not sealed for reading"""
    pass


class ObjectNotFound(Exception):
    """Object not found in store"""
    pass


class PlasmaStore:
    """
    Minimal Plasma-like store using SharedMemory and a Manager for metadata.
    Metadata schema (kept in Manager dict):
      metadata[obj_id_str] = {
          'shm_name': <shared memory name>,
          'size': <int>,
          'sealed': <bool>,
          'refcount': <int>
      }
    """

    def __init__(self, manager_dict=None):
        # If a manager_dict is provided, use it; otherwise create a Manager.
        if manager_dict is None:
            self._manager = Manager()
            self.meta = self._manager.dict()
            self._own_manager = True
        else:
            self.meta = manager_dict
            self._own_manager = False

    def create(self, size: int) -> Tuple[str, memoryview, SharedMemory]:
        """Allocate shared memory and return an object id and buffer view to write into."""
        obj_id = str(uuid.uuid4())
        # Use shorter name to avoid filesystem limits
        shm_name = f"coals_{obj_id[:8]}"
        shm = SharedMemory(create=True, size=size, name=shm_name)
        # initialize memory with zeros
        shm.buf[:size] = b"\x00" * size
        # store metadata
        self.meta[obj_id] = {
            "shm_name": shm_name,
            "size": size,
            "sealed": False,
            "refcount": 1,
        }
        # return (id, memoryview, SharedMemory) -- caller should .close() the SharedMemory when done writing
        return obj_id, shm.buf, shm

    def put(self, data_bytes):
        """Convenience that creates and writes bytes, then seals the object."""
        size = len(data_bytes)
        obj_id, buf, shm = self.create(size)
        buf[:size] = data_bytes
        shm.close()
        self.seal(obj_id)
        return obj_id

    def seal(self, obj_id):
        md = self.meta.get(obj_id)
        if md is None:
            raise ObjectNotFound(obj_id)
        md["sealed"] = True
        # write back to Manager dict
        self.meta[obj_id] = md

    def get(self, obj_id):
        """Return a SharedMemory object for reading and increase refcount."""
        md = self.meta.get(obj_id)
        if md is None:
            raise ObjectNotFound(obj_id)
        if not md["sealed"]:
            raise ObjectNotSealed(obj_id)
        shm = SharedMemory(name=md["shm_name"])
        # bump refcount
        md["refcount"] += 1
        self.meta[obj_id] = md
        return shm, md["size"]

    def release(self, obj_id):
        """Decrease refcount; if zero, unlink the shared memory and delete metadata."""
        md = self.meta.get(obj_id)
        if md is None:
            raise ObjectNotFound(obj_id)
        md["refcount"] -= 1
        if md["refcount"] <= 0:
            # remove from manager and unlink shared memory
            try:
                shm = SharedMemory(name=md["shm_name"])
                shm.close()
                shm.unlink()
            except FileNotFoundError:
                pass
            del self.meta[obj_id]
        else:
            self.meta[obj_id] = md

    def list_objects(self):
        return dict(self.meta)

    def shutdown(self):
        # Only shut down the manager if we own it.
        if getattr(self, "_own_manager", False):
            self._manager.shutdown()


