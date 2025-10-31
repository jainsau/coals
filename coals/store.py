"""Minimal in-memory object store for zero-copy data sharing between Python processes.

Uses SharedMemory segments with Manager.dict for metadata synchronization.
Objects must be sealed before retrieval. Reference counting manages object lifetime.

Features: put/get/seal/release operations, manual eviction, capacity limits, notifications.

Limitations: single-machine only, no persistence, no security, no Arrow integration.
"""

from multiprocessing import Manager, Condition
from multiprocessing.shared_memory import SharedMemory
import uuid
import time
from typing import Dict, Any, Tuple, Optional
from collections import deque


class ObjectExists(Exception):
    """Object already exists in store"""
    pass


class ObjectNotSealed(Exception):
    """Object not sealed for reading"""
    pass


class ObjectNotFound(Exception):
    """Object not found in store"""
    pass


class Store:
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

    def __init__(self, manager=None, capacity: int = 10_000_000):
        # If a manager is provided, use it; otherwise create a Manager.
        if manager is None:
            self._manager = Manager()
            self.meta = self._manager.dict()
            self._own_manager = True
        else:
            self._manager = manager
            self.meta = self._manager.dict()
            self._own_manager = False
        self.capacity = capacity
        self._current_size = 0
        self._notification_condition = self._manager.Condition()
        self._sealed_objects_queue = self._manager.list()

    def create(self, size: int) -> Tuple[str, memoryview, SharedMemory]:
        """Allocate shared memory and return an object id and buffer view to write into."""
        if self._current_size + size > self.capacity:
            raise MemoryError("Not enough capacity in the store")

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
            "created_at": time.time(),
        }
        self._current_size += size
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
        # Notify subscribers
        with self._notification_condition:
            self._sealed_objects_queue.append(obj_id)
            self._notification_condition.notify_all()

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

    def contains(self, obj_id):
        """Check if an object exists in the store."""
        return obj_id in self.meta

    def info(self, obj_id):
        """Get metadata for a specific object."""
        md = self.meta.get(obj_id)
        if md is None:
            raise ObjectNotFound(obj_id)
        return md

    def delete(self, obj_id):
        """Immediately remove an object from the store."""
        md = self.meta.get(obj_id)
        if md is None:
            raise ObjectNotFound(obj_id)
        try:
            shm = SharedMemory(name=md["shm_name"])
            shm.close()
            shm.unlink()
        except FileNotFoundError:
            pass
        self._current_size -= md["size"]
        del self.meta[obj_id]

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
            self._current_size -= md["size"]
            del self.meta[obj_id]
        else:
            self.meta[obj_id] = md

    def list_objects(self):
        return dict(self.meta)

    def store_capacity(self) -> int:
        """Return the total capacity of the store."""
        return self.capacity

    def evict(self, num_bytes: int):
        """Manually evict objects to free up space. Evicts oldest unsealed objects first."""
        bytes_freed = 0
        # Get all objects that are not sealed and have a refcount of 1
        evictable_objects = []
        for obj_id, md in self.meta.items():
            # Only evict if only the store holds a reference and it's sealed
            # Plasma's evict typically targets sealed objects that are not in use
            if md["refcount"] == 1 and md["sealed"]:
                evictable_objects.append((obj_id, md))

        # Sort by creation time (oldest first)
        evictable_objects.sort(key=lambda x: x[1]["created_at"])

        for obj_id, md in evictable_objects:
            if bytes_freed >= num_bytes:
                break
            # Delete the object
            self.delete(obj_id)
            bytes_freed += md["size"]

    def subscribe(self):
        """Returns the notification condition for clients to wait on."""
        return self._notification_condition

    def get_notification(self, timeout: Optional[float] = None) -> Optional[str]:
        """Waits for a notification and returns the sealed object_id."""
        with self._notification_condition:
            while not self._sealed_objects_queue:
                if not self._notification_condition.wait(timeout):
                    return None # Timeout
            return self._sealed_objects_queue.pop(0)

    def shutdown(self):
        # Only shut down the manager if we own it.
        if getattr(self, "_own_manager", False):
            self._manager.shutdown()


