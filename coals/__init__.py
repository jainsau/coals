"""
Coals: A Plasma-like in-memory object store using Python's multiprocessing.shared_memory.

This is a small educational prototype — not production-ready.
"""

from .store import PlasmaStore, ObjectExists, ObjectNotFound, ObjectNotSealed

__version__ = "0.1.0"
__all__ = [
    "PlasmaStore",
    "ObjectExists", 
    "ObjectNotFound",
    "ObjectNotSealed"
]
