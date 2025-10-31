# Coals

A Plasma-like in-memory object store using Python's multiprocessing.shared_memory.

This is a small educational prototype — not production-ready.

## Architecture

Coals provides zero-copy data sharing between Python processes on the same machine:
- Uses `multiprocessing.Manager.dict` for cross-process metadata synchronization
- Each object stored in a separate SharedMemory segment identified by name
- Reference counting tracks object lifetime across processes
- Condition variables provide notification mechanism for sealed objects

## Implementation Details

- Objects must be sealed before retrieval (immutability guarantee)
- Manual eviction via `evict()` method (oldest sealed objects with refcount=1)
- Capacity tracking prevents allocation beyond configured limit
- Metadata includes: shm_name, size, sealed status, refcount, created_at timestamp

## Features

- Put/get semantics using SharedMemory segments (zero-copy access by name)
- "seal" an object to mark it immutable
- Reference counting stored in a Manager dict (simple cross-process visibility)
- Basic notification system for sealed objects

## Limitations

- Single-machine only (no network/distributed support)
- No persistence (in-memory only, lost on process termination)
- No automatic eviction (manual only)
- No security/access control
- No Arrow integration (stores raw bytes)
- Synchronous API only
- Metadata kept in multiprocessing.Manager.dict (works for cross-process within same host)

## Installation

```bash
poetry install
```

## Usage

```python
from coals import Store
import pickle

# Create a store
store = Store()

# Store some data
data = pickle.dumps({"message": "hello", "value": 42})
obj_id = store.put(data)

# Retrieve the data
shm, size = store.get(obj_id)
retrieved_data = bytes(shm.buf[:size])
shm.close()

# Clean up
store.release(obj_id)
store.shutdown()
```

## Demo

Run the demo script to see it in action:

```bash
python demo.py
```
