# Coals

A Plasma-like in-memory object store using Python's multiprocessing.shared_memory.

This is a small educational prototype — not production-ready.

## Features

- Put/get semantics using SharedMemory segments (zero-copy access by name)
- "seal" an object to mark it immutable
- Reference counting stored in a Manager dict (simple cross-process visibility)

## Limitations

- No eviction, no persistence, no security, no sophisticated memory pooling
- Metadata is kept in a multiprocessing.Manager dict (works for cross-process within the same host)
- Objects are stored as raw bytes

## Installation

```bash
poetry install
```

## Usage

```python
from coals import PlasmaStore
import pickle

# Create a store
store = PlasmaStore()

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
