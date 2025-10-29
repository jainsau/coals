#!/usr/bin/env python3
"""
Demo script for coals library showing basic usage.
"""

import pickle
import time
from multiprocessing import Process
from coals import PlasmaStore, ObjectNotFound


def client_reader(meta_dict, obj_id):
    """Reader process that accesses the shared object."""
    store = PlasmaStore(manager_dict=meta_dict)
    print("[reader] metadata snapshot:", store.list_objects())
    try:
        shm, size = store.get(obj_id)
        try:
            data = bytes(shm.buf[:size])
            print("[reader] read bytes:", data)
            # demonstrate pickle round-trip for Python objects
            try:
                obj = pickle.loads(data)
                print("[reader] unpickled:", obj)
            except Exception:
                pass
        finally:
            shm.close()
            store.release(obj_id)
            print("[reader] released obj_id", obj_id)
    except Exception as e:
        print("[reader] error:", e)


def main():
    """Main demo function."""
    store = PlasmaStore()
    
    # Store a python object as pickled bytes (common pattern)
    payload = {"message": "hello from coals prototype", "time": time.time()}
    data = pickle.dumps(payload)
    obj_id = store.put(data)
    print("[main] put obj_id", obj_id)
    print("[main] metadata after put:", store.list_objects())

    # Spawn a reader process that uses the same Manager dict to access the object
    p = Process(target=client_reader, args=(store.meta, obj_id))
    p.start()
    p.join()

    # Main process can still access the object (because its refcount was bumped by reader)
    shm, size = store.get(obj_id)
    print("[main] main process read size", size)
    print("[main] main process bytes head:", bytes(shm.buf[: min(32, size)]))
    shm.close()
    
    # Release twice (one for original, one for the get above)
    store.release(obj_id)
    print("[main] after one release:", store.list_objects())
    store.release(obj_id)
    print("[main] after final release (should be cleaned):", store.list_objects())

    store.shutdown()


if __name__ == "__main__":
    main()
