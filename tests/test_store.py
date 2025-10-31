import pytest
from coals import Store, ObjectNotFound
import pickle
import time
from multiprocessing import Process, Queue, Manager

def test_put_get():
    store = Store()
    data = b"test_data"
    obj_id = store.put(data)
    shm, size = store.get(obj_id)
    retrieved_data = bytes(shm.buf[:size])
    shm.close()
    assert retrieved_data == data
    store.release(obj_id)
    store.shutdown()

def test_release():
    store = Store()
    data = b"test_data"
    obj_id = store.put(data)
    store.release(obj_id)
    with pytest.raises(Exception):
        store.get(obj_id)
    store.shutdown()

def test_seal():
    store = Store()
    data = b"test_data"
    obj_id = store.put(data)
    store.seal(obj_id)
    # After sealing, the object should be immutable.
    # This is not directly testable at this level,
    # but we can check the metadata.
    meta = store.list_objects()
    assert meta[obj_id]['sealed'] is True
    store.release(obj_id)
    store.shutdown()

def test_pickle_roundtrip():
    store = Store()
    data = {"a": 1, "b": [1,2,3]}
    pickled_data = pickle.dumps(data)
    obj_id = store.put(pickled_data)
    shm, size = store.get(obj_id)
    retrieved_pickled_data = bytes(shm.buf[:size])
    shm.close()
    retrieved_data = pickle.loads(retrieved_pickled_data)
    assert retrieved_data == data
    store.release(obj_id)
    store.shutdown()

def test_contains():
    store = Store()
    data = b"test_data"
    obj_id = store.put(data)
    assert store.contains(obj_id)
    store.release(obj_id)
    assert not store.contains(obj_id)
    store.shutdown()

def test_info():
    store = Store()
    data = b"test_data"
    obj_id = store.put(data)
    info = store.info(obj_id)
    assert info['size'] == len(data)
    assert info['sealed'] is True
    assert info['refcount'] == 1
    store.release(obj_id)
    with pytest.raises(ObjectNotFound):
        store.info(obj_id)
    store.shutdown()

def test_delete():
    store = Store()
    data = b"test_data"
    obj_id = store.put(data)
    assert store.contains(obj_id)
    store.delete(obj_id)
    assert not store.contains(obj_id)
    with pytest.raises(ObjectNotFound):
        store.get(obj_id)
    store.shutdown()

def test_store_capacity():
    store = Store(capacity=100)
    assert store.store_capacity() == 100
    store.shutdown()

def test_evict():
    store = Store(capacity=30)
    obj_id1 = store.put(b"data1") # size 5
    time.sleep(0.01)
    obj_id2 = store.put(b"data2") # size 5
    time.sleep(0.01)
    obj_id3 = store.put(b"data3") # size 5

    # Current size is 15. Evict 10 bytes. Should remove obj_id1 and obj_id2
    store.evict(10)

    assert not store.contains(obj_id1)
    assert not store.contains(obj_id2)
    assert store.contains(obj_id3)

    store.shutdown()

def client_subscriber(meta_dict, notification_condition, sealed_objects_queue, result_queue):
    # The child process directly uses the shared objects
    with notification_condition:
        while not sealed_objects_queue:
            if not notification_condition.wait(5):
                result_queue.put(None) # Timeout
                return
        notified_obj_id = sealed_objects_queue.pop(0)
        result_queue.put(notified_obj_id)

def test_subscribe_notification():
    # Create a manager in the main process
    manager = Manager()
    store = Store(manager=manager)
    result_queue = Queue()

    p = Process(target=client_subscriber, args=(store.meta, store._notification_condition, store._sealed_objects_queue, result_queue))
    p.start()

    # Give the subscriber a moment to start and subscribe
    time.sleep(0.1)

    data = b"notified_data"
    obj_id = store.put(data)

    p.join(timeout=5) # Wait for the process to finish
    p.terminate()

    notified_obj_id = result_queue.get(timeout=5)
    assert notified_obj_id == obj_id

    store.shutdown()
    manager.shutdown() # Shutdown the manager