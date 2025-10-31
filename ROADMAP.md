# Coals Roadmap: Achieving Plasma Parity

This roadmap outlines key features to implement in Coals to achieve functional parity with Apache Arrow Plasma, focusing on its core strengths as a high-performance, in-memory object store for zero-copy data exchange. We also aim to include advanced memory management capabilities.

## 1. Advanced Memory Management & Eviction

**Goal:** Implement sophisticated memory management and eviction strategies to optimize memory usage and performance, aligning with Plasma's capabilities.

**Features to Implement:**
- Automatic eviction policies (e.g., LRU, FIFO, custom policies)
- Memory pressure detection and automatic cleanup
- Configurable eviction thresholds
- Background eviction threads for asynchronous cleanup
- Multiple eviction strategies (size-based, count-based, time-based)
- Sophisticated memory pooling and pre-allocation

## 2. Performance & Optimization

**Goal:** Enhance Coals' performance to match Plasma's high-throughput, low-latency characteristics.

**Features to Implement:**
- Optimized metadata storage (move away from `Manager.dict` for critical paths)
- Lock-free data structures where possible for concurrent access
- Better concurrency control mechanisms
- Batching and pipelining of operations

## 3. Apache Arrow Integration

**Goal:** Enable seamless, zero-copy data exchange with Apache Arrow objects, a cornerstone of Plasma's design.

**Features to Implement:**
- Native Arrow buffer support
- Zero-copy integration with Arrow arrays/tables
- Metadata schema support for Arrow types
- Arrow serialization/deserialization helpers
- Arrow IPC protocol support

## 4. API Completeness

**Goal:** Provide a comprehensive and robust API that mirrors Plasma's client-facing functionalities.

**Features to Implement:**
- `fetch()` with timeout support
- `contains()` with timeout
- `list()` with filtering and pagination
- Async/await API for non-blocking operations
- Full pub/sub notification system (beyond basic object sealing notifications)
- Batch operations for `put`, `get`, `release`, etc.

## 5. Observability & Monitoring

**Goal:** Integrate monitoring capabilities to track Coals' performance and resource usage, similar to what would be expected in a production-grade Plasma store.

**Features to Implement:**
- Metrics collection (object count, memory usage, operation latency)
- Performance profiling and debugging tools