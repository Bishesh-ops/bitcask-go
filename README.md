# Bitcask-Go: Distributed Key-Value Database

A lightweight, distributed key-value database built entirely from scratch in Go. Designed with pure C-style systems mechanics, this project avoids heavy external frameworks and abstraction layers to focus on raw byte buffers, explicit memory layouts, OS-level file descriptors, persistent connection pooling, and multi-threaded synchronization.

---

## Architecture & Core Components

### 1. Storage Engine (`internal/engine`)
Implemented using the **Bitcask Append-Only** design pattern to guarantee highly sequential disk writes and constant-time reads.
* **Binary Layout:** Records are serialized into contiguous arrays using explicit Little-Endian framing. Every record features a **20-byte fixed header** containing a CRC32 checksum, microsecond timestamp, Key Size, and Value Size.
* **Lock-Free Reads:** State updates append to an active `os.File` descriptor with `O_APPEND`. Value lookups leverage an in-memory `keydir` hashmap pointing directly to raw byte locations on disk, read concurrently via thread-safe `pread` system calls (`ReadAt`).

### 2. Network Transport Layer (`internal/wire`)
A custom **TLV (Type-Length-Value)** binary protocol engineered to eliminate TCP sticky packets and network stream fragmentation.
* **Stream Framing:** Guaranteed packet boundary parsing using fixed 7-byte protocol headers read via `io.ReadFull`.
* **Shared Multiplexing:** A single TCP connection safely interleaves client database operations (`CmdPut`, `CmdGet`) alongside distributed internal cluster traffic without channel blocking.

### 3. Distributed Consensus Shell (`internal/raft`)
An embedded implementation of the **Raft Consensus Algorithm** managing high-availability state machine replication.
* **Contiguous Byte Serialization:** Custom encoding/decoding of `RequestVote` and `AppendEntries` arguments straight into raw byte buffers, completely eliminating the heap allocation overhead of reflection-based libraries (`encoding/gob` or `json`).
* **Connection Pooling:** Inter-node communication utilizes a persistent, thread-safe connection manager leveraging the **double-checked locking pattern** to lazily reuse underlying sockets and prevent ephemeral port exhaustion.
* **Independent Lifecycle:** Nodes operate asynchronously across clean Follower, Candidate, and Leader states using short-lived mutex locks (`sync.Mutex`), randomized election bounds (150ms–300ms), and isolated background threads broadcasting Keep-Alive frames (`AppendEntries`) every 50ms.

---

## Getting Started

### Prerequisites
* Go 1.25+ (or any version supporting standard module initialization)

### Running the Cluster Interface

1. **Boot the Hybrid Server/Node**
   Starts the underlying storage engine, binds the consensus engine, and opens the listening multiplexer port on `:8080`.
   ```bash
   go run cmd/server/main.go
   ```
2. **Execute the Integration Client**
    In a separate terminal tab, run the comprehensive verification suite. The client will:
      *    Execute standard key-value mutations directly to the storage engine.

      *    Dynamically detect background node terms to inject a dominant Leader keep-alive frame, forcing the server to suppress its timers and bow down as a clean Follower.

      *    Verify strict historical boundaries by testing the rejection of stale candidate packets.

    ```
        bash
    go run cmd/client/main.go
    ```
## Thread Safety and Gurantees
* **Storage Access:** Mutex-protected append paths serialize active disk writes while shared Read-Write locks (`sync.RWMutex`) allow unhindered concurrent access to the active pointer index.

* **Network Isolation:** Socket read/write paths are cleanly protected to prevent concurrent Goroutines from interleaving byte frames over shared OS network descriptors.

* **State Machine Defenses:** The consensus engine guarantees historical safety by enforcing absolute rejection of older incoming term parameters, protecting data state stability across unstable network boundaries.

## Liscense
Distributed under the MIT License.
