# Spine-Go
### High-Performance, Hybrid Communication Middleware for Go

**Spine-Go** is a lightweight, local-first communication library designed for robotics, edge computing, and distributed systems. It provides a "zero-config" interface for **Services (RPC)** and **Pub/Sub** patterns, optimized for extreme performance on local machines while seamlessly scaling to LAN/WAN via the `spined` daemon.

## 🚀 Key Philosophy: Hybrid Architecture
Spine-Go employs a dual-path architecture to ensure you never sacrifice performance for connectivity:

1.  **Local-First IPC**: When communicating on the same machine, Spine-Go uses **Unix Domain Sockets**, bypassing the network stack for ultra-low latency and maximum throughput.
2.  **Daemon-Assisted Networking**: When a connection outside the machine is needed, Spine-Go handshakes with the `spined` daemon.

---

## ✨ Features
*   **Zero IDL Boilerplate**: No `.proto` or `.msg` files. Define your services and topics using native Go types and generics.
*   **Compile-Time Type Safety**: Leverages Go 1.18+ generics to catch data mismatches before your code even runs.
*   **Extreme Performance**: Optimized buffer pooling and the `mad-go` serialization engine ensure minimal GC pressure and high throughput.
*   **Resilient Connectivity**: Built-in exponential backoff and automatic reconnection logic.
*   **Hybrid Discovery**: Works in "Local-Only" mode without any external dependencies, or "Global" mode by connecting to the `spined` sidecar.

---

## 📊 Performance Benchmarks
*Tested on AMD Ryzen Threadripper PRO 5945WX (Linux/amd64)*

| Pattern | Throughput | Latency (ns/op) | Memory (B/op) | Allocs/op |
| :--- | :--- | :--- | :--- | :--- |
| **Pub/Sub** | ~53,000 msg/sec | 18,896 | 221 | 8 |
| **Service Call (RPC)** | ~33,000 req/sec | 30,578 | 417 | 13 |
| **Threaded Service** | ~27,000 req/sec | 32,909 | 271 | 9 |

> *Note: Benchmarks represent local IPC overhead including serialization. Actual network performance depends on the `spined` configuration.*

---

## 🛠 Getting Started

### 1. Initialize a Namespace
All communication is isolated within a namespace.
```go
ctx := context.Background()
logger := slog.Default()

// Joins a namespace. Automatically detects spined daemon.
ns, err := spine.JointNamespace("robot_core", ctx, logger)
```

### 2. Services (RPC)
Turn any Go function into a network-discoverable service.

**Server:**
```go
handler := func(input string) (int, error) {
    return len(input), nil
}

// Register a standard service (sequential execution)
service, _ := spine.NewService(ns, "compute_len", handler)
```

**Client:**
```go
caller, _ := spine.NewServiceCaller[string, int](ns, "compute_len")

// Type-safe call
result, err := caller.Call("hello spine", context.Background())
```

### 3. Pub/Sub (Asynchronous)
**Publisher:**
```go
pub, _ := spine.NewPublisher[SensorData](ns, "lidar")
pub.Publish(data)
```

**Subscriber:**
```go
sub, _ := spine.NewSubscriber[SensorData](ns, "lidar")

// Polling for data
data, err := sub.Get()
```

---

## 🔧 Service Types
*   **Standard Service**: Processes requests sequentially. Best for handlers that modify shared state.
*   **Threaded Service**: Processes requests in parallel. Best for stateless, CPU-intensive, or I/O-bound handlers.

---

## 🏗 Installation
```bash
go get github.com/poisnoir/spine-go
```

*For cross-machine networking, ensure the `spined` daemon is running on your host.*

---
**Built for the Go Edge & Robotics community.**
