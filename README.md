# spine-go
### High-Performance, Local-First Service Library for Go

**Spine-Go** is a lightweight, "zero-config" middleware designed for high-performance communication on local networks (LAN) and edge clusters. It provides ROS-like communication patterns—**Services (RPC)** and **Pub/Sub**—built natively in Go with zero external brokers, minimal overhead, and absolute developer freedom.

> Spine-Go is currently in an **Alpha/Experimental** state. The core "backbone" is functional, but the protocol is still evolving. Expect breaking changes, and do not use this for mission-critical production systems yet.

---

## The Philosophy: "Just write a Function"
Unlike ROS or gRPC, Spine-Go doesn't force you into complex IDL files (.msg, .proto), custom build tools, or heavy middleware dependencies. 

If you can write a Go function, you can build a Spine service.
* **No IDL:** No Protobuf or ROS `.srv` files to maintain.
* **No Boilerplate:** Use any Go library (OpenCV, GORM, Periph.io) and "service-ify" it instantly.
* **Type-Safe:** Fully leverages **Go Generics** for compile-time safety.

---

## Key Features
* **KCP over UDP:** Superior performance on "lossy" or unstable networks (like busy Wi-Fi) compared to TCP.
* **Zeroconf (mDNS) Discovery:** Plug-and-play service registration. No IP management required.
* **Encrypted by Default:** Namespace-based isolation with built-in AES-GCM encryption.

---

## Spine-Go vs. ROS
| Feature | ROS 2 (DDS) | Spine-Go |
| :--- | :--- | :--- |
| **Complexity** | High (XML, IDLs, Colcon) | **Zero** (Standard Go tools) |
| **Discovery** | DDS Discovery (Heavier) | **Zeroconf** (Lightweight) |
| **Protocol** | TCP/UDP/DDS | **KCP/UDP** (Low Jitter) |

---

## Getting Started

### 1. Join a Namespace
All communication happens within a secured namespace.

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
// Join a namespace with a secret key and encryption enabled
ns, err := spine.JointNamespace("robot_arm", "secret_key", logger, true)
```

### 2. Create a Service
Turn any Go function into a network-discoverable service.
```go
// Define a handler: func(InputType) (OutputType, error)
handler := func(input string) (uint32, error) {
    return uint32(len(input)), nil
}

// Register the service
_, err = spine.NewService(ns, "string_length", handler)
```

### 3. Call a Service
Call services from any machine on the network using the same generic types.
```go
caller, err := NewServiceCaller[string, uint32](ns, "string_length")

ctx, _ := context.WithTimeout(context.Background(), time.Second)
// Blocks until result is received or context is canceled
result, err := caller.Call("hello world", ctx)
```

---

## Pub/Sub
Publishers and Subscribers allow for asynchronous data flow. Connections are established automatically once a publisher is discovered on the network.

```go
// Create a Publisher
pub, _ := spine.NewPublisher[SensorData](ns, "lidar_scan")
pub.Publish(currentData)

// Create a Subscriber
sub, _ := spine.NewSubscriber(ns, "lidar_scan", func(data SensorData) {
    fmt.Printf("Received data: %v\n", data)
})
```

---

## Examples
You can find practical implementations and usage patterns in the `example/` directory:
- `example/publisher/`: Asynchronous data broadcasting.
- `example/subscriber/`: Subscribing to data streams.
- `example/service/`: Setting up a network-discoverable service.
- `example/service_caller/`: Calling services across the network.

To run an example:
```bash
# In one terminal
go run example/service/service.go

# In another terminal
go run example/service_caller/caller.go
```

---

## Benchmarks
We take performance seriously. To run the benchmarks and see how Spine-Go performs on your machine:

```bash
# Run all benchmarks
go test -bench=. -benchmem

# Run specific service benchmarks
go test -bench=BenchmarkServiceCall
go test -bench=BenchmarkThreadedServiceCall
```

The benchmarks cover standard services, threaded services, parallel execution, and encrypted communication.

---

## Known Weaknesses
As we are "Still in Spine," there are several areas under active development:
1. **Error Propagation:** Currently, handler errors are logged on the server but not yet fully propagated as structured errors to the client.
2. **Lifecycle Management:** Graceful shutdowns and connection timeout handling are still being refined.
3. **Documentation:** We are still working on comprehensive guides and examples.

---

## Dependencies
- [mad-go](https://github.com/poisnoir/mad-go): Serialization
- [backoff](https://github.com/cenkalti/backoff): backoff

## Contribution
Feel free to contribute or suggest features. Contact: @rima1881

**Built with ❤️ for the Go Edge & Robotics community.**
