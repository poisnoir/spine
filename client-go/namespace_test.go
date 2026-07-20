package spine

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/poisnoir/spine-go/client-go/internal/globals"
	"github.com/poisnoir/spine-go/client-go/internal/mad"
)

// AddNamespace/GetInfo dial the same real, hardcoded socket path CreateNode
// does (see node.go) - there's no injectable path to swap in a test-scoped
// one, so these tests bind a fake spined directly at it, same fixed-
// convention-path approach the rest of this project relies on.
const testSpinedPath = "/tmp/spine/spined"

// startFakeSpined binds a fake spined at testSpinedPath and hands each
// accepted connection to handleConn on its own goroutine. The returned func
// tears it back down; callers must defer it.
func startFakeSpined(t *testing.T, handleConn func(net.Conn)) func() {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(testSpinedPath), 0o755); err != nil {
		t.Fatalf("failed to create spined dir: %v", err)
	}
	os.Remove(testSpinedPath)

	listener, err := net.Listen("unix", testSpinedPath)
	if err != nil {
		t.Fatalf("failed to bind fake spined at %s: %v", testSpinedPath, err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleConn(conn)
		}
	}()

	return func() {
		listener.Close()
		os.Remove(testSpinedPath)
	}
}

func TestAddNamespace_OK(t *testing.T) {
	var gotName string
	stop := startFakeSpined(t, func(conn net.Conn) {
		defer conn.Close()

		buf := make([]byte, 256)
		n, err := conn.Read(buf)
		if err != nil {
			t.Errorf("fake spined: read failed: %v", err)
			return
		}
		if buf[0] != globals.ADD_NAMESPACE_CODE {
			t.Errorf("fake spined: expected op code %d, got %d", globals.ADD_NAMESPACE_CODE, buf[0])
		}

		ser, err := mad.NewMad[createNamespacePayload]()
		if err != nil {
			t.Errorf("fake spined: %v", err)
			return
		}
		var payload createNamespacePayload
		if err := ser.Decode(buf[1:n], &payload); err != nil {
			t.Errorf("fake spined: decode failed: %v", err)
			return
		}
		gotName = payload.name.String()

		conn.Write([]byte{globals.OK_STATUS_CODE})
	})
	defer stop()

	if err := AddNamespace("robots"); err != nil {
		t.Fatalf("AddNamespace returned %v, want nil", err)
	}
	if gotName != "robots" {
		t.Fatalf("fake spined decoded namespace name %q, want %q", gotName, "robots")
	}
}

func TestAddNamespace_AlreadyRegistered(t *testing.T) {
	stop := startFakeSpined(t, func(conn net.Conn) {
		defer conn.Close()
		buf := make([]byte, 256)
		conn.Read(buf)
		conn.Write([]byte{globals.NAMESPACE_ALREADY_REGISTERED})
	})
	defer stop()

	err := AddNamespace("robots")
	if err != ErrNamespaceAlreadyRegistered {
		t.Fatalf("AddNamespace returned %v, want ErrNamespaceAlreadyRegistered", err)
	}
}

func TestAddNamespace_TooManyNamespaces(t *testing.T) {
	stop := startFakeSpined(t, func(conn net.Conn) {
		defer conn.Close()
		buf := make([]byte, 256)
		conn.Read(buf)
		conn.Write([]byte{globals.TOO_MANY_NAMESPACES})
	})
	defer stop()

	err := AddNamespace("robots")
	if err != ErrTooManyNamespaces {
		t.Fatalf("AddNamespace returned %v, want ErrTooManyNamespaces", err)
	}
}

func TestGetInfo_DecodesNamespacesAndNodes(t *testing.T) {
	stop := startFakeSpined(t, func(conn net.Conn) {
		defer conn.Close()

		var opBuf [1]byte
		if _, err := conn.Read(opBuf[:]); err != nil {
			t.Errorf("fake spined: read op code failed: %v", err)
			return
		}
		if opBuf[0] != globals.GET_INFO_CODE {
			t.Errorf("fake spined: expected op code %d, got %d", globals.GET_INFO_CODE, opBuf[0])
		}

		var wire getInfoResponseWire
		wire.namespace_num = 2

		wire.namespaces[0].name = newSpinedString("common")
		wire.namespaces[0].node_num = 1
		wire.namespaces[0].nodes[0].name = newSpinedString("arm-node")

		wire.namespaces[1].name = newSpinedString("robots")
		wire.namespaces[1].node_num = 0

		ser, err := mad.NewMad[getInfoResponseWire]()
		if err != nil {
			t.Errorf("fake spined: %v", err)
			return
		}
		out := make([]byte, ser.GetRequiredSize())
		if err := ser.Encode(&wire, out); err != nil {
			t.Errorf("fake spined: encode failed: %v", err)
			return
		}
		conn.Write(out)
	})
	defer stop()

	info, err := GetInfo()
	if err != nil {
		t.Fatalf("GetInfo returned %v, want nil", err)
	}

	if len(info.Namespaces) != 2 {
		t.Fatalf("got %d namespaces, want 2", len(info.Namespaces))
	}

	common := info.Namespaces[0]
	if common.Name != "common" {
		t.Fatalf("got namespace %q, want %q", common.Name, "common")
	}
	if len(common.Nodes) != 1 || common.Nodes[0].Name != "arm-node" {
		t.Fatalf("got common's nodes %+v, want one node named arm-node", common.Nodes)
	}

	robots := info.Namespaces[1]
	if robots.Name != "robots" {
		t.Fatalf("got namespace %q, want %q", robots.Name, "robots")
	}
	if len(robots.Nodes) != 0 {
		t.Fatalf("got robots' nodes %+v, want none", robots.Nodes)
	}
}
