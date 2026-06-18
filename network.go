package spine

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
)

func runListener(listener net.Listener, logger *slog.Logger, handler func(io.ReadWriteCloser)) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("unable to accept connection", "error", err)
			continue
		}
		go handler(conn)
	}
}

func createListener(path string) (net.Listener, error) {
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directories: %w", err)
	}

	_ = os.Remove(path)
	listener, err := net.Listen("unixpacket", path)
	return listener, err
}
