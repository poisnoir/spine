package spine

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
)

// The buffer is coming from pool and it is big enoght
// the response is gonna go to given buffer if exist
func write(sess io.ReadWriteCloser, buf []byte, requestSize int, hasResponse bool) (int, error) {

	_, err := sess.Write(buf[:requestSize])
	if !hasResponse {
		return 0, err
	}

	n, err := sess.Read(buf)
	return n, err
}

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
	listener, err := net.Listen("unix", path)
	return listener, err
}
