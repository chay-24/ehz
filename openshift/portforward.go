package openshift

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"time"
)

// PortForward starts an 'oc port-forward' subprocess tunneling remotePort on
// the given service to a random local port. It blocks until the local port is
// accepting connections (or the timeout is exceeded), then returns the local
// port and a cancel function that kills the subprocess.
func PortForward(ctx context.Context, cluster, namespace, service string, remotePort int) (localPort int, cancel func(), err error) {
	localPort, err = freePort()
	if err != nil {
		return 0, nil, fmt.Errorf("finding free port: %w", err)
	}

	args := []string{
		"--server=" + cluster,
		"--namespace=" + namespace,
		"port-forward",
		"svc/" + service,
		fmt.Sprintf("%d:%d", localPort, remotePort),
	}

	cmd := exec.CommandContext(ctx, "oc", args...)
	if err := cmd.Start(); err != nil {
		return 0, nil, fmt.Errorf("starting port-forward to %s: %w", service, err)
	}

	cancel = func() { _ = cmd.Process.Kill() }

	if err := waitForPort(localPort, 15*time.Second); err != nil {
		cancel()
		return 0, nil, fmt.Errorf("port-forward to %s did not become ready: %w", service, err)
	}

	return localPort, cancel, nil
}

// freePort asks the OS for an available TCP port by binding to :0 and
// immediately releasing it.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

// waitForPort dials 127.0.0.1:port repeatedly until it succeeds or timeout
// elapses.
func waitForPort(port int, timeout time.Duration) error {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()

			return nil
		}

		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("timed out after %s waiting for port %d", timeout, port)
}
