package redisconn

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The fake peer consumes AUTH without answering. Authentication, policy and
// credentials still go through the native library, rather than a mocked dial.
func authPeer(t *testing.T, answer bool) (string, <-chan struct{}, func()) {
	t.Helper()
	root := os.Getenv("REDIS_AUTH_TEST_DIR")
	if root == "" || os.Geteuid() != 0 {
		t.Skip("run tests/run-redis-auth-tests.sh")
	}
	policyRoot := filepath.Join(root, "policy")
	if err := os.MkdirAll(filepath.Join(policyRoot, "local-system"), 0755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(policyRoot) })
	serverRoot, err := os.MkdirTemp("", "swss-auth-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(serverRoot) })
	socket := filepath.Join(serverRoot, "redis.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := json.Marshal(map[string]interface{}{"username": "go-writer", "unix_socket": socket, "tcp_host": "127.0.0.1", "tcp_port": 0})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(policyRoot, "local.json"), policy, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(policyRoot, "local-system/writer.secret"), []byte("go-password"), 0400); err != nil {
		t.Fatal(err)
	}
	authenticated := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(conn)
		// Verify AUTH is actually transmitted before triggering cancellation.
		header, err := reader.ReadString('\n')
		if err != nil || header != "*3\r\n" {
			t.Error("native peer did not receive AUTH")
			return
		}
		for i := 0; i < 3; i++ {
			length, err := reader.ReadString('\n')
			if err != nil || !strings.HasPrefix(length, "$") {
				t.Error("invalid AUTH framing")
				return
			}
			n, err := strconv.Atoi(strings.TrimSpace(length[1:]))
			if err != nil || n < 0 || n > 128 {
				t.Error("invalid AUTH argument length")
				return
			}
			argument := make([]byte, n+2)
			if _, err := io.ReadFull(reader, argument); err != nil {
				t.Error("incomplete AUTH argument")
				return
			}
			if i == 0 && string(argument) != "AUTH\r\n" {
				t.Error("expected native AUTH command")
				return
			}
		}
		close(authenticated)
		if !answer {
			io.Copy(io.Discard, reader) // Remain silent until the client closes.
			return
		}
		fmt.Fprint(conn, "+OK\r\n")
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if line != "PING\r\n" {
				t.Error("unexpected post-authentication request")
				return
			}
			fmt.Fprint(conn, "+PONG\r\n")
		}
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() { listener.Close(); <-done })
	}
	t.Cleanup(stop)
	return socket, authenticated, stop
}

func TestNativeAuthCancellationAndDeadline(t *testing.T) {
	for _, scenario := range []string{"cancel", "cancel-unlimited", "deadline", "dial-timeout"} {
		t.Run(scenario, func(t *testing.T) {
			path, authenticated, stop := authPeer(t, false)
			before, err := os.ReadDir("/proc/self/fd")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			timeout := time.Second
			expected := context.Canceled
			switch scenario {
			case "deadline":
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				expected = context.DeadlineExceeded
			case "dial-timeout":
				timeout = 100 * time.Millisecond
				expected = os.ErrDeadlineExceeded
			case "cancel-unlimited":
				timeout = 0
			}
			if strings.HasPrefix(scenario, "cancel") {
				go func() {
					select {
					case <-authenticated:
						time.Sleep(50 * time.Millisecond)
						cancel()
					case <-ctx.Done():
					}
				}()
			}
			start := time.Now()
			conn, err := DialUnix(ctx, path, timeout)
			elapsed := time.Since(start)
			if conn != nil {
				conn.Close()
				t.Fatal("interrupted authentication returned a connection")
			}
			if !errors.Is(err, expected) {
				t.Fatalf("expected %v, got %v", expected, err)
			}
			if elapsed > 350*time.Millisecond {
				t.Fatalf("interrupted AUTH took %s", elapsed)
			}
			select {
			case <-authenticated:
			default:
				t.Fatal("test did not reach native AUTH")
			}
			if scenario == "dial-timeout" && ctx.Err() != nil {
				t.Fatal("dial timeout must not cancel the caller's context")
			}
			stop()
			dialErr := err
			after, err := os.ReadDir("/proc/self/fd")
			// stop closes the listener; all connection/native descriptors must go.
			if err != nil || len(after) != len(before)-1 {
				t.Fatalf("descriptor cleanup: before %d, after %d, error %v", len(before), len(after), err)
			}
			t.Logf("native AUTH %s returned after %s with %v; descriptors %d -> %d", scenario, elapsed, dialErr, len(before), len(after))
		})
	}
}

func TestCancellationAfterDialLeavesReturnedConnectionUsable(t *testing.T) {
	path, _, _ := authPeer(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := DialUnix(ctx, path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cancel()
	conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := fmt.Fprint(conn, "PING\r\n"); err != nil {
		t.Fatal(err)
	}
	if reply, err := bufio.NewReader(conn).ReadString('\n'); err != nil || reply != "+PONG\r\n" {
		t.Fatalf("returned connection was affected by late cancellation: %q, %v", reply, err)
	}
}

func TestCanceledContextDoesNotConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if conn, err := DialUnix(ctx, "/missing.sock", time.Second); conn != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled dial returned %v, %v", conn, err)
	}
}

func TestNativeAuthSuccessCancellationRace(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		t.Run(strconv.Itoa(attempt), func(t *testing.T) {
			path, authenticated, stop := authPeer(t, true)
			before, err := os.ReadDir("/proc/self/fd")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cancelled := make(chan struct{})
			go func() {
				defer close(cancelled)
				select {
				case <-authenticated:
					cancel()
				case <-ctx.Done():
				}
			}()
			conn, dialErr := DialUnix(ctx, path, time.Second)
			if dialErr != nil && !errors.Is(dialErr, context.Canceled) {
				t.Errorf("success/cancel race returned a native error: %v", dialErr)
			}
			if conn != nil {
				conn.Close()
			}
			cancel()
			<-cancelled
			stop()
			after, err := os.ReadDir("/proc/self/fd")
			if err != nil || len(after) != len(before)-1 {
				t.Fatalf("race descriptor cleanup: before %d, after %d, error %v", len(before), len(after), err)
			}
		})
	}
}
