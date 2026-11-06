package redisconn

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func path(t *testing.T) string {
	t.Helper()
	value := os.Getenv("SWSS_AUTH_UNIX_SOCKET")
	if value == "" {
		t.Skip("requires isolated native-auth Redis fixture")
	}
	return value
}
func command(t *testing.T, c net.Conn, text string) string {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(time.Second))
	if _, err := fmt.Fprint(c, text+"\r\n"); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return line
}

func TestNativeAuthentication(t *testing.T) {
	socket := path(t)
	for _, authenticate := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		c, err := DialUnixContext(ctx, socket, time.Second, authenticate)
		if err != nil {
			t.Fatal(err)
		}
		cancel() // A completed dial is independent of later cancellation.
		if command(t, c, "SELECT 6") != "+OK\r\n" {
			t.Fatal("SELECT failed")
		}
		response := command(t, c, "SET go-writer value")
		if authenticate && response != "+OK\r\n" {
			t.Fatal(response)
		}
		if !authenticate && !strings.Contains(response, "NOPERM") {
			t.Fatal(response)
		}
		_ = c.Close()
	}
}

func TestCancellationDuringAuthentication(t *testing.T) {
	path(t)
	socket := filepath.Join(t.TempDir(), "stalled.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile("/run/redis-auth/local.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.WriteFile("/run/redis-auth/local.json", original, 0644)
	var entry map[string]interface{}
	if err = json.Unmarshal(original, &entry); err != nil {
		t.Fatal(err)
	}
	entry["unix_socket"] = socket
	updated, _ := json.Marshal(entry)
	if err = os.WriteFile("/run/redis-auth/local.json", updated, 0644); err != nil {
		t.Fatal(err)
	}
	stop, joined := make(chan struct{}), make(chan struct{})
	var handlers sync.WaitGroup
	go func() {
		defer close(joined)
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				defer c.Close()
				buffer := make([]byte, 1024)
				_, _ = c.Read(buffer)
				<-stop
			}()
		}
	}()
	defer func() { _ = listener.Close(); <-joined; close(stop); handlers.Wait() }()
	for i := 0; i < 8; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		c, err := DialUnixContext(ctx, socket, time.Second, true)
		cancel()
		if c != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("connection=%v error=%v", c, err)
		}
	}
}

func TestInvalidArguments(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := DialUnixContext(ctx, "/ignored", 0, true); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := DialUnixContext(context.Background(), "/ignored", -time.Second, true); err == nil {
		t.Fatal("negative timeout accepted")
	}
	if _, err := DialUnixContext(context.Background(), "/invalid\x00path", 0, true); err == nil {
		t.Fatal("NUL path accepted")
	}
}
