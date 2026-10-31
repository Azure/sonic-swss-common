// Package redisconn exposes native Redis writer login to Go connection pools.
package redisconn

/*
#cgo CFLAGS: -I/usr/include/swss/c-api
#cgo LDFLAGS: -lswsscommon
#include <stdlib.h>
#include "dbconnector.h"
*/
import "C"

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type DialFunc func(context.Context, string, string) (net.Conn, error)

func nativeError(result C.SWSSResult) error {
	if result.exception == C.SWSSException_None {
		return nil
	}
	defer C.SWSSString_free(result.message)
	defer C.SWSSString_free(result.location)
	return errors.New(C.GoString(C.SWSSStrRef_c_str(C.SWSSStrRef(unsafe.Pointer(result.message)))))
}

func milliseconds(timeout time.Duration) C.uint32_t {
	if timeout <= 0 {
		return 0
	}
	value := timeout / time.Millisecond
	if timeout%time.Millisecond != 0 {
		value++
	}
	if value > 1<<32-1 {
		value = 1<<32 - 1
	}
	return C.uint32_t(value)
}

// contextError handles a native timeout racing the context timer callback.
func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// DialUnix returns an authenticated socket without selecting a logical database.
// C++ owns socket creation, policy parsing, credential access and AUTH. timeout
// bounds the complete connection/authentication operation; zero is unlimited.
func DialUnix(ctx context.Context, path string, timeout time.Duration) (net.Conn, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if strings.IndexByte(path, 0) >= 0 {
		return nil, errors.New("Redis socket path contains a null byte")
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		if timeout <= 0 || remaining < timeout {
			timeout = remaining
		}
	}
	var timeoutAt time.Time
	if timeout > 0 {
		timeoutAt = time.Now().Add(timeout)
	}

	// Closing the pipe's writer wakes native poll during connect or AUTH. No
	// socket descriptor is shared with the watcher. Join it before releasing the
	// pipe or returning, including when success and cancellation race.
	cancelFd := C.int32_t(-1)
	var done, stopped chan struct{}
	if ctx.Done() != nil {
		reader, writer, err := os.Pipe()
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		defer writer.Close()
		cancelFd = C.int32_t(reader.Fd())
		done, stopped = make(chan struct{}), make(chan struct{})
		go func() {
			defer close(stopped)
			select {
			case <-ctx.Done():
				writer.Close()
			case <-done:
			}
		}()
	}
	nativePath := C.CString(path)
	defer C.free(unsafe.Pointer(nativePath))
	fd := C.int32_t(-1)
	err := nativeError(C.SWSSRedisSocket_open_unix_interruptible(nativePath, milliseconds(timeout), cancelFd, &fd))
	if done != nil {
		close(done)
		<-stopped
	}
	var file *os.File
	if fd >= 0 {
		file = os.NewFile(uintptr(fd), "Redis Unix socket")
		defer file.Close()
	}
	if contextErr := contextError(ctx); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		if !timeoutAt.IsZero() && !time.Now().Before(timeoutAt) {
			return nil, &net.OpError{Op: "dial", Net: "unix", Err: os.ErrDeadlineExceeded}
		}
		return nil, err
	}
	if file == nil {
		return nil, errors.New("Native Redis connection returned no socket")
	}
	return net.FileConn(file)
}

// WrapDialer uses the native Unix factory and retains TCP/TLS dialing.
// Custom Unix dialers are rejected because their connection cannot be passed
// into the native factory. Custom TCP dialers are preserved.
func WrapDialer(existing DialFunc, timeout time.Duration, config *tls.Config) DialFunc {
	custom := existing != nil
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if timeout < 0 {
		timeout = 0
	}
	if existing == nil {
		dialer := &net.Dialer{Timeout: timeout, KeepAlive: 5 * time.Minute}
		existing = dialer.DialContext
		if config != nil {
			existing = (&tls.Dialer{NetDialer: dialer, Config: config}).DialContext
		}
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if network == "unix" {
			if custom || config != nil {
				return nil, errors.New("Native Redis Unix dialing does not accept a custom dialer or TLS")
			}
			return DialUnix(ctx, address, timeout)
		}
		if network == "tcp" || network == "tcp4" || network == "tcp6" {
			host, portString, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			port, err := strconv.Atoi(portString)
			if err != nil || port < 1 || port > 65535 {
				return nil, errors.New("Invalid Redis TCP port")
			}
			if strings.IndexByte(host, 0) >= 0 {
				return nil, errors.New("Redis host contains a null byte")
			}
			nativeHost := C.CString(host)
			err = nativeError(C.SWSSRedisSocket_check_tcp(nativeHost, C.int32_t(port)))
			C.free(unsafe.Pointer(nativeHost))
			if err != nil {
				return nil, err
			}
		}
		return existing(ctx, network, address)
	}
}
