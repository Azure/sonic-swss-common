// Package redisconn adapts Go clients to explicit native Unix authentication.
package redisconn

/*
#cgo CFLAGS: -I${SRCDIR}/../../common/c-api
#cgo LDFLAGS: -lswsscommon
#include <stdlib.h>
#include <errno.h>
#include "redissocket.h"
*/
import "C"

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"time"
	"unsafe"
)

// DialUnixContext opens an explicitly selected Unix connection. TCP/TLS dialing
// remains with the caller; no network selection or credential parsing occurs here.
func DialUnixContext(ctx context.Context, path string, timeout time.Duration, authenticate bool) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.IndexByte(path, 0) >= 0 {
		return nil, errors.New("invalid Unix path")
	}
	if timeout < 0 || timeout > time.Duration(^uint32(0))*time.Millisecond {
		return nil, errors.New("invalid native dial timeout")
	}
	read, write, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer read.Close()
	defer write.Close()
	stop, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-ctx.Done():
			_, _ = write.Write([]byte{1})
		case <-stop:
		}
	}()
	name := C.CString(path)
	defer C.free(unsafe.Pointer(name))
	milliseconds := timeout / time.Millisecond
	if timeout > 0 {
		milliseconds = (timeout + time.Millisecond - 1) / time.Millisecond
	}
	var enabled C.uint8_t
	if authenticate {
		enabled = 1
	}
	var fd, code C.int32_t
	result := C.SWSSRedisSocket_open_unix(name, C.uint32_t(milliseconds), enabled, C.int32_t(read.Fd()), &fd, &code)
	close(stop)
	<-joined
	defer C.SWSSString_free(result.message)
	defer C.SWSSString_free(result.location)
	if ctx.Err() != nil {
		if fd >= 0 {
			_ = os.NewFile(uintptr(fd), path).Close()
		}
		return nil, ctx.Err()
	}
	if result.exception != C.SWSSException_None {
		if code == C.ETIMEDOUT {
			return nil, os.ErrDeadlineExceeded
		}
		return nil, errors.New(C.GoString(C.SWSSStrRef_c_str(C.SWSSStrRef(unsafe.Pointer(result.message)))))
	}
	file := os.NewFile(uintptr(fd), path)
	conn, err := net.FileConn(file)
	_ = file.Close()
	return conn, err
}
