package stats

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TestConn struct {
	w         io.Writer
	werr      error // write error
	writeFn   func(b []byte) (n int, err error)
	dialCount int32
}

func (c *TestConn) DialCount() int {
	return int(atomic.LoadInt32(&c.dialCount))
}

func NewTestConn(w io.Writer) *TestConn {
	return &TestConn{w: w}
}

func (c *TestConn) Read(b []byte) (n int, err error) {
	panic("not implemented")
}

func (c *TestConn) Write(b []byte) (n int, err error) {
	if c.writeFn != nil {
		return c.writeFn(b)
	}
	return c.w.Write(b)
}

func (c *TestConn) Close() error {
	if wc, ok := c.w.(io.WriteCloser); ok {
		return wc.Close()
	}
	return nil
}

func (c *TestConn) Dial(network, address string) (net.Conn, error) {
	atomic.AddInt32(&c.dialCount, 1)
	return c, nil
}

func (c *TestConn) LocalAddr() net.Addr  { panic("not implemented") }
func (c *TestConn) RemoteAddr() net.Addr { panic("not implemented") }

func (*TestConn) SetDeadline(_ time.Time) error      { return nil }
func (*TestConn) SetReadDeadline(_ time.Time) error  { return nil }
func (*TestConn) SetWriteDeadline(_ time.Time) error { return nil }

func TestConnSink(t *testing.T) {
	var buf bytes.Buffer
	opts := Options{
		CustomDialer: &TestConn{
			w: &buf,
		},
	}
	conn, err := opts.Connect()
	if err != nil {
		t.Fatal(err)
	}
	var exp bytes.Buffer
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("name-%d", i)
		conn.FlushCounter(name, uint64(i))
		fmt.Fprintf(&exp, "%s:%d|c\n", name, uint64(i))
	}
	if err := conn.FlushTimeout(time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), exp.Bytes()) {
		t.Errorf("ConnSink: want:\n%s\ngot:\n%s\n", exp.Bytes(), buf.Bytes())
	}
}

func TestConnSink_Reconnect(t *testing.T) {
	var buf bytes.Buffer
	tconn := &TestConn{
		w: &buf,
	}
	opts := Options{
		ReconnectWait: 1,
		CustomDialer:  tconn,
	}
	conn, err := opts.Connect()
	if err != nil {
		t.Fatal(err)
	}
	var exp bytes.Buffer
	tconn.writeFn = func(p []byte) (int, error) {
		tconn.writeFn = nil
		return 0, errors.New("wat")
	}
	// CEV: this works (kinda), but the buffer
	// is large and we discared a lot of it
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("name-%d", i)
		conn.FlushCounter(name, uint64(i))
		conn.Flush()

		// we drop the first write
		if i != 0 {
			fmt.Fprintf(&exp, "%s:%d|c\n", name, uint64(i))
		}
	}
	if tconn.DialCount() != 2 {
		t.Errorf("Dial count want: %d got: %d", 2, tconn.DialCount())
	}

	if err := conn.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), exp.Bytes()) {
		t.Errorf("ConnSink: want: %d got: %d ", len(exp.Bytes()), len(buf.Bytes()))
		// t.Errorf("ConnSink: want:\n%s\ngot:\n%s\n", exp.Bytes(), buf.Bytes())
	}

	// make sure the writer was reset
	buf.Reset()
	exp.Reset()
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("name-%d", i)
		conn.FlushCounter(name, uint64(i))
		fmt.Fprintf(&exp, "%s:%d|c\n", name, uint64(i))
	}
	if tconn.DialCount() != 2 {
		t.Errorf("Dial count want: %d got: %d", 2, tconn.DialCount())
	}
	if err := conn.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), exp.Bytes()) {
		t.Errorf("ConnSink Reset Failed: want: %d got: %d ", len(exp.Bytes()), len(buf.Bytes()))
		// t.Errorf("ConnSink: want:\n%s\ngot:\n%s\n", exp.Bytes(), buf.Bytes())
	}
}

func waitForConnStatus(t *testing.T, conn *ConnSink, exp connStatus, to time.Duration) {
	t.Helper()
	start := time.Now()
	for {
		status := conn.Status()
		if status == exp {
			t.Logf("WaitForStatus (%d): %s", exp, time.Since(start))
			return
		}
		if time.Since(start) > to {
			t.Fatalf("Waiting for status: want: %d got: %d", exp, status)
		}
		time.Sleep(time.Millisecond)
	}
}

// WARN WARN WARN WARN WARN WARN WARN WARN WARN WARN
//
// THIS IS WAY TOO SLOW
//
// WARN WARN WARN WARN WARN WARN WARN WARN WARN WARN
func TestConnSink_Parallel_Reconnect(t *testing.T) {
	var buf bytes.Buffer
	tconn := &TestConn{
		w: &buf,
	}
	opts := Options{
		ReconnectWait: 1,
		CustomDialer:  tconn,
	}
	conn, err := opts.Connect()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	tconn.writeFn = func(p []byte) (int, error) {
		n++
		tconn.writeFn = nil
		return 0, errors.New("wat")
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			name := fmt.Sprintf("name-%d", i)
			conn.FlushCounter(name, uint64(i))
		}(i)
	}
	close(start)
	wg.Wait()

	if err := conn.FlushTimeout(time.Millisecond * 50); err != nil {
		t.Error("Flush:", err)
	}

	// Adding a sleep here makes the test pass...
	// time.Sleep(time.Millisecond * 5)
	waitForConnStatus(t, conn, statusConnected, time.Second/10)
	if tconn.DialCount() != 2 {
		t.Errorf("Dial count want: %d got: %d", 2, tconn.DialCount())
	}
}

/*
type StdLogger struct {
	log log.Logger
}

func (l StdLogger) Debug(msg ...interface{}) {
	args := append([]interface{}{"[DEBUG]"}, msg...)
	l.log.Println(args...)
}

func (l StdLogger) Info(msg ...interface{}) {
	args := append([]interface{}{"[INFO]"}, msg...)
	l.log.Println(args...)
}

func (l StdLogger) Warn(msg ...interface{}) {
	args := append([]interface{}{"[WARN]"}, msg...)
	l.log.Println(args...)
}

func (l StdLogger) Error(msg ...interface{}) {
	args := append([]interface{}{"[ERROR]"}, msg...)
	l.log.Println(args...)
}

func (l StdLogger) Panic(msg ...interface{}) {
	args := append([]interface{}{"[PANIC]"}, msg...)
	l.log.Println(args...)
}

func (l StdLogger) Fatal(msg ...interface{}) {
	args := append([]interface{}{"[FATAL]"}, msg...)
	l.log.Println(args...)
}
*/

type nopWriter struct{}

func (n nopWriter) Write(p []byte) (int, error) { return len(p), nil }

func newBenchmarkConnSink(tb testing.TB) (*ConnSink, *TestConn) {
	tb.Helper()
	tconn := &TestConn{
		w: nopWriter{},
	}
	opts := Options{
		CustomDialer:  tconn,
		FlushInterval: time.Millisecond,
	}
	conn, err := opts.Connect()
	if err != nil {
		tb.Fatal(err)
	}
	return conn, tconn
}

func BenchmarkConnSink(b *testing.B) {
	const name = "prefix" + ".__foo=blah_blah.__q=p"
	conn, tconn := newBenchmarkConnSink(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.FlushCounter(name, uint64(i))
		// conn.Flush()
	}
	if tconn.DialCount() != 1 {
		b.Errorf("DialCount: want: %d got: %d", 1, tconn.DialCount())
	}
}

func BenchmarkConnSink_Parallel(b *testing.B) {
	const name = "prefix" + ".__foo=blah_blah.__q=p"
	conn, tconn := newBenchmarkConnSink(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		u := uint64(0)
		for pb.Next() {
			conn.FlushCounter(name, u)
			u++
		}
	})
	if tconn.DialCount() != 1 {
		b.Errorf("DialCount: want: %d got: %d", 1, tconn.DialCount())
	}
}

/*
type WriterConn struct {
	w io.Writer
}

func NewWriterConn(w io.Writer) *WriterConn {
	return &WriterConn{w}
}

func (c *WriterConn) Read(b []byte) (n int, err error) {
	panic("not implemented")
}

func (c *WriterConn) Write(b []byte) (n int, err error) {
	return c.w.Write(b)
}

func (c *WriterConn) Close() error {
	if wc, ok := c.w.(io.WriteCloser); ok {
		return wc.Close()
	}
	return nil
}

func (c *WriterConn) LocalAddr() net.Addr  { panic("not implemented") }
func (c *WriterConn) RemoteAddr() net.Addr { panic("not implemented") }

func (*WriterConn) SetDeadline(_ time.Time) error      { return nil }
func (*WriterConn) SetReadDeadline(_ time.Time) error  { return nil }
func (*WriterConn) SetWriteDeadline(_ time.Time) error { return nil }
*/
