package stats

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// TODO (CEV): Consider removing these

// A Sink is used by a Store to flush its data.
// These functions may buffer the given data.
type Sink interface {
	FlushCounter(name string, value uint64)
	FlushGauge(name string, value uint64)
	FlushTimer(name string, value float64)
}

// FlushableSink is an extension of Sink that provides a Flush() function that
// will flush any buffered stats to the underlying store.
type FlushableSink interface {
	Sink
	Flush()
}

// The Settings type is used to configure gostats. gostats uses environment
// variables to setup its settings.
type Settings struct {
	// Use statsd as a stats sink.
	UseStatsd bool `envconfig:"USE_STATSD" default:"true"`
	// Address where statsd is running at.
	StatsdHost string `envconfig:"STATSD_HOST" default:"localhost"`
	// Port where statsd is listening at.
	StatsdPort int `envconfig:"STATSD_PORT" default:"8125"`
	// Flushing interval.
	FlushIntervalS int `envconfig:"GOSTATS_FLUSH_INTERVAL_SECONDS" default:"5"`
}

type Logger interface {
	Debug(msg ...interface{})
	Info(msg ...interface{})
	Warn(msg ...interface{})
	Error(msg ...interface{})
	Panic(msg ...interface{})
	Fatal(msg ...interface{})
}

// TODO: organize
const (
	DefaultBufferSize       = 32 * 1024
	DefaultDialTimeout      = time.Second
	DefaultDrainTimeout     = time.Second * 10
	DefaultFlushInterval    = time.Second
	DefaultMaxRetries       = 10
	DefaultProtocol         = "tcp"
	DefaultReconnectBufSize = 64 * 1024 * 1024 // 64MB
	DefaultReconnectWait    = time.Second
	DefaultStatsdPort       = 8125
	defaultStatsdPortString = "8125"
	DefaultWriteTimeout     = time.Millisecond * 100
	DefaultURL              = "tcp://127.0.0.1:8125"
	defaultWriteBufferSize  = 32 * 1024
	flushChanSize           = 1024
	maxReconnectAttempts    = 60
)

var (
	ErrConnectionClosed     = errors.New("stats: connection closed")
	ErrReconnectBufExceeded = errors.New("stats: reconnect buffer size exceeded")
	ErrInvalidTimeout       = errors.New("stats: invalid timeout duration")
	ErrTimeout              = errors.New("stats: timeout")
)

// TODO: remove if unused
//
// Option is a function on the options for a connection.
type Option func(*Options) error

// CustomDialer can be used to specify any dialer, not necessarily
// a *net.Dialer.
type CustomDialer interface {
	Dial(network, address string) (net.Conn, error)
}

// TODO (CEV): organize options
//
// Options can be used to create a customized connection.
type Options struct {
	// URL is the statsd server the client will connect and send stats to.
	// If empty DefaultURL is used.
	URL string

	// The communication protocol used to send messages ("tcp", "upd")
	// Protocol is the network protocol used to send messages.  It will
	// only be used if URL does specify a protocol.  The default is TCP.
	Protocol string

	// ReconnectWait sets the time to backoff after attempting a reconnect
	// to a server that we were already connected to previously.
	ReconnectWait time.Duration

	// DialTimeout sets the timeout for a Dial operation on a connection.
	DialTimeout time.Duration

	// DrainTimeout sets the timeout for a Drain Operation to complete.
	DrainTimeout time.Duration

	// WriteTimeout is the maximum time to wait for write operations to the
	// statsd server to complete.
	WriteTimeout time.Duration

	// TODO: add an AlwaysFlush option
	//
	// FlushInterval sets the interval at which buffered stats will be flushed
	// to the server.
	FlushInterval time.Duration

	// ReconnectBufSize is the size of the backing bufio during reconnect.
	// Once this has been exhausted publish operations will return an error.
	ReconnectBufSize int

	// TODO:
	//  * Add an example using a buffer, logger or null sink.
	//  * Note that URL can be omitted since the DefaultURL will be used.
	//
	// CustomDialer allows specifying a custom dialer.  This allows using a
	// dialer that is not necessarily a *net.Dialer.
	CustomDialer CustomDialer

	// TODO: set a no-op logger
	Log Logger

	// NoRandomizeHosts configures whether we randomize the order of hosts when
	// connecting.
	NoRandomizeHosts bool
}

func (o Options) Connect() (*ConnSink, error) {
	cs := &ConnSink{Opts: o}
	if cs.Opts.URL == "" {
		cs.Opts.URL = DefaultURL
	}
	if cs.Opts.Protocol == "" {
		cs.Opts.Protocol = DefaultProtocol
	}
	if cs.Opts.ReconnectWait <= 0 {
		cs.Opts.ReconnectWait = DefaultReconnectWait
	}
	if cs.Opts.DialTimeout <= 0 {
		cs.Opts.DialTimeout = DefaultDialTimeout
	}
	if cs.Opts.DrainTimeout <= 0 {
		cs.Opts.DrainTimeout = DefaultDrainTimeout
	}
	// ensure there is always a write timeout
	if cs.Opts.WriteTimeout <= 0 {
		cs.Opts.WriteTimeout = DefaultWriteTimeout
	}
	if cs.Opts.FlushInterval <= 0 {
		cs.Opts.FlushInterval = DefaultFlushInterval
	}
	if cs.Opts.ReconnectBufSize <= 0 {
		cs.Opts.ReconnectBufSize = DefaultReconnectBufSize
	}
	if err := cs.connect(); err != nil {
		return nil, err
	}
	return cs, nil
}

// WARN: make sure we use all of these
type connStatus int32

// WARN: make sure we use all of these
const (
	statusDisconnected connStatus = 1 << iota
	statusConnected
	statusClosed
	statusReconnecting
	statusConnecting // WARN: remove if not used
)

func (s connStatus) Is(mask connStatus) bool { return s&mask != 0 }
func (s connStatus) Disconnected() bool      { return s == statusDisconnected }
func (s connStatus) Connected() bool         { return s == statusConnected }
func (s connStatus) Closed() bool            { return s == statusClosed }
func (s connStatus) Reconnecting() bool      { return s == statusReconnecting }
func (s connStatus) Connecting() bool        { return s == statusConnecting }

// CEV: thoughts on flushing and avoiding double buffering:
//   * Use two buffers and swap when one is full and needs to be flushed
//   * Pre-emptively flush when the buffer is nearly full (post-write)
type ConnSink struct {
	mu      sync.Mutex
	url     *url.URL
	bw      *bufio.Writer
	pending *bytes.Buffer
	hosts   []string
	conn    net.Conn
	Opts    Options
	status  connStatus // WARN: remove if useless

	// flushing
	wg    sync.WaitGroup
	flush chan chan struct{}
	err   error // WARN (CEV): what actually checks this ???

	// last connect attempt
	lastAttempt time.Time

	// errors
	dropppedBytes int64
}

// TODO: consider removing
func parseURL(rawurl, defaultProtocol string) (*url.URL, error) {
	if !strings.Contains(rawurl, "://") {
		rawurl = fmt.Sprintf("%s://%s", defaultProtocol, rawurl)
	}
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if u.Port() != "" {
		return u, nil
	}

	// try adding default port
	if !strings.HasSuffix(rawurl, ":") {
		rawurl += ":"
	}
	rawurl += defaultStatsdPortString
	u, err = url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if u.Port() != "" {
		return u, nil
	}

	return nil, fmt.Errorf("stats: setting default port for URL: %q", rawurl)
}

func (c *ConnSink) setURL() error {
	u, err := parseURL(c.Opts.URL, c.Opts.Protocol)
	if err == nil {
		c.url = u
	}
	return err
}

func (c *ConnSink) DropppedBytes() int64 {
	return atomic.LoadInt64(&c.dropppedBytes)
}

func (c *ConnSink) connect() error {
	if c.url == nil {
		if err := c.setURL(); err != nil {
			return err
		}
	}
	c.lastAttempt = time.Now()

	// TODO: make sure this works with custom dialers that are not
	// net connections, but are instead buffers/loggers.

	var hosts []string

	// Handle the case where the provided host is not an IP address
	if net.ParseIP(c.url.Hostname()) == nil {
		addrs, _ := net.LookupHost(c.url.Hostname()) // ignore error
		for _, addr := range addrs {
			hosts = append(hosts, net.JoinHostPort(addr, c.url.Port()))
		}
	}

	// Use the provided host.
	if len(hosts) == 0 {
		hosts = append(hosts, c.url.Host)
	}

	if len(hosts) > 1 && !c.Opts.NoRandomizeHosts {
		rand.Shuffle(len(hosts), func(i, j int) {
			hosts[i], hosts[j] = hosts[j], hosts[i]
		})
	}

	dialer := c.Opts.CustomDialer
	if dialer == nil {
		dialer = &net.Dialer{
			Timeout: c.Opts.DialTimeout / time.Duration(len(hosts)),
		}
	}

	var err error
	for _, host := range hosts {
		c.conn, err = dialer.Dial(c.url.Scheme, host)
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}

	if c.pending != nil && c.bw != nil {
		c.bw.Flush() // flush to pending buffer
	}

	w := &timeoutWriter{
		conn:    c.conn,
		timeout: c.Opts.WriteTimeout,
	}
	c.bw = bufio.NewWriterSize(w, defaultWriteBufferSize)

	c.status = statusConnected // WARN: do we want to use Connecting?
	c.startFlushLoop()         // WARN: start flusher

	return nil
}

func (c *ConnSink) FlushTimeout(timeout time.Duration) error {
	// TODO: define these errors
	if timeout <= 0 {
		return ErrInvalidTimeout
	}
	c.mu.Lock()
	status := c.status
	flush := c.flush
	c.mu.Unlock()
	if status.Is(statusClosed) {
		return ErrConnectionClosed
	}

	t := getTimer(timeout)
	ch := make(chan struct{}, 1)

	var err error
	select {
	case flush <- ch:
		select {
		case <-ch:
			close(ch)
		case <-t.C:
			err = ErrTimeout
		}
	case <-t.C:
		err = ErrTimeout
	}
	putTimer(t)
	return err
}

// no error so that we implement gostats Flusher()
func (c *ConnSink) Flush() {
	if err := c.FlushTimeout(time.Second * 10); err != nil {
		if c.Opts.Log != nil {
			c.Opts.Log.Error("[stats] flush: " + err.Error())
		}
	}
}

func (c *ConnSink) startFlushLoop() {
	if c.flush == nil {
		c.flush = make(chan chan struct{}, flushChanSize)
	}
	c.wg.Add(1)
	go c.flushLoop()
}

func sendFlushResponse(ch chan struct{}) {
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (c *ConnSink) flushLoop() {

	defer c.wg.Done()

	c.mu.Lock()
	bw := c.bw
	conn := c.conn
	flush := c.flush
	c.mu.Unlock()

	if bw == nil || conn == nil {
		return
	}

	for ch := range flush {
		c.mu.Lock()
		if !c.status.Connected() || bw != c.bw || conn != c.conn {
			c.mu.Unlock()
			sendFlushResponse(ch)
			return
		}
		var err error
		if bw.Buffered() > 0 {
			err = bw.Flush()
		}
		c.mu.Unlock()

		sendFlushResponse(ch)
		if err != nil {
			c.handleErr(err)
		}
	}
}

func (c *ConnSink) stopFlushLoop() {
	// Signal flusher and wait for exit.  The flusher will exit as our status
	// is no longer 'Connected'.
	select {
	case c.flush <- nil:
	default:
	}
	c.wg.Wait()

	// WARN: we may not want to do this !!!
	//
	// clear pending flushes
	// n := len(c.flush)
	// for i := 0; i < n; i++ {
	// 	select {
	// 	case ch := <-c.flush:
	// 		sendFlushResponse(ch)
	// 	default:
	// 	}
	// }
}

// WARN WARN WARN WARN WARN
// WARN: do not use this
func (c *ConnSink) Status() connStatus {
	c.mu.Lock()
	st := c.status
	c.mu.Unlock()
	return st
}

func (c *ConnSink) Close() error {
	c.mu.Lock()
	if c.status.Is(statusClosed) {
		c.mu.Unlock()
		return nil
	}
	c.status = statusClosed

	c.mu.Unlock()
	c.stopFlushLoop()
	c.mu.Lock()

	if c.conn != nil {
		c.bw.Flush()
		c.conn.Close()
		c.conn = nil
	}

	c.mu.Unlock()
	return nil
}

func (c *ConnSink) reconnect() {

	// Max sleep duration on the first reconnect attempt
	const MaxFirstAttemptDuration = time.Millisecond * 10

	c.stopFlushLoop()

	c.mu.Lock()
	c.err = nil

	var stopFlusher bool

	for i := 0; i < maxReconnectAttempts; i++ {
		var sleep time.Duration
		if d := time.Since(c.lastAttempt); d < c.Opts.ReconnectWait {
			sleep = c.Opts.ReconnectWait - d
		}
		// use a short sleep on the first reconnect attempt
		if i == 0 && sleep > MaxFirstAttemptDuration {
			sleep = MaxFirstAttemptDuration
		}

		c.mu.Unlock()
		if sleep <= 0 {
			runtime.Gosched() // WARN
		} else {
			time.Sleep(sleep)
		}
		if stopFlusher {
			c.stopFlushLoop()
			stopFlusher = false
		}
		c.mu.Lock()

		if c.status.Closed() {
			break // closed before we could reconnect
		}

		// WARN: THIS BLOCKS
		if err := c.connect(); err != nil {
			c.err = nil
			continue
		}

		// WARN WARN WARN WARN WARN WARN WARN
		//
		// Copy the bufio.Writer size then check .Flush() for errors this
		// way we wont discard data on a bad connection.
		//
		// WARN WARN WARN WARN WARN WARN WARN
		//
		// connected
		if c.pending != nil && c.pending.Len() > 0 {
			// TODO: copy in chunks if large???
			c.pending.WriteTo(c.bw)
		}

		// flush the buffer - this will return any write error that
		// occurred while writing the pending buffer
		c.err = c.bw.Flush()
		if c.err != nil {
			// bad reconnect
			c.status = statusReconnecting
			c.bw.Reset(c.pending)
			stopFlusher = true
			continue
		}

		// successfully reconnected
		c.pending = nil
		c.status = statusConnected
		c.mu.Unlock()
		return
	}

	if c.err == nil {
		c.err = errors.New("[stats] failed to reconnect")
	}
	c.mu.Unlock()
	// WARN: Close here ???
}

func (c *ConnSink) handleErr(err error) {
	if err == nil {
		panic("handleErr: called with nil error")
	}
	if c.Opts.Log != nil {
		c.Opts.Log.Error("[stats] write: " + err.Error())
	}

	c.mu.Lock() // TODO (CEV): make sure there isn't a race with c.Opts.Log
	if c.status.Is(statusConnecting | statusClosed | statusReconnecting) {
		c.mu.Unlock()
		return
	}

	if c.status.Connected() {
		c.status = statusReconnecting
		if c.conn != nil {
			// CEV: Flush() won't block if there was a previous write error
			c.bw.Flush()
			// WARN: make sure we don't double count
			atomic.AddInt64(&c.dropppedBytes, int64(c.bw.Buffered()))
			c.conn.Close()
			c.conn = nil
		}
		// TODO: consider using limitedWriter
		c.pending = new(bytes.Buffer)
		c.bw.Reset(c.pending)
		go c.reconnect()
		c.mu.Unlock()
		return
	}

	c.status = statusDisconnected
	c.err = err // WARN (CEV): what actually checks this ???
	c.mu.Unlock()
	c.Close()
}

func (c *ConnSink) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.status.Is(statusDisconnected | statusClosed) {
		c.mu.Unlock()
		return 0, ErrConnectionClosed
	}

	// if reconnecting make sure we haven't exceeded the reconnect
	// buffer size.
	if c.status.Reconnecting() {
		c.bw.Flush()
		if c.pending.Len() >= c.Opts.ReconnectBufSize {
			c.mu.Unlock()
			return 0, ErrReconnectBufExceeded
		}
	}

	// WARN: do not write partial stats - flush before !!!
	n, err := c.bw.Write(p)
	c.mu.Unlock()
	if n < len(p) {
		// WARN: make sure we don't double count
		atomic.AddInt64(&c.dropppedBytes, int64(len(p)-n))
		if err == nil {
			err = io.ErrShortWrite
		}
	}
	if err != nil {
		c.handleErr(err)
	}
	return n, err
}

func (c *ConnSink) flushUint64(name, suffix string, u uint64) error {
	b := ppFree.Get().(*buffer)

	b.WriteString(name)
	b.WriteByte(':')
	b.WriteUnit64(u)
	b.WriteString(suffix)

	_, err := c.Write(*b)

	*b = (*b)[:0]
	ppFree.Put(b)
	return err // TODO (CEV): don't flush - use Flush() instead
}

func (c *ConnSink) flushFloat64(name, suffix string, f float64) error {
	b := ppFree.Get().(*buffer)

	b.WriteString(name)
	b.WriteByte(':')
	b.WriteFloat64(f)
	b.WriteString(suffix)

	_, err := c.Write(*b)

	*b = (*b)[:0]
	ppFree.Put(b)
	return err // TODO (CEV): don't flush - use Flush() instead
}

func (c *ConnSink) FlushCounter(name string, value uint64) {
	c.flushUint64(name, "|c\n", value)
}

func (c *ConnSink) FlushGauge(name string, value uint64) {
	c.flushUint64(name, "|g\n", value)
}

func (c *ConnSink) FlushTimer(name string, value float64) {
	const MaxUint64 float64 = 1<<64 - 1

	// Since we mistakenly use floating point values to represent time
	// durations this method is often passed an integer encoded as a
	// float. Formatting integers is much faster (>2x) than formatting
	// floats so use integer formatting whenever possible.
	//
	if 0 <= value && value < MaxUint64 && math.Trunc(value) == value {
		c.flushUint64(name, "|ms\n", uint64(value))
	} else {
		c.flushFloat64(name, "|ms\n", value)
	}
}

//////////////////////////////////////////////////////////////////

// WARN: use or remove
type limitedWriter struct {
	W *bytes.Buffer // underlying writer
	N int64         // max bytes remaining
	D *int64        // bytes dropped
}

func (l *limitedWriter) dropped(n int64) {
	if l.D != nil && n > 0 {
		atomic.AddInt64(l.D, n)
	}
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	size := int64(len(p))
	if l.N <= 0 {
		l.dropped(size)
		return 0, io.ErrShortWrite // WARN: use a better error
	}
	if size > l.N {
		l.dropped(size - l.N)
		p = p[0:l.N]
	}
	n, err := l.W.Write(p)
	l.N -= int64(n)
	return n, err
}

//////////////////////////////////////////////////////////////////

var timerPool sync.Pool

func getTimer(d time.Duration) *time.Timer {
	if t, _ := timerPool.Get().(*time.Timer); t != nil {
		t.Reset(d)
		return t
	}
	return time.NewTimer(d)
}

func putTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	timerPool.Put(t)
}

//////////////////////////////////////////////////////////////////

type timeoutWriter struct {
	timeout time.Duration
	conn    net.Conn
	err     error
}

func (w *timeoutWriter) Write(b []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	// CEV: ignore deadline errors as there is nothing we
	// can do about them.
	w.conn.SetWriteDeadline(time.Now().Add(w.timeout))
	var n int
	n, w.err = w.conn.Write(b)
	w.conn.SetWriteDeadline(time.Time{})
	return n, w.err
}

//////////////////////////////////////////////////////////////////

const maxRetries = 10

// TODO: add optional logger
// TODO: consider setting write deadline
type netWriter struct {
	conn    net.Conn
	mu      sync.Mutex
	network string
	raddr   string
}

// TODO: maybe rename to 'dial'
func newNetWriter(network, raddr string) (*netWriter, error) {
	w := &netWriter{
		network: network,
		raddr:   raddr,
	}
	if err := w.connect(); err != nil {
		return nil, err
	}
	return w, nil
}

const (
	DialTimeout  = time.Second
	WriteTimeout = time.Millisecond * 500
)

func (w *netWriter) connect() error {
	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}
	c, err := net.DialTimeout(w.network, w.raddr, DialTimeout)
	if err != nil {
		return err
	}
	w.conn = c
	return nil
}

func (w *netWriter) Close() (err error) {
	w.mu.Lock()
	if w.conn != nil {
		err = w.conn.Close()
		w.conn = nil
	}
	w.mu.Unlock()
	return
}

func (w *netWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	n, err := w.writeAndRetry(b, 0)
	w.mu.Unlock()
	return n, err
}

func (w *netWriter) writeAndRetry(b []byte, retryCount int) (n int, err error) {
	if w.conn != nil {
		to := time.Now().Add(WriteTimeout)
		if err = w.conn.SetWriteDeadline(to); err != nil {
			return // WARN: what to do here?
		}
		if n, err = w.conn.Write(b); err == nil {
			// TODO: handle partial writes which
			// may occur if there is a timeout
			return
		}
		// prevent an infinite retry loop
		retryCount++
		if retryCount >= maxRetries {
			return
		}
	}
	// TODO: backoff
	if err = w.connect(); err != nil {
		return
	}
	return w.writeAndRetry(b, retryCount)
}

//////////////////////////////////////////////////////////////////

// TODO (CEV): rename once we remove bufferPool
var ppFree = sync.Pool{
	New: func() interface{} {
		b := make(buffer, 0, 128)
		return &b
	},
}

// Use a fast and simple buffer for constructing statsd messages
type buffer []byte

func (b *buffer) Write(p []byte) {
	*b = append(*b, p...)
}

func (b *buffer) WriteString(s string) {
	*b = append(*b, s...)
}

func (b *buffer) WriteByte(c byte) {
	*b = append(*b, c)
}

func (b *buffer) WriteUnit64(val uint64) {
	var buf [20]byte // big enough for 64bit value base 10
	i := len(buf) - 1
	for val >= 10 {
		q := val / 10
		buf[i] = byte('0' + val - q*10)
		i--
		val = q
	}
	buf[i] = byte('0' + val)
	*b = append(*b, buf[i:]...)
}

func (b *buffer) WriteFloat64(val float64) {
	// TODO: trim precision to 6 ???
	// CEV: fmt uses a precision of 6 by default
	*b = strconv.AppendFloat(*b, val, 'f', -1, 64)
}
