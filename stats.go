package stats

import (
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// A Store holds statistics.
// There are two options when creating a new store:
//  create a store backed by a tcp_sink to statsd
//  s := stats.NewDefaultStore()
//  create a store with a user provided Sink
//  s := stats.NewStore(sink, true)
// Currently that only backing store supported is statsd via a TCP sink, https://github.com/lyft/gostats/blob/master/tcp_sink.go.
// However, implementing other Sinks (https://github.com/lyft/gostats/blob/master/sink.go) should be simple.
//
// A store holds Counters, Gauges, and Timers. You can add unscoped Counters, Gauges, and Timers to the store
// with:
//  s := stats.NewDefaultStore()
//  c := s.New[Counter|Gauge|Timer]("name")
type Store interface {
	// Flush Counters and Gauges to the Sink attached to the Store.
	// To flush the store at a regular interval call the
	//  Start(*time.Ticker)
	// method on it.
	//
	// The store will flush either at the regular interval, or whenever
	//  Flush()
	// is called. Whenever the store is flushed,
	// the store will call
	//  GenerateStats()
	// on all of its stat generators,
	// and flush all the Counters and Gauges registered with it.
	Flush()

	// Start a timer for periodic stat Flushes.
	Start(*time.Ticker)

	// Add a StatGenerator to the Store that programatically generates stats.
	AddStatGenerator(StatGenerator)
	Scope
}

// A Scope namespaces Statistics.
//  store := stats.NewDefaultStore()
//  scope := stats.Scope("service")
//  // the following counter will be emitted at the stats tree rooted at `service`.
//  c := scope.NewCounter("success")
// Additionally you can create subscopes:
//  store := stats.NewDefaultStore()
//  scope := stats.Scope("service")
//  networkScope := scope.Scope("network")
//  // the following counter will be emitted at the stats tree rooted at service.network.
//  c := networkScope.NewCounter("requests")
type Scope interface {
	// Scope creates a subscope.
	Scope(name string) Scope

	// ScopeWithTags creates a subscope with Tags to a store or scope. All child scopes and metrics
	// will inherit these tags by default.
	ScopeWithTags(name string, tags map[string]string) Scope

	// Store returns the Scope's backing Store.
	Store() Store

	// NewCounter adds a Counter to a store, or a scope.
	NewCounter(name string) Counter

	// NewCounterWithTags adds a Counter with Tags to a store, or a scope.
	NewCounterWithTags(name string, tags map[string]string) Counter

	// NewPerInstanceCounter adds a Per instance Counter with optional Tags to a store, or a scope.
	NewPerInstanceCounter(name string, tags map[string]string) Counter

	// NewGauge adds a Gauge to a store, or a scope.
	NewGauge(name string) Gauge

	// NewGaugeWithTags adds a Gauge with Tags to a store, or a scope.
	NewGaugeWithTags(name string, tags map[string]string) Gauge

	// NewPerInstanceGauge adds a Per instance Gauge with optional Tags to a store, or a scope.
	NewPerInstanceGauge(name string, tags map[string]string) Gauge

	// NewTimer adds a Timer to a store, or a scope.
	NewTimer(name string) Timer

	// NewTimerWithTags adds a Timer with Tags to a store, or a scope with Tags.
	NewTimerWithTags(name string, tags map[string]string) Timer

	// NewPerInstanceTimer adds a Per instance Timer with optional Tags to a store, or a scope.
	NewPerInstanceTimer(name string, tags map[string]string) Timer
}

// A Counter is an always incrementing stat.
type Counter interface {
	// Add increments the Counter by the argument's value.
	Add(uint64)

	// Inc increments the Counter by 1.
	Inc()

	// Set sets an internal counter value which will be written in the next flush.
	// Its use is discouraged as it may break the counter's "always incrementing" semantics.
	Set(uint64)

	// String returns the current value of the Counter as a string.
	String() string

	// Value returns the current value of the Counter as a uint64.
	Value() uint64
}

// A Gauge is a stat that can increment and decrement.
type Gauge interface {
	// Add increments the Gauge by the argument's value.
	Add(uint64)

	// Sub decrements the Gauge by the argument's value.
	Sub(uint64)

	// Inc increments the Gauge by 1.
	Inc()

	// Dec decrements the Gauge by 1.
	Dec()

	// Set sets the Gauge to a value.
	Set(uint64)

	// String returns the current value of the Gauge as a string.
	String() string

	// Value returns the current value of the Gauge as a uint64.
	Value() uint64
}

// A Timespan is used to measure spans of time.
// They measure time from the time they are allocated by a Timer with
//   AllocateSpan()
// until they call
//   Complete()`
// or
//   CompleteWithDuration(time.Duration)
// When either function is called the timespan is flushed.
// When Complete is called the timespan is flushed.
//
// A Timespan can be flushed at function
// return by calling Complete with golang's defer statement.
type Timespan interface {
	// End the Timespan and flush it.
	Complete() time.Duration

	// End the Timespan and flush it. Adds additional time.Duration to the measured time
	CompleteWithDuration(time.Duration)
}

// A Timer is used to flush timing statistics.
type Timer interface {
	// WARN: this API is super broken we need a better way of adding timers !!!
	//
	// AddValue flushs the timer with the argument's value.
	AddValue(float64)

	// AllocateSpan allocates a Timespan.
	AllocateSpan() Timespan
}

// A StatGenerator can be used to programatically generate stats.
// StatGenerators are added to a store via
//  AddStatGenerator(StatGenerator)
// An example is https://github.com/lyft/gostats/blob/master/runtime.go.
type StatGenerator interface {
	// Runs the StatGenerator to generate Stats.
	GenerateStats()
}

var _ Counter = (*counter)(nil)

type counter struct {
	curr uint64
	prev uint64
}

func (c *counter) Inc()         { atomic.AddUint64(&c.curr, 1) }
func (c *counter) Add(i uint64) { atomic.AddUint64(&c.curr, i) }
func (c *counter) Set(i uint64) { atomic.StoreUint64(&c.curr, i) }

func (c *counter) Value() uint64 {
	curr := atomic.LoadUint64(&c.curr)
	prev := atomic.LoadUint64(&c.prev)
	if curr == prev {
		return 0
	}
	// TODO: consider using a loop and CAS
	atomic.SwapUint64(&c.prev, curr)
	return curr - prev
}

func (c *counter) String() string {
	return strconv.FormatUint(c.Value(), 10)
}

type timer struct {
	mu     sync.Mutex
	values []float64
	sink   Sink
}

// WARN: this API is super broken we need a better way of adding timers !!!
func (t *timer) AddValue(value float64) {
	t.mu.Lock()
	t.values = append(t.values, value)
	t.mu.Unlock()
}

type rootStore struct {
	counters sync.Map
}

type store struct {
	counters sync.Map
}

func (s *store) NewCounter(name string) Counter {
	if v, ok := s.counters.Load(name); ok {
		return v.(*counter)
	}
	v, _ := s.counters.LoadOrStore(name, new(counter))
	return v.(*counter)
}

func (s *store) NewCounterWithTags(name string, tags map[string]string) Counter {
	key := serializeTags(name, tags)
	if v, ok := s.counters.Load(key); ok {
		return v.(*counter)
	}
	v, _ := s.counters.LoadOrStore(key, new(counter))
	return v.(*counter)
}

// TODO MOVE TO DIFF FILE

type tagPair struct {
	dimension string
	value     string
}

type tagSet []tagPair

func (t tagSet) Len() int           { return len(t) }
func (t tagSet) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t tagSet) Less(i, j int) bool { return t[i].dimension < t[j].dimension }

func serializeTags(name string, tags map[string]string) string {
	const prefix = ".__"
	const sep = "="

	// switch len(tags) {
	switch len(tags) {
	case 0:
		return name
	case 1:
		for k, v := range tags {
			return name + prefix + k + sep + replaceChars(v)
		}
		panic("unreachable")
	case 2:
		var a, b tagPair
		for k, v := range tags {
			b = a
			a = tagPair{k, replaceChars(v)}
		}
		if a.dimension > b.dimension {
			a, b = b, a
		}
		return name +
			prefix + a.dimension + sep + a.value +
			prefix + b.dimension + sep + b.value
	default:
		// n stores the length of the serialized name + tags
		n := (len(prefix) + len(sep)) * len(tags)
		n += len(name)

		pairs := make(tagSet, 0, len(tags))
		for k, v := range tags {
			n += len(k) + len(v)
			pairs = append(pairs, tagPair{
				dimension: k,
				value:     replaceChars(v),
			})
		}
		sort.Sort(pairs)

		// CEV: this is same as strings.Builder, but works with go1.9 and earlier
		b := make([]byte, 0, n)
		b = append(b, name...)
		for _, tag := range pairs {
			b = append(b, prefix...)
			b = append(b, tag.dimension...)
			b = append(b, sep...)
			b = append(b, tag.value...)
		}
		return *(*string)(unsafe.Pointer(&b))
	}
}

func replaceChars(s string) string {
	var buf []byte // lazily allocated
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '.' || c == ':' || c == '|' {
			if buf == nil {
				buf = []byte(s)
			}
			buf[i] = '_'
		}
	}
	if buf == nil {
		return s
	}
	return string(buf)
}
