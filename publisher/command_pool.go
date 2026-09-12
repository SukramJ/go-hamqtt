// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"hash/fnv"
	"log/slog"
	"sync"
)

// commandPool runs command handlers on a fixed set of workers so the
// transport's read loop can hand off blocking work and return.
//
// The pool is this package's second dispatcher and deliberately not the
// first. [dispatcher], which carries the birth resync, collapses a burst
// onto one pending job — correct there, because every job it carries is the
// same idempotent replay. A command is the opposite: each one is distinct,
// none may be dropped, and two for the same entity must keep their order.
//
// Same-key jobs land on the same worker, chosen by an FNV-1a hash of the
// key, so per-key submission order is preserved while different keys run
// concurrently. The router keys on the inbound topic, which is the identity
// a consumer actually cares about — two writes to one entity never reorder,
// two entities never wait for each other.
type commandPool struct {
	log       *slog.Logger
	queues    []*commandQueue
	softDepth int
	wg        sync.WaitGroup
}

// commandQueue is one worker's FIFO backlog: a mutex plus a condition
// variable over a slice, deliberately not a buffered channel.
//
// A buffered channel parks its sender once full, and the sender here is the
// transport's read loop — the same goroutine that delivers the
// acknowledgement a parked worker is waiting for. Waiting for room would
// therefore wait for a worker that can only make room once the waiter
// returns: the in-flight publish runs into its ack timeout, PINGRESP goes
// unread, and the keep-alive watchdog drops a link that was never actually
// broken. An unbounded slice trades memory for that deadlock, and logs when
// the trade starts happening.
type commandQueue struct {
	mu     sync.Mutex
	ready  *sync.Cond
	jobs   []func()
	closed bool
}

func newCommandQueue() *commandQueue {
	q := &commandQueue{}
	q.ready = sync.NewCond(&q.mu)
	return q
}

func (q *commandQueue) push(job func()) (depth int, accepted bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0, false
	}
	q.jobs = append(q.jobs, job)
	q.ready.Signal()
	return len(q.jobs), true
}

// pop blocks until a job is available. ok is false once the queue is both
// closed and drained, which ends the worker.
func (q *commandQueue) pop() (job func(), ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.jobs) == 0 && !q.closed {
		q.ready.Wait()
	}
	if len(q.jobs) == 0 {
		return nil, false
	}
	job = q.jobs[0]
	q.jobs[0] = nil // let the closure go while the slice header lives on
	q.jobs = q.jobs[1:]
	return job, true
}

func (q *commandQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.ready.Broadcast()
}

// newCommandPool starts workers goroutines. Both counts are clamped to at
// least one, so a misconfigured caller gets a working if serial pool rather
// than a panic at the far end of its boot.
func newCommandPool(workers, softDepth int, log *slog.Logger) *commandPool {
	if workers < 1 {
		workers = 1
	}
	if softDepth < 1 {
		softDepth = 1
	}
	p := &commandPool{log: log, queues: make([]*commandQueue, workers), softDepth: softDepth}
	for i := range p.queues {
		p.queues[i] = newCommandQueue()
		p.wg.Add(1)
		go p.work(p.queues[i])
	}
	return p
}

func (p *commandPool) work(q *commandQueue) {
	defer p.wg.Done()
	for {
		job, ok := q.pop()
		if !ok {
			return
		}
		job()
	}
}

// enqueue hands job to the worker key hashes to and returns immediately.
//
// It neither blocks nor drops. A backlog past the soft depth logs one
// warning per job — visible proof of backpressure, which is what an
// operator needs to see before the memory does — and runs anyway. After
// close it logs and discards, because no worker remains to run it.
func (p *commandPool) enqueue(key string, job func()) {
	q := p.queues[poolIndex(key, len(p.queues))]
	depth, accepted := q.push(job)
	if !accepted {
		p.log.Warn("publisher.command.dropped_after_close", slog.String("key", key))
		return
	}
	if depth > p.softDepth {
		p.log.Warn("publisher.command.backlog",
			slog.String("key", key), slog.Int("depth", depth))
	}
}

// close stops accepting jobs and blocks until every worker has drained its
// queue and exited. Idempotent, and a no-op on a nil pool — a router that
// never started has none.
func (p *commandPool) close() {
	if p == nil {
		return
	}
	for _, q := range p.queues {
		q.close()
	}
	p.wg.Wait()
}

// flush blocks until every job enqueued before this call has run, by driving
// a sentinel through each queue. A no-op once close has run, and on a nil
// pool.
func (p *commandPool) flush() {
	if p == nil {
		return
	}
	dones := make([]chan struct{}, 0, len(p.queues))
	for _, q := range p.queues {
		done := make(chan struct{})
		if _, accepted := q.push(func() { close(done) }); !accepted {
			continue
		}
		dones = append(dones, done)
	}
	for _, done := range dones {
		<-done
	}
}

// poolIndex hashes key to a worker slot in [0, n).
func poolIndex(key string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n)) //nolint:gosec // n is a small worker count
}
