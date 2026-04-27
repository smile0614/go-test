package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type waiter struct {
	ch   chan string
	done chan struct{}
}

type broker struct {
	mu      sync.Mutex
	queues  map[string][]string
	waiters map[string][]*waiter
}

func newBroker() *broker {
	return &broker{queues: map[string][]string{}, waiters: map[string][]*waiter{}}
}

func (b *broker) put(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.waiters[name]) > 0 {
		w := b.waiters[name][0]
		b.waiters[name] = b.waiters[name][1:]
		select {
		case <-w.done:
			continue
		case w.ch <- msg:
			return
		}
	}
	b.queues[name] = append(b.queues[name], msg)
}

func (b *broker) popNow(name string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queues[name]
	if len(q) == 0 {
		return "", false
	}
	msg := q[0]
	b.queues[name] = q[1:]
	return msg, true
}

func (b *broker) popWait(ctx context.Context, name string) (string, bool) {
	if msg, ok := b.popNow(name); ok {
		return msg, true
	}
	w := &waiter{ch: make(chan string, 1), done: make(chan struct{})}
	b.mu.Lock()
	b.waiters[name] = append(b.waiters[name], w)
	b.mu.Unlock()
	select {
	case msg := <-w.ch:
		return msg, true
	case <-ctx.Done():
		close(w.done)
		b.mu.Lock()
		ws := b.waiters[name]
		for i := range ws {
			if ws[i] == w {
				b.waiters[name] = append(ws[:i], ws[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
		return "", false
	}
}

func main() {
	if len(os.Args) != 2 {
		os.Exit(1)
	}
	b := newBroker()
	h := func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path
		if len(name) < 2 {
			http.NotFound(w, r)
			return
		}
		name = name[1:]
		switch r.Method {
		case http.MethodPut:
			v := r.URL.Query().Get("v")
			if v == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			b.put(name, v)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			t := r.URL.Query().Get("timeout")
			if t == "" {
				msg, ok := b.popNow(name)
				if !ok {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_, _ = w.Write([]byte(msg))
				return
			}
			sec, err := strconv.Atoi(t)
			if err != nil || sec < 0 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), time.Duration(sec)*time.Second)
			defer cancel()
			msg, ok := b.popWait(ctx, name)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(msg))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
	s := &http.Server{Addr: ":" + os.Args[1], Handler: http.HandlerFunc(h)}
	if err := s.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		os.Exit(1)
	}
}
