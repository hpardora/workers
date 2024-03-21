package workers

import (
	"sync"
)

// WorkMuxLimited is a Beanstalkd Job multiplexer.
// It matches the tube of each incoming job against a list
// of registered tubes and calls the handler of that tube.
type WorkMuxLimited struct {
	mu sync.RWMutex // guards muxEntry
	m  map[string]muxEntryLimited
}

type muxEntryLimited struct {
	h    Handler
	tube string
	sem  chan struct{}
}

// NewWorkMuxLimited allocates and returns a new WorkMux.
func NewWorkMuxLimited() *WorkMuxLimited {
	return &WorkMuxLimited{
		m: make(map[string]muxEntryLimited),
	}
}

// Handle registers the job handler for the given tube.
// If a handler already exists for tube, Handle panics.
func (mux *WorkMuxLimited) Handle(tube string, handler Handler, limit int) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if tube == "" {
		panic("invalid tube")
	}

	if handler == nil {
		panic("nil handler")
	}

	if _, found := mux.m[tube]; found {
		panic("multiple registrations for " + tube)
	}

	sem := make(chan struct{}, limit)

	mux.m[tube] = muxEntryLimited{
		h:    handler,
		tube: tube,
		sem:  sem,
	}
}

// Handler returns the handler to use for the given job. If there is no
// registered handler that applies to the job, Handler returns nil.
func (mux *WorkMuxLimited) Handler(tube string) Handler {
	mux.mu.RLock()
	defer mux.mu.RUnlock()

	if handler, found := mux.m[tube]; found {
		return handler.h
	}

	return nil
}

// Tubes returns a list of tubes handled by the WorkMux.
func (mux *WorkMuxLimited) Tubes() []string {
	mux.mu.RLock()
	defer mux.mu.RUnlock()

	tubes := make([]string, len(mux.m))
	i := 0

	for k := range mux.m {
		tubes[i] = k
		i++
	}

	return tubes
}

// Work dispatches the job to the proper handler. Makes WorkMux Implements
// the Handler interface. Work panics if no handler is defined to handle the
// job.
func (mux WorkMuxLimited) Work(j *Job) {
	h := mux.Handler(j.Tube)

	if h == nil {
		panic("no handler for tube " + j.Tube)
	}
	sem := mux.m[j.Tube].sem
	sem <- struct{}{}
	go func() {
		defer func() { <-sem }()
		h.Work(j)
	}()
}
