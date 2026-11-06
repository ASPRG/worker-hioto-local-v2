package main

import "sync"

// deviceRegistry is a concurrency-safe in-memory cache of registered devices.
// It wraps map[string]Device behind an RWMutex so the AMQP consumer goroutine
// and the HTTP handler goroutines can access it without data races.
type deviceRegistry struct {
	mu sync.RWMutex
	m  map[string]Device
}

func newRegistry(initial []Device) *deviceRegistry {
	r := &deviceRegistry{m: make(map[string]Device, len(initial))}
	for _, d := range initial {
		r.m[d.GUID] = d
	}
	return r
}

// Get returns the device for the given GUID and whether it was found.
func (r *deviceRegistry) Get(guid string) (Device, bool) {
	r.mu.RLock()
	d, ok := r.m[guid]
	r.mu.RUnlock()
	return d, ok
}

// Name returns the device name for the given GUID, or the GUID itself if not found.
func (r *deviceRegistry) Name(guid string) string {
	r.mu.RLock()
	d, ok := r.m[guid]
	r.mu.RUnlock()
	if ok && d.Name != "" {
		return d.Name
	}
	return guid
}

// Set stores or replaces a device.
func (r *deviceRegistry) Set(guid string, d Device) {
	r.mu.Lock()
	r.m[guid] = d
	r.mu.Unlock()
}

// All returns a snapshot of all devices (safe to iterate without holding the lock).
func (r *deviceRegistry) All() []Device {
	r.mu.RLock()
	out := make([]Device, 0, len(r.m))
	for _, d := range r.m {
		out = append(out, d)
	}
	r.mu.RUnlock()
	return out
}
