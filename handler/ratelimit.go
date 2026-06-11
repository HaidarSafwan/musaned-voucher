package handler

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type ipLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// RateLimiter enforces a per-IP token-bucket rate limit.
// Stale entries are purged every minute to prevent unbounded memory growth.
type RateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*ipLimiter
	r        rate.Limit
	burst    int
}

func NewRateLimiter(rps, burst int) *RateLimiter {
	rl := &RateLimiter{
		limiters: make(map[string]*ipLimiter),
		r:        rate.Limit(rps),
		burst:    burst,
	}
	go rl.cleanupLoop()
	return rl
}

func (rl *RateLimiter) getLimiter(ip string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	l, ok := rl.limiters[ip]
	if !ok {
		l = &ipLimiter{limiter: rate.NewLimiter(rl.r, rl.burst)}
		rl.limiters[ip] = l
	}
	l.lastSeen = time.Now()
	return l.limiter
}

func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-5 * time.Minute)
		rl.mu.Lock()
		for ip, l := range rl.limiters {
			if l.lastSeen.Before(cutoff) {
				delete(rl.limiters, ip)
			}
		}
		rl.mu.Unlock()
	}
}

// Middleware returns an http.Handler that enforces the rate limit.
// Requests that exceed the limit receive 429 Too Many Requests.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := realIP(r)
		if !rl.getLimiter(ip).Allow() {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]string{"error": "rate limit exceeded"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// realIP extracts the client IP, respecting common reverse-proxy headers.
func realIP(r *http.Request) string {
	if ip := r.Header.Get("X-Real-IP"); ip != "" {
		return strings.TrimSpace(ip)
	}
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		return strings.TrimSpace(strings.SplitN(fwd, ",", 2)[0])
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}
