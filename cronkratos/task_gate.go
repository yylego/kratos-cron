package cronkratos

import (
	"runtime/debug"
	"sync/atomic"
	"time"
)

// wrapRecoverable wraps cmd with a deferred recover that logs the panic via Server.slog.
// AddFunc applies this to scheduled and startup runs when panic catch is enabled.
// wrapRecoverable 给 cmd 包一层 defer recover，通过 Server.slog 记录 panic。
// AddFunc 在启用异常捕获时，将此包装同时用于定时与启动执行。
func (s *Server) wrapRecoverable(spec, name string, cmd func()) func() {
	return func() {
		defer func() {
			if rec := recover(); rec != nil {
				s.slog.Error("cron task panic", "spec", spec, "name", name, "reason", rec, "stack", string(debug.Stack()))
			}
		}()
		cmd()
	}
}

// wrapSkipRunning lets one run of this registration go at a time; a run that comes while one is going logs a skip and returns.
// wrapSkipRunning 让同一注册项同时只跑一次；有一次正在跑时，新来的那次记一条跳过日志后直接返回。
func (s *Server) wrapSkipRunning(spec, name string, cmd func()) func() {
	var active atomic.Pointer[time.Time]
	return func() {
		started := time.Now()
		for {
			if since := active.Load(); since != nil {
				s.slog.Info("cron task skipped", "spec", spec, "name", name, "elapsed", time.Since(*since))
				return
			}
			if active.CompareAndSwap(nil, &started) {
				break
			}
		}
		defer active.Store(nil)
		cmd()
	}
}
