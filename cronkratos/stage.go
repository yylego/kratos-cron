package cronkratos

import (
	"context"
	"sync"
)

// Stage is the task-scope lock-stage aid injected into each cmd added
// via Server.AddFunc. Use Stage.Do to acquire the read-lock around a
// hot section, cooperating with clean shutdown.
//
// Stage 是 Server.AddFunc 注册的每个 task 都会拿到的锁阶段辅助
// 用 Stage.Do 在关键代码段自动持读锁,配合优雅退出协调
type Stage struct {
	rmu sync.Locker
}

// NewStage builds a Stage on the given sync.Locker.
// Server uses NewStage(mutex.RLocker()) inside; on-demand paths that
// bypass shutdown coordination should invoke NonStage instead.
//
// NewStage 用给定的 sync.Locker 构造 Stage
// Server 内部用 NewStage(mutex.RLocker()); 手动触发等不参与协调的路径请用 NonStage
func NewStage(rmu sync.Locker) *Stage {
	return &Stage{rmu: rmu}
}

// NonStage returns a Stage with no-op lock semantics — apt on on-demand
// paths that bypass cron Server's shutdown coordination, where the business
// method's signature requires *Stage but locking is not needed.
//
// NonStage 返回一个 noop Stage — 给手动触发等场景用,不参与 cron Server 协调
// 业务方法签名要求 *Stage 但调用方不需要真正持锁时,直接传 NonStage() 即可
func NonStage() *Stage {
	return NewStage(&noopMutex{})
}

// noopMutex implements sync.Locker with blank Lock/Unlock — supports NonStage.
//
// noopMutex 实现 sync.Locker, Lock/Unlock 都是空操作 — 支撑 NonStage
type noopMutex struct{}

func (*noopMutex) Lock()   {}
func (*noopMutex) Unlock() {}

// stageHoldingCtxKey tags a ctx as having Stage's read-lock on this
// invocation chain. Do uses this tag to short-circuit nested invocations and
// avoid the classic R/W reentrance deadlock with a concurrent Stop.
//
// stageHoldingCtxKey 标记 ctx 在当前调用链上已持有 Stage 读锁
// Do 用它识别嵌套调用,跳过重复加锁,避免跟并发 Stop 撞上经典的 R/W 重入死锁
type stageHoldingCtxKey struct{}

// Do acquires the read-lock when ctx is fresh, runs fn, then releases. When the
// supplied ctx has the "holding" tag (from an outermost Do on the same
// invocation chain), fn runs inline without re-acquiring the lock - this
// avoids the nested-RLock deadlock that would otherwise arise with a
// concurrent Stop. The lock is held just while fn executes; Stop() blocks
// on the write lock—fn returns, preventing ctx invalidation mid-task -
// as long as the ctx supplied to Stop does not time out before this fn returns.
//
// Do 检测 ctx 上的"持锁中"标记:首次调用加读锁并把标记注入 ctx 传给 fn;
// 嵌套调用时 ctx 已有标记,直接执行 fn 不再加锁 — 天然避免嵌套死锁
// Stop() 拿写锁会阻塞等 fn 返回,确保 ctx 在 task 中途不失效 —
// 前提是传给 Stop 的 ctx 在 fn 返回前不超时
//
// Sample pattern (each-iteration locking inside a long-running cron task):
//
//	srv.AddFunc(spec, func(ctx context.Context, stage *cronkratos.Stage) {
//	    for _, item := range list {
//	        stage.Do(ctx, func(ctx context.Context) {
//	            if ctx.Err() != nil { return }
//	            // process item inside the read-lock
//	        })
//	    }
//	})
//
// 典型用法(长任务内每次迭代加解锁):
//
//	srv.AddFunc(spec, func(ctx context.Context, stage *cronkratos.Stage) {
//	    for _, item := range list {
//	        stage.Do(ctx, func(ctx context.Context) {
//	            if ctx.Err() != nil { return }
//	            // 持读锁内处理 item
//	        })
//	    }
//	})
//
// Nested calls are safe as long as ctx is propagated through the invocation chain -
// the nested Do sees the tag on ctx and skips re-locking:
//
//	stage.Do(ctx, func(ctx context.Context) {
//	    helperCallingDo(ctx, stage)  // nested Do sees the tag, skips re-locking
//	})
//
// 嵌套调用安全,前提是 ctx 沿调用链传下去 — 内层 Do 看到标记就跳过加锁:
//
//	stage.Do(ctx, func(ctx context.Context) {
//	    helperCallingDo(ctx, stage)  // 内层 Do 看到标记,跳过重新加锁
//	})
//
// What this prevents: sync.RWMutex.RLock is NOT reentrant — when a concurrent
// Stop has Lock() pending, a second RLock on the same goroutine waits on
// this pending Lock() (RWMutex's anti-starvation guarantee). Outermost
// RLock waits on fn, fn waits on nested RLock, nested RLock waits on
// Lock(), Lock() waits on outermost RLock — deadlock loop. The ctx-tag
// lets nested Do skip re-locking, sidestepping this trap.
//
// 这个机制要解决的问题:sync.RWMutex.RLock 不可重入 — 当并发的 Stop 已经在等
// Lock() 时, 同一 goroutine 第二次 RLock 会被这个 pending Lock() 顶住
// (RWMutex 防写锁饥饿的机制). 外层 RLock 等 fn, fn 等内层 RLock,
// 内层 RLock 等 Lock(), Lock() 等外层 RLock — 死锁闭环
// ctx 标记让内层 Do 跳过加锁,直接绕开这个陷阱
//
// Panic semantics: Stage.Do does NOT deferred-recover panics raised inside
// fn — these propagate out of Do (and out of cmd), where Server's
// Recoverable() / WithRecover() can catch them. The read-lock is not
// leaked on panic: Go runs deferred Unlock during panic stack-unwind,
// releasing the read-lock as the panic propagates. Code past stage.Do in cmd
// does NOT execute on panic (Go unwinds the stack), so no gap exists where
// "panic was recovered upstream and business goes on as if it succeeded".
// Use defer recover inside fn to handle panics as values yourself.
//
// panic 行为:Stage.Do 不内置 deferred recover — fn 内 panic 会 propagate
// 出 Do (出 cmd), 由 Server 的 Recoverable() / WithRecover() 兜住
// 锁不会泄漏:Go 在 panic 栈展开过程中仍会执行 defer 的 Unlock,锁正常释放
// cmd 内 stage.Do 后面的代码在 panic 时不会执行 (Go panic 会 unwind 栈),
// 所以"panic 被 recover 后业务以为成功"的坑不存在
// 业务想把 panic 当值处理, 在 fn 内自己 defer recover 即可
//
// Design rationale: no built-in recover — panic propagation is the safe default,
// letting it unwind out of cmd makes "business goes on as if it succeeded"
// impossible (no code past the panic runs). Catching it via Server's
// wrapRecoverable keeps panic handling in one place (at Server-side scope),
// so Stage stays focused on lock semantics alone. Standard sync primitives
// (sync.Mutex, sync.RWMutex) take the same stance — none catches panics.
//
// 设计取舍:Stage.Do 不内置 recover — panic 直接传播是更安全的默认行为,
// 让 panic unwind 出 cmd, "业务以为成功"的坑根本不会出现 (panic 之后的代码不执行)
// 由 Server 的 wrapRecoverable 统一兜 panic, 把 "panic 处理" 这件事集中在 Server 这一层,
// Stage 保持专注于锁语义本身. Go 标准库的同步原语 (sync.Mutex / sync.RWMutex)
// 也都是这个立场 — 不替调用方兜 panic
func (s *Stage) Do(ctx context.Context, fn func(ctx context.Context)) {
	if ctx.Value(stageHoldingCtxKey{}) != nil {
		fn(ctx)
		return
	}
	s.rmu.Lock()
	defer s.rmu.Unlock()
	fn(context.WithValue(ctx, stageHoldingCtxKey{}, struct{}{}))
}
