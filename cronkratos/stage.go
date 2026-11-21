package cronkratos

import (
	"context"
	"sync"
	"time"
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
// Nested invocations sharing a lock must reuse the same Stage instance.
//
// NewStage 用给定的 sync.Locker 构造 Stage
// Server 内部用 NewStage(mutex.RLocker()); 手动触发等不参与协调的路径请用 NonStage
// 同一把锁的嵌套调用应复用同一个 Stage，不复制或另建包装。
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

// stageHoldingTag tags a ctx as having Stage's read-lock on this
// invocation chain. Do uses this tag to short-circuit nested invocations and
// avoid the classic R/W reentrance deadlock with a concurrent Stop.
//
// stageHoldingTag 标记 ctx 在当前调用链上已持有 Stage 读锁
// Do 用它识别嵌套调用，跳过重复加锁，避免与并发 Stop 相互等待
type stageHoldingTag struct{ stage *Stage }

// Do acquires the read-lock when ctx is fresh, runs fn, then releases. When the
// supplied ctx has this Stage's "holding" tag (from an active enclosing Do on the same
// invocation chain), fn runs inline without re-acquiring the lock - this
// avoids the nested-RLock deadlock that would otherwise arise with a
// concurrent Stop. The lock is held just while fn executes; Stop() blocks
// on the write lock while fn executes, preventing shutdown cancellation mid-task.
// Do skips fn and returns false once ctx is done: it checks before locking and
// once more on acquiring the lock, since a Do queued behind a pending Stop gets
// the lock just as Stop cancels the task context. It returns true when fn ran.
// Check ctx.Err() inside fn: derived context deadlines can expire while fn runs.
// Pass the tagged ctx through synchronous nested calls while the enclosing Do remains active.
// Do not retain this ctx beyond that scope; do not pass it to independent goroutines.
//
// Do 检测 ctx 上的"持锁中"标记:首次调用加读锁并把标记注入 ctx 传给 fn;
// 同一个 Stage 同步嵌套时，ctx 已有对应标记，直接执行 fn 不再加锁。
// Stop() 拿写锁会等 fn 返回，避免关停流程在关键段中途取消上下文。
// ctx 已结束时 Do 不执行 fn 并返回 false：加锁前查一次，拿到锁后再查一次——
// 等在 Stop 后面的 Do 拿到锁时，Stop 刚好取消了任务上下文。fn 执行了返回 true。
// fn 内仍须检查 ctx.Err()：派生的超时期限可能在 fn 执行期间到期。
// 带标记的 ctx 只用于外层 Do 尚未返回时的同步嵌套，不传给独立 goroutine，也不在外层返回后复用。
//
// Sample pattern (each-iteration locking inside a long-running cron task):
//
//	srv.AddFunc(spec, func(ctx context.Context, stage *cronkratos.Stage) {
//	    for _, item := range list {
//	        if !stage.Do(ctx, func(ctx context.Context) {
//	            // process item inside the read-lock
//	        }) {
//	            return
//	        }
//	        if cronkratos.SleepContext(ctx, pause) != nil { return }
//	    }
//	})
//
// 典型用法(长任务内每次迭代加解锁):
//
//	srv.AddFunc(spec, func(ctx context.Context, stage *cronkratos.Stage) {
//	    for _, item := range list {
//	        if !stage.Do(ctx, func(ctx context.Context) {
//	            // 持读锁内处理 item
//	        }) {
//	            return
//	        }
//	        if cronkratos.SleepContext(ctx, pause) != nil { return }
//	    }
//	})
//
// Nested calls are safe as long as ctx is propagated through the invocation chain -
// the nested Do sees the tag on ctx and skips re-locking:
//
//	stage.Do(ctx, func(ctx context.Context) {
//	    processBatch(ctx, stage)  // nested Do sees the tag, skips re-locking
//	})
//
// 嵌套调用安全,前提是 ctx 沿调用链传下去 — 内层 Do 看到标记就跳过加锁:
//
//	stage.Do(ctx, func(ctx context.Context) {
//	    processBatch(ctx, stage)  // 内层 Do 看到标记,跳过重新加锁
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
// 内层 RLock 等 Lock(), Lock() 等外层 RLock — 循环等待
// ctx 标记让内层 Do 跳过加锁,直接绕开这个陷阱
//
// Panic: Do does not recover panics raised inside fn. Such panics propagate out of Do and cmd, where
// Server's Recoverable() / WithRecover() catches them in one place, the same stance as
// sync.Mutex / sync.RWMutex. The deferred Unlock runs during the unwind too, so the read-lock
// is not leaked; the code past stage.Do does not run, so a panic cannot pass as a success.
// Use defer recover inside fn to handle panics as values.
//
// panic:Do 不替 fn 兜 panic，panic 会传出 Do 和 cmd，由 Server 的 Recoverable() / WithRecover()
// 统一兜住，跟 sync.Mutex / sync.RWMutex 的立场一样。栈展开时 defer 的 Unlock 照常执行，读锁不会泄漏；
// stage.Do 后面的代码不会执行，panic 不会被当成成功。想把 panic 当值处理，就在 fn 里自己 defer recover。
func (s *Stage) Do(ctx context.Context, fn func(ctx context.Context)) bool {
	if ctx.Err() != nil {
		return false
	}
	tag := stageHoldingTag{stage: s}
	if ctx.Value(tag) != nil {
		fn(ctx)
		return true
	}
	s.rmu.Lock()
	defer s.rmu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	fn(context.WithValue(ctx, tag, struct{}{}))
	return true
}

// SleepContext waits d and returns nil; once ctx is done it returns ctx.Err() at once.
// Call it between Stage.Do steps: inside Do, Stop waits on the step before
// cancelling ctx, so a sleep there holds up shutdown to its end.
//
// SleepContext 等待 d 后返回 nil；ctx 结束时立即返回 ctx.Err()。
// 放在两次 Stage.Do 之间调用：Do 内部的等待会让 Stop 一直等到它结束才取消 ctx。
func SleepContext(ctx context.Context, d time.Duration) error {
	if cause := ctx.Err(); cause != nil {
		return cause
	}
	wake := time.NewTimer(d)
	defer wake.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake.C:
		return nil
	}
}
