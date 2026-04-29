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
// Nested calls are safe as long as ctx is propagated through the invocation chain -
// the nested Do sees the tag on ctx and skips re-locking:
//
//	stage.Do(ctx, func(ctx context.Context) {
//	    helperCallingDo(ctx, stage)  // nested Do sees the tag, skips re-locking
//	})
//
// Do 检测 ctx 上的"持锁中"标记:首次调用加读锁并把标记注入 ctx 传给 fn;
// 嵌套调用时 ctx 已有标记,直接执行 fn 不再加锁 — 天然避免嵌套死锁
// Stop() 拿写锁会阻塞等 fn 返回,确保 ctx 在 task 中途不失效 —
// 前提是传给 Stop 的 ctx 在 fn 返回前不超时
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
// 嵌套调用安全,前提是 ctx 沿调用链传下去 — 内层 Do 看到标记就跳过加锁:
//
//	stage.Do(ctx, func(ctx context.Context) {
//	    helperCallingDo(ctx, stage)  // 内层 Do 看到标记,跳过重新加锁
//	})
func (s *Stage) Do(ctx context.Context, fn func(ctx context.Context)) {
	if ctx.Value(stageHoldingCtxKey{}) != nil {
		fn(ctx)
		return
	}
	s.rmu.Lock()
	defer s.rmu.Unlock()
	fn(context.WithValue(ctx, stageHoldingCtxKey{}, struct{}{}))
}
