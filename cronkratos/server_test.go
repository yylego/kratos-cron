package cronkratos_test

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"github.com/yylego/kratos-cron/cronkratos"
	"github.com/yylego/rese"
)

// TestBasicTwoTasks runs the most common scenario: two cron tasks side—side,
// one with DoOnStartup and one without, verifying both fire as expected
//
// TestBasicTwoTasks 跑最常见场景:两个 cron 任务并存,一个带 DoOnStartup 一个不带,验证都按预期触发
func TestBasicTwoTasks(t *testing.T) {
	srv := cronkratos.NewServer(cron.New(cron.WithSeconds()), slog.Default(),
		cronkratos.WithRecover(),
	)

	var startupCount, scheduledCount int32

	rese.C1(srv.AddFunc("* * * * * *", func(_ context.Context, _ *cronkratos.Stage) {
		atomic.AddInt32(&startupCount, 1)
	}, cronkratos.DoOnStartup()))

	rese.C1(srv.AddFunc("* * * * * *", func(_ context.Context, _ *cronkratos.Stage) {
		atomic.AddInt32(&scheduledCount, 1)
	}))

	require.NoError(t, srv.Start(context.Background()))
	time.Sleep(2500 * time.Millisecond)
	require.NoError(t, srv.Stop(context.Background()))

	require.GreaterOrEqual(t, atomic.LoadInt32(&startupCount), int32(3))
	require.GreaterOrEqual(t, atomic.LoadInt32(&scheduledCount), int32(2))
}

// TestStageCoordinatesShutdown verifies the three-step clean shutdown -
// Stage.Do holds the read-lock so Stop waits—fn returns before cancelling ctx
//
// TestStageCoordinatesShutdown 验证三步优雅关停 — Stage.Do 持读锁,Stop 等 fn 返回才 cancel ctx
func TestStageCoordinatesShutdown(t *testing.T) {
	srv := cronkratos.NewServer(cron.New(cron.WithSeconds()), slog.Default())

	taskRunning := make(chan struct{})
	var taskOnce sync.Once
	var ctxValidAtEnd int32

	rese.C1(srv.AddFunc("* * * * * *", func(ctx context.Context, stage *cronkratos.Stage) {
		stage.Do(ctx, func(ctx context.Context) {
			taskOnce.Do(func() { close(taskRunning) })
			time.Sleep(800 * time.Millisecond)
			if ctx.Err() == nil {
				atomic.StoreInt32(&ctxValidAtEnd, 1)
			}
		})
	}))

	require.NoError(t, srv.Start(context.Background()))
	<-taskRunning
	require.NoError(t, srv.Stop(context.Background()))
	require.Equal(t, int32(1), atomic.LoadInt32(&ctxValidAtEnd))
}

// TestStopProceedsAfterCtxTimeout covers the edge case where Stop's ctx
// times out mid-shutdown - Stop continues to acquire the write lock and
// cancel (Stop ignores the timeout to skip the wait; mutex.Lock waits
// on the in-flight task to release the read-lock regardless)
//
// TestStopProceedsAfterCtxTimeout 边界场景:Stop 的 ctx 中途超时,Stop 仍继续走完拿写锁 + cancel
// (Stop 并不会因 timeout 提前返回,mutex.Lock 必然等业务读锁释放)
func TestStopProceedsAfterCtxTimeout(t *testing.T) {
	srv := cronkratos.NewServer(cron.New(cron.WithSeconds()), slog.Default())

	taskRunning := make(chan struct{})
	var taskOnce sync.Once

	rese.C1(srv.AddFunc("* * * * * *", func(ctx context.Context, stage *cronkratos.Stage) {
		stage.Do(ctx, func(_ context.Context) {
			taskOnce.Do(func() { close(taskRunning) })
			time.Sleep(800 * time.Millisecond)
		})
	}))

	require.NoError(t, srv.Start(context.Background()))
	<-taskRunning

	stopCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.NoError(t, srv.Stop(stopCtx))
	require.GreaterOrEqual(t, time.Since(start), 400*time.Millisecond)
}

// TestStageNestedDoNoDeadlock verifies the ctx-tag nested-Do guard: nested
// Stage.Do within the same invocation chain skips re-locking, so it cannot deadlock
// against a concurrent Lock() pending on RWMutex
//
// TestStageNestedDoNoDeadlock 验证 ctx 标记防嵌套机制:同一调用链上嵌套 Stage.Do 跳过重复加锁,
// 不会跟并发写锁撞死
func TestStageNestedDoNoDeadlock(t *testing.T) {
	srv := cronkratos.NewServer(cron.New(cron.WithSeconds()), slog.Default())

	var innerRan int32
	rese.C1(srv.AddFunc("* * * * * *", func(ctx context.Context, stage *cronkratos.Stage) {
		stage.Do(ctx, func(ctx context.Context) {
			stage.Do(ctx, func(_ context.Context) {
				atomic.StoreInt32(&innerRan, 1)
			})
		})
	}))

	require.NoError(t, srv.Start(context.Background()))
	time.Sleep(1500 * time.Millisecond)
	require.NoError(t, srv.Stop(context.Background()))
	require.Equal(t, int32(1), atomic.LoadInt32(&innerRan))
}

// TestServerLevelWithRecover verifies WithRecover() catches panics from each
// task added via AddFunc, so panicking tasks do not crash the process
// and the cron schedule keeps firing
//
// TestServerLevelWithRecover 验证 WithRecover() 兜住所有 task 的 panic,进程不崩,调度照常
func TestServerLevelWithRecover(t *testing.T) {
	srv := cronkratos.NewServer(cron.New(cron.WithSeconds()), slog.Default(),
		cronkratos.WithRecover(),
	)

	var fired int32
	rese.C1(srv.AddFunc("* * * * * *", func(_ context.Context, _ *cronkratos.Stage) {
		atomic.AddInt32(&fired, 1)
		panic("deliberate panic — service-side recover should catch this")
	}))

	require.NoError(t, srv.Start(context.Background()))
	time.Sleep(2500 * time.Millisecond)
	require.NoError(t, srv.Stop(context.Background()))

	require.GreaterOrEqual(t, atomic.LoadInt32(&fired), int32(2))
}
