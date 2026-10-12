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
	"github.com/yylego/erero"
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

func TestStopBeforeFirstRun(t *testing.T) {
	t.Run("no active task", func(t *testing.T) {
		srv := cronkratos.NewServer(cron.New(cron.WithSeconds()), slog.Default())
		ran := false
		_, err := srv.AddFunc("@every 24h", func(context.Context, *cronkratos.Stage) { ran = true })
		require.NoError(t, err)
		require.NoError(t, srv.Start(context.Background()))
		require.NoError(t, srv.Stop(context.Background()))
		require.False(t, ran)
	})
}

func TestStopAwaitsStepAndTask(t *testing.T) {
	for _, startup := range []bool{false, true} {
		name := "scheduled"
		if startup {
			name = "startup"
		}
		t.Run(name, func(t *testing.T) {
			t.Run("completion", func(t *testing.T) {
				srv := cronkratos.NewServer(cron.New(cron.WithSeconds()), slog.Default())
				entered := make(chan context.Context, 1)
				finishStep := make(chan struct{})
				finishTask := make(chan struct{})
				cleaned := false
				ranNext := false
				opts := []cronkratos.TaskOption{}
				spec := "@every 1s"
				if startup {
					opts = append(opts, cronkratos.DoOnStartup())
					spec = "@every 24h"
				}
				_, err := srv.AddFunc(spec, func(ctx context.Context, stage *cronkratos.Stage) {
					defer func() {
						<-finishTask
						cleaned = true
					}()
					stage.Do(ctx, func(ctx context.Context) {
						entered <- ctx
						<-finishStep
					})
					<-ctx.Done()
					stage.Do(ctx, func(ctx context.Context) {
						if ctx.Err() != nil {
							return
						}
						ranNext = true
					})
				}, opts...)
				require.NoError(t, err)
				require.NoError(t, srv.Start(context.Background()))
				ctx := <-entered
				stopped := make(chan error, 1)
				stopCtx, cancel := context.WithCancel(context.Background())
				cancel()
				go func() { stopped <- srv.Stop(stopCtx) }()
				select {
				case <-stopped:
					t.Fatal("Stop returned before the active step completed")
				case <-time.After(20 * time.Millisecond):
				}
				require.NoError(t, ctx.Err())
				require.Empty(t, stopped)
				close(finishStep)
				<-ctx.Done()
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				select {
				case <-stopped:
					t.Fatal("Stop returned before task cleanup completed")
				case <-time.After(20 * time.Millisecond):
				}
				close(finishTask)
				require.NoError(t, <-stopped)
				require.True(t, cleaned)
				require.False(t, ranNext)
			})
		})
	}
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
			if ctx.Err() != nil {
				return
			}
			atomic.StoreInt32(&ctxValidAtEnd, 1)
		})
	}))

	require.NoError(t, srv.Start(context.Background()))
	<-taskRunning
	require.NoError(t, srv.Stop(context.Background()))
	require.Equal(t, int32(1), atomic.LoadInt32(&ctxValidAtEnd))
}

// TestStopAwaitsCompletionDespiteTimeout checks that a stop deadline does not cut short an active step.
//
// TestStopAwaitsCompletionDespiteTimeout 验证停止期限不会中断当前步骤或让 Stop 提前返回。
func TestStopAwaitsCompletionDespiteTimeout(t *testing.T) {
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
// 避免与并发写锁相互等待
func TestStageNestedDoNoDeadlock(t *testing.T) {
	srv := cronkratos.NewServer(cron.New(cron.WithSeconds()), slog.Default())

	var nestedRan int32
	rese.C1(srv.AddFunc("* * * * * *", func(ctx context.Context, stage *cronkratos.Stage) {
		stage.Do(ctx, func(ctx context.Context) {
			stage.Do(ctx, func(_ context.Context) {
				atomic.StoreInt32(&nestedRan, 1)
			})
		})
	}))

	require.NoError(t, srv.Start(context.Background()))
	time.Sleep(1500 * time.Millisecond)
	require.NoError(t, srv.Stop(context.Background()))
	require.Equal(t, int32(1), atomic.LoadInt32(&nestedRan))
}

// TestServerPanicCatch verifies WithRecover() catches panics from each
// task added via AddFunc, so panicking tasks do not crash the process
// and the cron schedule keeps firing
//
// TestServerPanicCatch 验证 WithRecover() 兜住所有 task 的 panic,进程不崩,调度照常
func TestServerPanicCatch(t *testing.T) {
	srv := cronkratos.NewServer(cron.New(cron.WithSeconds()), slog.Default(),
		cronkratos.WithRecover(),
	)

	var fired int32
	rese.C1(srv.AddFunc("* * * * * *", func(_ context.Context, _ *cronkratos.Stage) {
		atomic.AddInt32(&fired, 1)
		panic(erero.New("panic catch test"))
	}))

	require.NoError(t, srv.Start(context.Background()))
	time.Sleep(2500 * time.Millisecond)
	require.NoError(t, srv.Stop(context.Background()))

	require.GreaterOrEqual(t, atomic.LoadInt32(&fired), int32(2))
}
