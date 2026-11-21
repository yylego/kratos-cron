package cronkratos_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yylego/kratos-cron/cronkratos"
)

type countLock struct{ held int }

func (m *countLock) Lock()   { m.held++ }
func (m *countLock) Unlock() { m.held-- }

func TestDistinctStagesAcquireOwnLocks(t *testing.T) {
	a, b := &countLock{}, &countLock{}
	first, second := cronkratos.NewStage(a), cronkratos.NewStage(b)
	first.Do(context.Background(), func(ctx context.Context) {
		second.Do(ctx, func(ctx context.Context) {
			require.Equal(t, 1, a.held)
			require.Equal(t, 1, b.held)
			first.Do(ctx, func(context.Context) { require.Equal(t, 1, a.held) })
		})
	})
	require.Zero(t, a.held)
	require.Zero(t, b.held)
}

// TestDoSkipsDoneContext verifies Do skips both the lock and fn once ctx is done, nested calls included.
//
// TestDoSkipsDoneContext 验证 ctx 已结束时 Do 不加锁也不执行 fn，嵌套调用同样如此。
func TestDoSkipsDoneContext(t *testing.T) {
	lock := &countLock{}
	stage := cronkratos.NewStage(lock)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, stage.Do(ctx, func(context.Context) { t.Fatal("fn ran on a done ctx") }))
	require.Zero(t, lock.held)

	require.True(t, stage.Do(context.Background(), func(ctx context.Context) {
		nested, stop := context.WithCancel(ctx)
		stop()
		require.False(t, stage.Do(nested, func(context.Context) { t.Fatal("nested fn ran on a done ctx") }))
		require.True(t, stage.Do(ctx, func(context.Context) {}))
	}))
	require.Zero(t, lock.held)
}

// TestDoQueuedBehindStop verifies a Do queued behind a pending write lock, as Stop holds,
// sees the cancellation made while that lock is held and skips fn.
//
// TestDoQueuedBehindStop 验证排在写锁(即 Stop)后面的 Do 拿到锁时能看到写锁内的取消，不执行 fn。
func TestDoQueuedBehindStop(t *testing.T) {
	mutex := &sync.RWMutex{}
	stage := cronkratos.NewStage(mutex.RLocker())
	ctx, cancel := context.WithCancel(context.Background())

	entered, release := make(chan struct{}), make(chan struct{})
	go stage.Do(ctx, func(context.Context) {
		close(entered)
		<-release
	})
	<-entered
	stopped := make(chan struct{})
	go func() {
		mutex.Lock()
		cancel()
		mutex.Unlock()
		close(stopped)
	}()
	time.Sleep(20 * time.Millisecond) // the write lock is now pending // 写锁此时已在排队
	queued := make(chan bool, 1)
	go func() {
		queued <- stage.Do(ctx, func(context.Context) { t.Error("queued fn ran past Stop") })
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	require.False(t, <-queued)
	<-stopped
}

// TestSleepContext verifies SleepContext waits the whole span, and returns the ctx cause at once when ctx ends.
//
// TestSleepContext 验证 SleepContext 正常睡满，ctx 结束时立即返回原因。
func TestSleepContext(t *testing.T) {
	start := time.Now()
	require.NoError(t, cronkratos.SleepContext(context.Background(), 30*time.Millisecond))
	require.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, cronkratos.SleepContext(ctx, time.Hour), context.Canceled)

	ctx, cancel = context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	start = time.Now()
	require.ErrorIs(t, cronkratos.SleepContext(ctx, time.Hour), context.Canceled)
	require.Less(t, time.Since(start), time.Second)
}

// TestNonStage_DoesNotBlock verifies NonStage's Do runs fn inline without
// blocking — noopMutex.Lock/Unlock are no-ops
//
// TestNonStage_DoesNotBlock 验证 NonStage 的 Do 不会阻塞 — noopMutex 的 Lock/Unlock 都是空操作
func TestNonStage_DoesNotBlock(t *testing.T) {
	stage := cronkratos.NonStage()
	var ran atomic.Int32
	stage.Do(context.Background(), func(_ context.Context) {
		ran.Store(1)
	})
	require.Equal(t, int32(1), ran.Load())
}

// TestNonStage_NestedDoOK verifies nested Do on a NonStage works -
// the ctx-tag nested-Do guard kicks in same as on a standard Stage
//
// TestNonStage_NestedDoOK 验证 NonStage 嵌套 Do 没问题 — ctx 标记防嵌套机制照常生效
func TestNonStage_NestedDoOK(t *testing.T) {
	stage := cronkratos.NonStage()
	var nestRan atomic.Int32
	stage.Do(context.Background(), func(ctx context.Context) {
		stage.Do(ctx, func(_ context.Context) {
			nestRan.Store(1)
		})
	})
	require.Equal(t, int32(1), nestRan.Load())
}
