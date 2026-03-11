package cronkratos_test

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"github.com/yylego/kratos-cron/cronkratos"
	"github.com/yylego/rese"
)

// mockCronServerL implements CronServerL interface with lock protection
// Used to test cron task addition with clean shutdown coordination
//
// mockCronServerL 实现带锁保护的 CronServerL 接口
// 用于测试支持优雅退出协调的定时任务注册
type mockCronServerL struct {
	count int
	score int
}

// RegisterCron adds cron task that runs each second with lock protection
// RegisterCron 注册每秒执行的定时任务，带锁保护
func (m *mockCronServerL) RegisterCron(ctx context.Context, c *cron.Cron, locker sync.Locker) {
	// Run each second with locker protection
	// 每秒执行一次，带锁保护
	rese.C1(c.AddFunc("* * * * * *", func() {
		m.LoopAddCountAndScore(ctx, locker)
	}))
}

// LoopAddCountAndScore executes the cron task business logic with lock protection
// Holds lock in execution to coordinate with clean shutdown
//
// LoopAddCountAndScore 执行带锁保护的定时任务业务逻辑
// 执行期间持有锁以配合优雅退出
func (m *mockCronServerL) LoopAddCountAndScore(ctx context.Context, locker sync.Locker) {
	for i := 0; i < 10; i++ {
		m.onceAddCountAndScore(ctx, locker)
	}
}

// onceAddCountAndScore executes one operation with lock protection
// Lock is obtained each invocation, not once in the loop, so Stop() can get write lock between iterations
//
// The steps of this function:
//  1. locker.Lock()         - get read lock (RLocker)
//  2. defer locker.Unlock() - release lock when function ends
//  3. ctx.Err() != nil      - check if ctx is cancelled
//
// Stop() execution sequence:
//  1. cron.Stop()   - stop scheduling new tasks
//  2. mutex.Lock()  - get write lock, wait read locks to release
//  3. cancel()      - cancel ctx
//
// cancel() is invoked following write lock acquisition, write lock waits read locks, so if ctx is valid when checked inside lock it won't become invalid
//
// This design ensures one operation atomicity:
//   - complete execution, otherwise exit at checkpoint
//   - avoid half-done data, inconsistent state, not-released resources
//   - ctx won't turn invalid mid-execution
//
// onceAddCountAndScore 执行单次带锁保护的操作
// 每次调用都重新加锁，而非整个循环只加一次锁，让 Stop() 能在迭代间隙获取写锁
//
// 当前函数的逻辑:
//  1. locker.Lock()         - 获取读锁 (RLocker)
//  2. defer locker.Unlock() - 函数结束时释放锁
//  3. ctx.Err() != nil      - 检查 ctx 是否已取消
//
// Stop() 执行顺序:
//  1. cron.Stop()   - 停止调度新任务
//  2. mutex.Lock()  - 获取写锁，等待所有读锁释放
//  3. cancel()      - 取消 ctx
//
// cancel() 在写锁后调用，写锁又等读锁释放，所以锁内检查后若有效则不会变为无效
//
// 这个设计确保单次操作的原子性:
//   - 要么完整执行，要么在检查点退出
//   - 避免数据写一半、状态不一致、资源未释放等问题
//   - ctx 不会在执行过程中突然变为无效
func (m *mockCronServerL) onceAddCountAndScore(ctx context.Context, locker sync.Locker) {
	locker.Lock()
	defer locker.Unlock()
	if ctx.Err() != nil {
		return
	}
	m.count++
	m.score += rand.IntN(100)
}

// TestRegisterCronServerL tests cron task addition with lock protection
// Verifies tasks execute as expected and lock coordinates with clean shutdown
//
// TestRegisterCronServerL 测试带锁保护的定时任务注册
// 验证任务正确执行且锁能配合优雅退出
func TestRegisterCronServerL(t *testing.T) {
	c := cron.New(cron.WithSeconds())
	srv := cronkratos.NewServer(c, log.DefaultLogger)
	mock := &mockCronServerL{}

	cronkratos.RegisterCronServerL(srv, mock)

	err := srv.Start(context.Background())
	require.NoError(t, err)

	time.Sleep(2500 * time.Millisecond)

	err = srv.Stop(context.Background())
	require.NoError(t, err)

	cnt := mock.count
	require.GreaterOrEqual(t, cnt, 2)
	t.Logf("cron task with lock execute times: %d", cnt)

	avg := float64(mock.score) / float64(cnt)
	require.GreaterOrEqual(t, avg, float64(0))
	t.Logf("cron task with lock score average: %v", avg)
}
