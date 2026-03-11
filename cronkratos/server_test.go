package cronkratos_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"github.com/yylego/kratos-cron/cronkratos"
	"github.com/yylego/rese"
)

// mockCronServer implements CronServer interface
// Used to test basic cron task registration without lock protection
//
// mockCronServer 实现 CronServer 接口
// 用于测试无锁保护的基础定时任务注册
type mockCronServer struct {
	count int32
}

// RegisterCron adds cron task that runs each second
// RegisterCron 注册每秒执行的定时任务
func (m *mockCronServer) RegisterCron(ctx context.Context, c *cron.Cron) {
	// Run each second
	// 每秒执行一次
	rese.C1(c.AddFunc("* * * * * *", func() {
		m.Run(ctx)
	}))
}

// Run executes the cron task business logic
// Run 执行定时任务业务逻辑
func (m *mockCronServer) Run(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	atomic.AddInt32(&m.count, 1)
}

// TestRegisterCronServer tests basic cron task registration and execution
// Verifies tasks execute at expected intervals via CronServer interface
//
// TestRegisterCronServer 测试基础定时任务注册和执行
// 验证通过 CronServer 接口注册的任务按预期间隔执行
func TestRegisterCronServer(t *testing.T) {
	c := cron.New(cron.WithSeconds())
	srv := cronkratos.NewServer(c, log.DefaultLogger)
	mock := &mockCronServer{}

	cronkratos.RegisterCronServer(srv, mock)

	err := srv.Start(context.Background())
	require.NoError(t, err)

	time.Sleep(2500 * time.Millisecond)

	err = srv.Stop(context.Background())
	require.NoError(t, err)

	cnt := atomic.LoadInt32(&mock.count)
	require.GreaterOrEqual(t, cnt, int32(2))
	t.Logf("cron task execute times: %d", cnt)
}
