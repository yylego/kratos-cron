package cronkratos

import (
	"context"
	"sync"

	"github.com/robfig/cron/v3"
)

// CronServer defines basic interface to add cron service (no lock protection)
// Service added via this interface cannot coordinate with clean shutdown
//
// CronServer 定义基础的定时任务注册接口（无锁保护）
// 通过此接口注册的任务无法与优雅退出协调
type CronServer interface {
	RegisterCron(ctx context.Context, c *cron.Cron)
}

// RegisterCronServer registers cron service from CronServer to Server
// Passes context and cron instance to the service
//
// RegisterCronServer 将 CronServer 的定时任务注册到 Server
// 传递上下文和 cron 实例给服务
func RegisterCronServer(srv *Server, service CronServer) {
	service.RegisterCron(srv.ctx, srv.cron)
}

// CronServerL defines interface with read-lock to support clean shutdown
// Service should hold the lock in execution to prevent premature exit
//
// CronServerL 定义带读锁的接口以支持优雅退出
// 任务执行时应持有锁以防止提前退出
type CronServerL interface {
	RegisterCron(ctx context.Context, c *cron.Cron, locker sync.Locker)
}

// RegisterCronServerL adds cron service with read-lock access
// Provides RLocker so service can coordinate with Server.Stop clean shutdown
//
// RegisterCronServerL 注册带读锁访问的定时任务
// 提供 RLocker 使任务能与 Server.Stop 优雅退出协调
func RegisterCronServerL(srv *Server, service CronServerL) {
	service.RegisterCron(srv.ctx, srv.cron, srv.mutex.RLocker())
}
