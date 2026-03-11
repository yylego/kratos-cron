// Package cronkratos: Cron server integration with Kratos framework
// Wraps robfig/cron as Kratos transport.Server, supports clean shutdown and lifecycle management
// Provides two registration modes: basic mode and locker-protected mode
//
// cronkratos: Cron 服务与 Kratos 框架集成
// 将 robfig/cron 封装成 Kratos transport.Server，支持优雅退出和生命周期管理
// 提供两种注册模式：基础模式和带锁保护模式
package cronkratos

import (
	"context"
	"sync"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/robfig/cron/v3"
)

// Server wraps cron instance with Kratos lifecycle management
// Implements Kratos transport.Server interface with Start and Stop methods
//
// Server 封装 cron 实例，支持 Kratos 生命周期管理
// 实现 Kratos transport.Server 接口的 Start 和 Stop 方法
type Server struct {
	cron   *cron.Cron         // Cron schedule instance // Cron 调度器实例
	ctx    context.Context    // Context passed to cron server // 传递给定时任务的上下文
	cancel context.CancelFunc // Cancel function to stop cron server // 取消函数用于停止任务
	mutex  *sync.RWMutex      // RWMutex to coordinate clean shutdown // 读写锁用于协调优雅退出
	slog   *log.Helper        // Structured log instance // 结构化日志实例
}

// NewServer creates a new cron Server instance
// Initializes context, mutex and wraps the cron schedule
//
// NewServer 创建新的 cron Server 实例
// 初始化上下文、互斥锁并封装提供的 cron 调度器
func NewServer(cron *cron.Cron, slog log.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		cron:   cron,
		ctx:    ctx,
		cancel: cancel,
		mutex:  &sync.RWMutex{},
		slog:   log.NewHelper(slog),
	}
}

// Start implements Kratos transport.Server interface
// Starts the cron server to begin executing
//
// Start 实现 Kratos transport.Server 接口
// 启动 cron 调度器开始执行定时任务
func (s *Server) Start(ctx context.Context) error {
	s.slog.Info("cron server starting")
	s.cron.Start()
	return nil
}

// Stop implements Kratos transport.Server interface
// Stops the cron server and waits to complete before exit
//
// Execution sequence:
//  1. cron.Stop()   - stop scheduling new tasks
//  2. mutex.Lock()  - get write lock, wait read locks to release
//  3. cancel()      - cancel ctx
//
// cancel() is invoked following write lock acquisition, write lock waits read locks
// so if ctx is valid when checked inside read lock it won't become invalid
//
// This design ensures operation atomicity:
//   - complete execution, otherwise exit at checkpoint
//   - avoid half-done data, inconsistent state, not-released resources
//   - ctx won't turn invalid mid-execution
//
// Stop 实现 Kratos transport.Server 接口
// 优雅地停止 cron 调度器并等待运行中的任务完成
//
// 执行顺序:
//  1. cron.Stop()   - 停止调度新任务
//  2. mutex.Lock()  - 获取写锁，等待读锁释放
//  3. cancel()      - 取消 ctx
//
// cancel() 在写锁后调用，写锁又等读锁释放，所以锁内检查后若有效则不会变为无效
//
// 这个设计确保操作的原子性:
//   - 要么完整执行，要么在检查点退出
//   - 避免数据写一半、状态不一致、资源未释放等问题
//   - ctx 不会在执行过程中突然变为无效
func (s *Server) Stop(ctx context.Context) error {
	s.slog.Info("cron server stopping")
	// cron.Stop() stops scheduling new tasks and returns context done when complete
	// cron.Stop() 停止调度新任务，返回的 context 会在当前任务完成后结束
	stopCtx := s.cron.Stop()
	select {
	case <-stopCtx.Done():
		s.slog.Info("cron server schedule complete")
	case <-ctx.Done():
		s.slog.Warn("cron server stop timeout")
	}
	// Acquire write lock to wait running tasks complete (ctx is still active)
	// 获取写锁等待运行中的任务完成（此时 ctx 仍然有效）
	s.mutex.Lock()
	// Cancel context once tasks complete
	// 所有任务完成后再取消 context
	s.cancel()
	s.slog.Info("cron server shutdown complete")
	s.mutex.Unlock()
	return nil
}
