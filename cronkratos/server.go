// Package cronkratos integrates robfig/cron with the Kratos framework.
//
// Wraps cron as a Kratos transport.Server with first-class support of:
//   - clean shutdown coordinated via an in-process RWMutex (Stop waits running
//     tasks to complete before invalidating ctx)
//   - panic catch built in (cron.Recover JobWrapper, on as default)
//   - one-shot startup execution declared at task scope via AddFunc(..., DoOnStartup())
//   - read-lock aid that lets business tasks coordinate with shutdown
//
// Business side does not touch cron.Logger adaptation, cron.WithChain wiring /
// cron.Recover - each is encapsulated. Users see just Server methods.
//
// Sample pattern:
//
//	c := cron.New(
//	    cron.WithSeconds(),
//	    cron.WithLocation(time.FixedZone("CST", 8*60*60)),
//	)
//	srv := cronkratos.NewServer(c, slog)
//
//	// Bind a 2am task that should also run once at startup,
//	// with panic catch so transient bugs do not crash the service.
//	// stage.Do holds the read-lock so Stop waits—fn returns before cancelling ctx.
//	rese.V1(srv.AddFunc("0 0 2 * * *", func(ctx context.Context, stage *cronkratos.Stage) {
//	    stage.Do(ctx, func(ctx context.Context) {
//	        if ctx.Err() != nil { return }
//	        // business logic
//	    })
//	}, cronkratos.DoOnStartup(), cronkratos.Recoverable()))
//
//	// Bind a 3am task that runs just on schedule, no panic catch, no lock-coordination needed
//	rese.V1(srv.AddFunc("0 0 3 * * *", func(ctx context.Context, stage *cronkratos.Stage) { ... }))
//
// Else set service-side panic catch once and skip task-scope Recoverable():
//
//	srv := cronkratos.NewServer(c, slog, cronkratos.WithRecover())
//
// cronkratos: 把 robfig/cron 跟 Kratos 框架集成
//
// 包成 Kratos transport.Server,一站式提供:
//   - 优雅退出(读写锁机制:Stop 等运行中任务完成才 cancel ctx)
//   - panic 兜底内建(cron.Recover JobWrapper,默认启用)
//   - 启动跑一次:在 AddFunc 时通过 DoOnStartup() 选项声明,task 粒度配置
//   - 读锁配合 — 业务任务持读锁可跟 Stop 协调
//
// 业务侧不用碰 cron.Logger 适配 / cron.WithChain / cron.Recover - 都封装在内部
//
// 典型用法:
//
//	c := cron.New(
//	    cron.WithSeconds(),
//	    cron.WithLocation(time.FixedZone("CST", 8*60*60)),
//	)
//	srv := cronkratos.NewServer(c, slog)
//
//	// 注册一个每天跑的任务,启动时也跑一次,带 panic 兜底防止偶发 bug 让进程崩
//	// stage.Do 持读锁,Stop 会等 fn 返回再 cancel ctx
//	rese.V1(srv.AddFunc("0 0 2 * * *", func(ctx context.Context, stage *cronkratos.Stage) {
//	    stage.Do(ctx, func(ctx context.Context) {
//	        if ctx.Err() != nil { return }
//	        // 业务逻辑
//	    })
//	}, cronkratos.DoOnStartup(), cronkratos.Recoverable()))
//
//	// 或者构造时声明服务级 recover,task 级就不用每个写 Recoverable() 了:
//	srv := cronkratos.NewServer(c, slog, cronkratos.WithRecover())
package cronkratos

import (
	"context"
	"sync"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/robfig/cron/v3"
	"github.com/yylego/erero"
)

// Server is a Kratos transport.Server wrapping a cron schedule.
// Construct via NewServer; do not zero-initialise.
//
// Server 是 Kratos transport.Server,封装 cron 调度器
// 用 NewServer 构造,不要零值初始化
type Server struct {
	cron        *cron.Cron
	ctx         context.Context
	cancel      context.CancelFunc
	mutex       *sync.RWMutex
	slog        *log.Helper
	startupRuns []func() // tasks that run once at Server.Start() // 启动时跑一次的任务
	recoverable bool     // service-side default panic catch flag // 服务级默认 recover 开关
	stage       *Stage   // shared lock-stage aid injected into each AddFunc cmd // 注入到每个 task 的锁阶段辅助
}

// NewServer constructs a Server wrapping the supplied cron instance.
// Caller creates the cron instance with the cron options it needs
// (cron.WithSeconds / cron.WithLocation / cron.WithChain / cron.WithParser)
// before passing it in. The kratos log.Logger is adapted to cron.Logger
// in here - business side does not see the adaptation.
//
// NewServer 构造 Server,封装传入的 cron 实例
// 业务侧自己 cron.New(...)(用 cron.WithSeconds / cron.WithLocation / cron.WithChain
// / cron.WithParser 等任何 cron 选项),再传进来
// kratos log.Logger 适配到 cron.Logger 在内部完成,业务侧看不到适配过程
func NewServer(c *cron.Cron, slog log.Logger, opts ...ServerOption) *Server {
	cfg := &serverConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mutex := &sync.RWMutex{}
	return &Server{
		cron:        c,
		ctx:         ctx,
		cancel:      cancel,
		mutex:       mutex,
		slog:        log.NewHelper(slog),
		startupRuns: nil,
		recoverable: cfg.recoverable,
		stage:       NewStage(mutex.RLocker()),
	}
}

// AddFunc registers a function with the cron schedule.
//
// Settings controlled via options (each opt-in, no implicit defaults):
//   - Recoverable() wraps this task with deferred recover (panic -> log + survive)
//   - DoOnStartup() also runs the task once at Server.Start()
//
// Note: NewServer's WithRecover() is service-side and applies panic catch to
// each task added through this method, even when a task does not
// declare Recoverable() on its own.
//
// Returns cron.EntryID plus the spec-parsing error from the underlying cron
// package. The startup run (when present) is deferred to Server.Start().
//
// # AddFunc 注册定时任务
//
// 选项控制行为(全部 opt-in,没有隐式默认):
//   - Recoverable() 给这个 task 包 defer recover(panic -> 打日志 + 不崩进程)
//   - DoOnStartup() 让 Server.Start() 时也跑一次
//
// 注:NewServer 的 WithRecover() 是服务级配置,作用于通过本方法注册的所有 task,
// 不论 task 是否单独声明 Recoverable()
//
// 返回 cron.EntryID 和 spec 解析错误(底层 cron 库返回)
// 启动跑一次的部分(如有)推迟到 Server.Start() 触发
func (s *Server) AddFunc(spec string, cmd func(ctx context.Context, stage *Stage), opts ...TaskOption) (cron.EntryID, error) {
	cfg := &taskConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	// Adapt cmd(ctx, *Stage) to cron's func() signature via a closure that binds
	// Server's ctx plus the shared stage instance. When task-scope Recoverable()
	// has been set—elsewise service-side WithRecover()—wrap the closure with
	// deferred recover. The same runFunc is reused on the cron schedule plus
	// on the startup invocation (when DoOnStartup is set).
	//
	// 通过闭包把 cmd(ctx, *Stage) 适配成 cron 要求的 func() — 绑定 Server 的 ctx 和共享 stage 实例
	// 当 task 级 Recoverable() 或 service 级 WithRecover() 任一开启时,给闭包包一层 defer recover
	// 同一个 runFunc 同时用于 cron 调度和启动时跑一次
	runFunc := func() { cmd(s.ctx, s.stage) }
	if s.recoverable || cfg.recoverable {
		runFunc = s.wrapRecoverable(spec, runFunc)
	}

	num, err := s.cron.AddFunc(spec, runFunc)
	if err != nil {
		return 0, erero.WithMessage(err, "add cron function")
	}

	if cfg.doOnStartup {
		s.startupRuns = append(s.startupRuns, runFunc)
	}

	return num, nil
}

// wrapRecoverable wraps cmd with a deferred recover that logs the panic via Server.slog.
// Used via AddFunc when task is Recoverable() / Server has WithRecover(). The
// wrapped function is reused on cron schedule invocations plus
// DoOnStartup invocations, so each path shares the same panic-catch semantics.
//
// wrapRecoverable 给 cmd 包一层 defer recover,通过 Server.slog 打 panic 日志
// AddFunc 在 task 是 Recoverable() 或 Server 有 WithRecover() 时调用
// 包装后的函数同时用于 cron 调度跟 DoOnStartup,两条路径 panic 兜底语义一致
func (s *Server) wrapRecoverable(spec string, cmd func()) func() {
	return func() {
		defer func() {
			if rec := recover(); rec != nil {
				s.slog.Errorw("msg", "cron task panic", "spec", spec, "reason", rec)
			}
		}()
		cmd()
	}
}

// Start implements Kratos transport.Server.
// Starts the cron schedule then triggers each DoOnStartup task in goroutines.
//
// Each startupRuns item is the (perhaps wrapped) runFunc from AddFunc - when
// the task is Recoverable() / the Server has WithRecover() it has the
// panic catch baked in, so Start does not add an extra defer recover here.
// Tasks without panic catch propagate panics to the goroutine edge as is standard.
//
// Start 实现 Kratos transport.Server
// 启动 cron 调度,然后异步触发每个 DoOnStartup 任务
//
// startupRuns 中的每项是 AddFunc 中(可能已包装的)runFunc - task 是 Recoverable() 或 Server 有 WithRecover()
// 就已经包了 panic 兜底,Start 不再加额外 defer recover。其他 task panic 按 goroutine 默认行为传播
func (s *Server) Start(_ context.Context) error {
	s.slog.Info("cron starting")
	s.cron.Start()

	for _, fn := range s.startupRuns {
		go fn()
	}

	if n := len(s.startupRuns); n > 0 {
		s.slog.Infof("cron triggered %d startup task(s)", n)
	}
	return nil
}

// Stop implements Kratos transport.Server with three-step clean shutdown:
//
//  1. cron.Stop()    - stop scheduling new tasks; returned ctx done when running tasks complete
//  2. mutex.Lock()   - acquire write lock, waits on read locks (held in business tasks) to release
//  3. cancel()       - invalidate srv.ctx
//
// The sequence guarantees that a ctx checked inside a business read-lock stays
// valid throughout that task's run.
//
// Stop 实现 Kratos transport.Server 的三步优雅退出:
//
//  1. cron.Stop()    - 停止调度新任务,返回的 ctx 在运行任务完成时 done
//  2. mutex.Lock()   - 拿写锁,等业务任务持有的读锁释放
//  3. cancel()       - 取消 srv.ctx
//
// 顺序保证业务读锁内检查到的 ctx 在业务返回前一定有效
func (s *Server) Stop(ctx context.Context) error {
	s.slog.Info("cron stopping")

	stopCtx := s.cron.Stop()
	select {
	case <-stopCtx.Done():
		s.slog.Info("cron schedule complete")
	case <-ctx.Done():
		s.slog.Warn("cron stop timeout")
	}

	s.mutex.Lock()
	s.cancel()
	s.mutex.Unlock()
	s.slog.Info("cron shutdown complete")
	return nil
}
