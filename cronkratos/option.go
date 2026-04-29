package cronkratos

// ServerOption configures a Server during NewServer.
//
// ServerOption 在 NewServer 时配置 Server
type ServerOption func(*serverConfig)

type serverConfig struct {
	recoverable bool
}

// WithRecover enables service-side panic catch on each task added
// via Server.AddFunc. Equivalent to declaring each task as Recoverable(),
// just configured once on the Server.
//
// Task-scope Recoverable() plus service-side WithRecover() compose with OR
// semantics: when one is enabled, the task is wrapped with deferred recover.
// When a task lacks Recoverable() and the Server lacks WithRecover() it
// runs raw - panics propagate to crash the process (matching Kratos
// gRPC/HTTP "recover is opt-in" convention).
//
// WithRecover 启用服务级 panic 兜底,作用于通过 Server.AddFunc 注册的所有 task
// 等价于每个 task 都声明 Recoverable(),但只配置一次
//
// task 级 Recoverable() 与 service 级 WithRecover() 用 OR 组合:任一开启就包 defer recover
// 两层都没配置的 task 裸跑 - panic 传播让进程崩(对齐 Kratos gRPC/HTTP 的 "recover 是 opt-in" 习惯)
func WithRecover() ServerOption {
	return func(c *serverConfig) { c.recoverable = true }
}

// TaskOption tunes a single AddFunc registration (task-scope settings).
//
// TaskOption 配置单个 AddFunc 注册项(任务级)
type TaskOption func(*taskConfig)

type taskConfig struct {
	doOnStartup bool
	recoverable bool
}

// DoOnStartup also runs the task once when Server.Start() is invoked,
// in addition to the standard cron schedule.
// Triggered in its own goroutine.
//
// Use case: a 2am sweep task that should also run on each boot to drain
// unfinished work / refresh metadata. Same business function reused on both
// scheduled and startup runs.
//
// DoOnStartup 让任务除了按 cron 表达式调度,Server.Start() 时也异步跑一次
// 内部用独立 goroutine
//
// 适用场景:每日采集任务同时也想"每次启动跑一次"(断点续爬 / 刷元数据)
// 同一个业务函数同时承担定时任务和启动跑一次,不用写两份
func DoOnStartup() TaskOption {
	return func(c *taskConfig) { c.doOnStartup = true }
}

// Recoverable marks this task as recoverable - panics during execution are
// caught via a deferred recover, logged through Server.slog, and the process
// continues. Without this option (and without service-side WithRecover())
// the task runs raw, propagating panics to the cron schedule goroutine.
//
// Naming: the -able suffix declares an attribute of the task ("this task is
// recoverable"), in contrast with the service-side WithRecover() that
// configures the Server itself.
//
// Recoverable 把这个 task 标记为"可恢复" - 执行中 panic 被 defer recover 捕获、
// 通过 Server.slog 打日志,进程继续。不传这个选项(同时 Server 也没传 WithRecover())
// task 裸跑,panic 会传到 cron 调度 goroutine
//
// 命名:-able 后缀声明 task 的属性("这个 task 是可恢复的"),区别于服务级的
// WithRecover() 是配置 Server 本身
func Recoverable() TaskOption {
	return func(c *taskConfig) { c.recoverable = true }
}
