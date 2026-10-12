<!-- TEMPLATE (ZH) BEGIN: BADGES -->

[![GitHub Workflow Status (branch)](https://img.shields.io/github/actions/workflow/status/yylego/kratos-cron/release.yml?branch=main&label=BUILD)](https://github.com/yylego/kratos-cron/actions/workflows/release.yml?query=branch%3Amain)
[![GoDoc](https://pkg.go.dev/badge/github.com/yylego/kratos-cron)](https://pkg.go.dev/github.com/yylego/kratos-cron)
[![Coverage Status](https://img.shields.io/coveralls/github/yylego/kratos-cron/main.svg)](https://coveralls.io/github/yylego/kratos-cron?branch=main)
[![Supported Go Versions](https://img.shields.io/badge/Go-1.25%2B-lightgrey.svg)](https://go.dev/)
[![GitHub Release](https://img.shields.io/github/release/yylego/kratos-cron.svg)](https://github.com/yylego/kratos-cron/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/yylego/kratos-cron)](https://goreportcard.com/report/github.com/yylego/kratos-cron)
<!-- TEMPLATE (ZH) CLOSE: BADGES -->

# kratos-cron

把 `robfig/cron` 跟 Kratos 框架集成,封装成 Kratos `transport.Server`。提供可选的 panic 兜底、启动跑一次、读锁保护的退出关键段。

---

<!-- TEMPLATE (ZH) BEGIN: LANGUAGE NAVIGATION -->

## 英文文档

[ENGLISH README](README.md)

<!-- TEMPLATE (ZH) CLOSE: LANGUAGE NAVIGATION -->

## 核心特性

- 🕐 **Kratos transport.Server** — 直接塞给 `kratos.New(kratos.Server(...))` 用
- 🛡️ **退出协调** — 停止调度、等当前关键段完成、取消 ctx，再等任务返回
- ⚡ **关键段保护** — `Stage.Do` 持读锁，阻止关停流程在关键段内取消上下文
- 🚀 **启动跑一次** — 任务粒度的 `DoOnStartup()` 选项,不用单独写启动钩子
- 🛟 **panic 兜底可选** — 任务级 `Recoverable()` 或服务级 `WithRecover()`
- 🔁 **嵌套 Do 避免互等** — 内层 `stage.Do` 自动识别 ctx 标记跳过重复加锁
- 📦 **精简 API** — 生命周期方法与按需开启的任务选项

## 安装

```bash
go get github.com/yylego/kratos-cron/cronkratos
```

## 快速上手

```go
package main

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v3"
	"github.com/go-kratos/kratos/v3/log"
	"github.com/robfig/cron/v3"
	"github.com/yylego/kratos-cron/cronkratos"
	"github.com/yylego/must"
	"github.com/yylego/rese"
)

func main() {
	c := cron.New(
		cron.WithSeconds(),
		cron.WithLocation(time.FixedZone("CST", 8*60*60)),
	)

	applog := log.NewLogger(log.NewHandler())
	srv := cronkratos.NewServer(c, applog, cronkratos.WithRecover())

	rese.C1(srv.AddFunc("0 0 2 * * *", func(ctx context.Context, stage *cronkratos.Stage) {
		stage.Do(ctx, func(ctx context.Context) {
			if ctx.Err() != nil {
				return
			}
			// 业务逻辑 — 防止关停流程在关键段内取消上下文
		})
	}, cronkratos.DoOnStartup()))

	app := kratos.New(kratos.Server(srv))
	must.Done(app.Run())
}
```

## 用法

### 1. 注册一个定时任务

```go
srv.AddFunc("0 */5 * * * *", func(ctx context.Context, stage *cronkratos.Stage) {
    // 每 5 分钟跑一次
})
```

`cmd` 签名是 `func(ctx context.Context, stage *Stage)`。Server 注入自己持有的 ctx(Stop 时取消)和共享的 `*Stage` 实例。

### 2. 启动时也跑一次

```go
srv.AddFunc("0 0 2 * * *", fn, cronkratos.DoOnStartup())
```

`fn` 既按 cron 表达式调度,**也**在 `Server.Start()` 时异步跑一次。适合"每日采集 + 每次启动跑一次断点续抓"这种场景。

### 3. panic 兜底

兜底是 opt-in(对齐 Kratos gRPC/HTTP 的"recover 是 opt-in"习惯)。两种打开方式:

```go
// 任务级 — 只包某一个特定任务
srv.AddFunc(spec, fn, cronkratos.Recoverable())

// 服务级 — 包通过本 Server 注册的所有任务
srv := cronkratos.NewServer(c, slog, cronkratos.WithRecover())
```

两层用 **OR** 组合:任一开启就包 defer recover。两层都没配置的任务裸跑,panic 按 goroutine 默认行为传播。

`WithName("数据同步")` 为异常和跳过日志添加业务名称，捕获的 panic 同时记录堆栈。
两项默认均不开启。被跳过的执行不排队、不补跑。HTTP 接口直接调用业务函数时不经过门控，需要统一互斥时应放在业务共同入口。任务一直不返回时会一直占用门控；本选项不负责超时或取消任务。
`SkipIfRunning()` 跳过同一注册项在当前进程中的重叠执行，也覆盖 `DoOnStartup()`。跳过时记录已运行时长，任务返回或 panic 时释放门控，不取消正在运行的任务。同名的不同注册项相互独立，不提供分布式锁；任务另起 goroutine 时，必须在任务函数中等待它结束，门控才会覆盖这部分工作。

```go
rese.C1(srv.AddFunc(spec, fn, cronkratos.WithName("数据同步"), cronkratos.SkipIfRunning(), cronkratos.Recoverable()))
```

### 4. 优雅退出协调 — `Stage.Do`

关键代码段用 `stage.Do(ctx, fn)` 包起来:

```go
srv.AddFunc(spec, func(ctx context.Context, stage *cronkratos.Stage) {
    for _, item := range list {
        if ctx.Err() != nil {
            return
        }
        stage.Do(ctx, func(ctx context.Context) {
            if ctx.Err() != nil {
                return
            }
            process(ctx, item)
        })
    }
})
```

`stage.Do` 在 fn 执行期间持 Server 的读锁。`Server.Stop()` 必须先拿写锁再取消自身上下文，因此会等待正在执行的关键段。进入回调时仍须检查 `ctx.Err()`：上下文可能在进入前已被取消，业务自行派生的上下文也可能独立超时。

`Stop` 先等当前关键段完成并取消任务上下文，再等任务返回。业务应在步骤之间检查取消，休息时使用可取消的等待。传入的停止期限不会提前结束等待；任务始终不返回，停机也会继续等待。不能在被等待的任务中调用 `Stop`；任务自行启动的 goroutine 应由任务等待完成后再返回。

持锁标记区分每个 `Stage`。派生上下文只用于外层调用期间的同步嵌套调用，不要在外层返回后复用，也不要传给独立运行的 goroutine。

### 5. 嵌套 `stage.Do` 安全

辅助函数可以再调一次同一实例的 `stage.Do` — 同步调用沿用派生的 `ctx`，内层识别持锁标记并跳过重复加锁，避免与并发写锁相互等待:

```go
stage.Do(ctx, func(ctx context.Context) {
    processBatch(ctx, stage)  // 内层 stage.Do 看到标记,跳过重新加锁
})
```

### 6. 按需触发路径(`NonStage`)

业务上同一个方法既挂 cron 调度,也通过 RPC 接口按需触发执行(比如 admin 后台点一下立即跑一次)。按需路径拿不到 `*Stage`,这时用 `cronkratos.NonStage()` 拿一个 noop Stage 即可 — 业务方法签名保持干净(`*Stage` 永远不为 nil),按需路径明确不参与优雅退出协调:

```go
func (s *Service) RunSomething(ctx context.Context) error {
    return s.runSomething(ctx, cronkratos.NonStage())
}
```

少数高级场景需要自定义 `sync.Locker` 时,用 `cronkratos.NewStage(customLock)` — 跟 `Server` 内部用的是同一个构造函数。

### 7. 跟 Kratos 集成

`*Server` 直接实现 `transport.Server` 接口:

```go
app := kratos.New(
    kratos.Name("app-service"),
    kratos.Server(httpSrv, grpcSrv, srv),
)
```

`Start` 跟 `Stop` 由 Kratos 生命周期驱动。生命周期操作前完成任务注册；实例内部串行协调启动与停止，重复调用不重复执行。停止后如需重新启动，应创建新实例。

启动任务由本包跟踪，关停时与定时任务一起等待完成。传给 `cron.WithChain` 的包装只作用于定时执行，不覆盖启动执行；本包的任务选项则覆盖两条路径。

## 设计说明

### 三步优雅退出

`Server.Stop` 按顺序跑三步:

1. **`cron.Stop()`** — 停止调度新任务，不等待下一次定时触发。
2. **`mutex.Lock()` + `cancel()` + `mutex.Unlock()`** — 等当前关键段完成，持写锁取消任务上下文，然后释放写锁。
3. **等待任务返回** — 等定时和启动任务连同其 defer 完成。外部强制结束进程无法保证这一流程完成。

写锁保证关停流程不会在正在执行的关键段内取消上下文；它不会恢复已经取消的上下文，也不能阻止调用方自行设置的期限到期。

### `Stage.Do` 的封装职责

两个理由:

- **隐藏锁。** 业务侧根本不碰 `sync.RWMutex` / `RLocker` / 写锁这些概念,API 表面只有 `stage.Do(ctx, fn)`。
- **解决嵌套重入。** `sync.RWMutex.RLock` 不可重入，并发的 `Lock` 等待时，重复获取读锁会造成相互等待。`Stage.Do` 使用上下文标记识别同一实例的同步嵌套，跳过重复加锁。

### `cronkratos` 替业务侧封装的事情

| 底层细节                                 | 内部处理                                      |
| ---------------------------------------- | --------------------------------------------- |
| 任务异常与跳过日志                       | 使用传入的 `*slog.Logger`                     |
| `cron.WithChain(cron.Recover(...))` 配线 | 替换成 `Recoverable()` / `WithRecover()` 选项 |
| `defer recover()` 模板代码               | `wrapRecoverable` 在任一开关启用时自动加      |
| `sync.RWMutex` + `RLocker` 管线          | 封装在 `Stage.Do` 里                          |
| `context.WithCancel` 生命周期            | `NewServer` 创建,`Stop` 取消                  |
| 启动跑一次的簿记                         | 由 `DoOnStartup()` 选项驱动                   |

底层 cron 仍使用它自己的日志配置，本包不会安装 `cron.Logger` 适配器。

## 演示

完整的 Kratos 集成示例见 [kratos-cron-demos](https://github.com/yylego/kratos-cron-demos)。

---

<!-- TEMPLATE (ZH) BEGIN: STANDARD PROJECT FOOTER -->
<!-- VERSION 2025-11-25 03:52:28.131064 +0000 UTC -->

## 📄 许可证类型

MIT 许可证 - 详见 [LICENSE](LICENSE)。

---

## 💬 联系与反馈

非常欢迎贡献代码！报告 BUG、建议功能、贡献代码：

- 🐛 **问题报告？** 在 GitHub 上提交问题并附上重现步骤
- 💡 **新颖思路？** 创建 issue 讨论
- 📖 **文档疑惑？** 报告问题，帮助我们完善文档
- 🚀 **需要功能？** 分享使用场景，帮助理解需求
- ⚡ **性能瓶颈？** 报告慢操作，协助解决性能问题
- 🔧 **配置困扰？** 询问复杂设置的相关问题
- 📢 **关注进展？** 关注仓库以获取新版本和功能
- 🌟 **成功案例？** 分享这个包如何改善工作流程
- 💬 **反馈意见？** 欢迎提出建议和意见

---

## 🔧 代码贡献

新代码贡献，请遵循此流程：

1. **Fork**：在 GitHub 上 Fork 仓库（使用网页界面）
2. **克隆**：克隆 Fork 的项目（`git clone https://github.com/yourname/repo-name.git`）
3. **导航**：进入克隆的项目（`cd repo-name`）
4. **分支**：创建功能分支（`git checkout -b feature/xxx`）
5. **编码**：实现您的更改并编写全面的测试
6. **测试**：（Golang 项目）确保测试通过（`go test ./...`）并遵循 Go 代码风格约定
7. **文档**：面向用户的更改需要更新文档
8. **暂存**：暂存更改（`git add .`）
9. **提交**：提交更改（`git commit -m "Add feature xxx"`）确保向后兼容的代码
10. **推送**：推送到分支（`git push origin feature/xxx`）
11. **PR**：在 GitHub 上打开 Merge Request（在 GitHub 网页上）并提供详细描述

请确保测试通过并包含相关的文档更新。

---

## 🌟 项目支持

非常欢迎通过提交 Merge Request 和报告问题来贡献此项目。

**项目支持：**

- ⭐ **给予星标**如果项目对您有帮助
- 🤝 **分享项目**给团队成员和（golang）编程朋友
- 📝 **撰写博客**关于开发工具和工作流程 - 我们提供写作支持
- 🌟 **加入生态** - 致力于支持开源和（golang）开发场景

**祝你用这个包编程愉快！** 🎉🎉🎉

<!-- TEMPLATE (ZH) CLOSE: STANDARD PROJECT FOOTER -->

---

<!-- TEMPLATE (ZH) BEGIN: GITHUB STARS -->

## GitHub 标星点赞

[![Stargazers](https://starchart.cc/yylego/kratos-cron.svg?variant=adaptive)](https://starchart.cc/yylego/kratos-cron)
<!-- TEMPLATE (ZH) CLOSE: GITHUB STARS -->
