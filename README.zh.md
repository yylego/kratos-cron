[![GitHub Workflow Status (branch)](https://img.shields.io/github/actions/workflow/status/yylego/kratos-cron/release.yml?branch=main&label=BUILD)](https://github.com/yylego/kratos-cron/actions/workflows/release.yml?query=branch%3Amain)
[![GoDoc](https://pkg.go.dev/badge/github.com/yylego/kratos-cron)](https://pkg.go.dev/github.com/yylego/kratos-cron)
[![Coverage Status](https://img.shields.io/coveralls/github/yylego/kratos-cron/main.svg)](https://coveralls.io/github/yylego/kratos-cron?branch=main)
[![Supported Go Versions](https://img.shields.io/badge/Go-1.25+-lightgrey.svg)](https://go.dev/)
[![GitHub Release](https://img.shields.io/github/release/yylego/kratos-cron.svg)](https://github.com/yylego/kratos-cron/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/yylego/kratos-cron)](https://goreportcard.com/report/github.com/yylego/kratos-cron)

# kratos-cron

把 `robfig/cron` 跟 Kratos 框架集成,封装成 Kratos `transport.Server`。内建 panic 兜底、启动跑一次、读锁配合的优雅退出。

---

<!-- TEMPLATE (ZH) BEGIN: LANGUAGE NAVIGATION -->

## 英文文档

[ENGLISH README](README.md)

<!-- TEMPLATE (ZH) END: LANGUAGE NAVIGATION -->

## 核心特性

🕐 **Kratos transport.Server** — 直接塞给 `kratos.New(kratos.Server(...))` 用
🛡️ **优雅退出** — 三步退出,等所有运行中任务完成才让 ctx 失效
⚡ **关键段 ctx 安全** — `Stage.Do` 持读锁,任务执行期间 ctx 保证有效
🚀 **启动跑一次** — 任务粒度的 `DoOnStartup()` 选项,不用单独写启动钩子
🛟 **panic 兜底可选** — 任务级 `Recoverable()` 或服务级 `WithRecover()`
🔁 **嵌套 Do 防死锁** — 内层 `stage.Do` 自动识别 ctx 标记跳过重复加锁
📦 **极简 API** — 4 个方法 (`NewServer` / `AddFunc` / `Start` / `Stop`) + 3 个选项

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

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
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

	slog := log.DefaultLogger
	srv := cronkratos.NewServer(c, slog, cronkratos.WithRecover())

	rese.C1(srv.AddFunc("0 0 2 * * *", func(ctx context.Context, stage *cronkratos.Stage) {
		stage.Do(ctx, func(ctx context.Context) {
			if ctx.Err() != nil {
				return
			}
			// 业务逻辑 — fn 执行期间 ctx 保证有效
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

### 4. 优雅退出协调 — `Stage.Do`

关键代码段用 `stage.Do(ctx, fn)` 包起来:

```go
srv.AddFunc(spec, func(ctx context.Context, stage *cronkratos.Stage) {
    for _, item := range list {
        stage.Do(ctx, func(ctx context.Context) {
            if ctx.Err() != nil {
                return
            }
            process(ctx, item)
        })
    }
})
```

`stage.Do` 在 fn 执行期间持 Server 的读锁。`Server.Stop()` 时会先拿**写锁**再取消 ctx — 写锁会等所有未完成的 `stage.Do` 全部返回。所以即使 Stop 跟业务任务并发,`stage.Do` 回调内部检查到的 `ctx` 一定是有效的。

上面那种"循环里每次重新加锁"的写法,让 `Stop` 能在迭代间隙拿写锁,长任务自然收尾,不会被打断到一半。

### 5. 嵌套 `stage.Do` 安全

辅助函数可以再调一次 `stage.Do` — 只要 `ctx` 沿调用链传下去,内层 `stage.Do` 会看到 ctx 上的"持锁中"标记,跳过重复加锁,避免 `RWMutex` 跟并发写者撞上的经典重入死锁:

```go
stage.Do(ctx, func(ctx context.Context) {
    helperUsingStage(ctx, stage)  // 内层 stage.Do 看到标记,跳过重新加锁
})
```

### 6. 跟 Kratos 集成

`*Server` 直接实现 `transport.Server` 接口:

```go
app := kratos.New(
    kratos.Name("app-service"),
    kratos.Server(httpSrv, grpcSrv, srv),
)
```

`Start` 跟 `Stop` 由 Kratos 生命周期驱动。

## 设计说明

### 三步优雅退出

`Server.Stop` 按顺序跑三步:

1. **`cron.Stop()`** — 停止调度新任务。返回的 ctx 在所有运行中的 cron 任务完成时 done。这一步监听 `Stop` 自己传入的 ctx — 超时只打个 warn 日志,仍会继续走第 2 步(`mutex.Lock` 不看 ctx,该等多久还等多久)。
2. **`mutex.Lock()`** — 拿写锁,等所有读锁持有者(运行中的 `stage.Do` 回调)释放。
3. **`cancel()`** — 取消 Server 的 ctx。

因为 (3) 一定在 (2) 之后,而 (2) 等读锁释放,所以任何 `stage.Do` 回调内部检查到的 ctx 一定在回调返回前都有效。没有"ctx 中途失效"这种坑。

### 为什么用 `Stage.Do` 而不是裸的 mutex?

两个理由:

- **隐藏锁。** 业务侧根本不碰 `sync.RWMutex` / `RLocker` / 写锁这些概念,API 表面只有 `stage.Do(ctx, fn)`。
- **解决嵌套重入。** `sync.RWMutex.RLock` 不可重入 — 在并发的 `Lock` 在等的情况下,同一 goroutine 第二次 `RLock` 会死锁(为防写锁饥饿)。`Stage.Do` 在 ctx 上挂私有标记,嵌套调用看到标记就跳过加锁,死锁消失。

### `cronkratos` 替业务侧封装的事情

| 底层细节                                 | 内部处理                                      |
| ---------------------------------------- | --------------------------------------------- |
| `cron.Logger` 接口适配                   | 包在传入的 Kratos `log.Logger` 外             |
| `cron.WithChain(cron.Recover(...))` 配线 | 替换成 `Recoverable()` / `WithRecover()` 选项 |
| `defer recover()` 模板代码               | `wrapRecoverable` 在任一开关启用时自动加      |
| `sync.RWMutex` + `RLocker` 管线          | 封装在 `Stage.Do` 里                          |
| `context.WithCancel` 生命周期            | `NewServer` 创建,`Stop` 取消                  |
| 启动跑一次的簿记                         | 由 `DoOnStartup()` 选项驱动                   |

业务侧看到的只有 4 个方法、3 个选项,加上 `Stage` 类型本身。

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
2. **克隆**：克隆 Fork 的项目（`git clone https://github.com/yourname/kratos-cron.git`）
3. **导航**：进入克隆的项目（`cd kratos-cron`）
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

<!-- TEMPLATE (ZH) END: STANDARD PROJECT FOOTER -->

---

## GitHub 标星点赞

[![标星点赞](https://starchart.cc/yylego/kratos-cron.svg?variant=adaptive)](https://starchart.cc/yylego/kratos-cron)
