<!-- TEMPLATE (EN) BEGIN: BADGES -->
[![GitHub Workflow Status (branch)](https://img.shields.io/github/actions/workflow/status/yylego/kratos-cron/release.yml?branch=main&label=BUILD)](https://github.com/yylego/kratos-cron/actions/workflows/release.yml?query=branch%3Amain)
[![GoDoc](https://pkg.go.dev/badge/github.com/yylego/kratos-cron)](https://pkg.go.dev/github.com/yylego/kratos-cron)
[![Coverage Status](https://img.shields.io/coveralls/github/yylego/kratos-cron/main.svg)](https://coveralls.io/github/yylego/kratos-cron?branch=main)
[![Supported Go Versions](https://img.shields.io/badge/Go-1.25%2B-lightgrey.svg)](https://go.dev/)
[![GitHub Release](https://img.shields.io/github/release/yylego/kratos-cron.svg)](https://github.com/yylego/kratos-cron/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/yylego/kratos-cron)](https://goreportcard.com/report/github.com/yylego/kratos-cron)
<!-- TEMPLATE (EN) CLOSE: BADGES -->

# kratos-cron

Cron task integration with the Kratos framework. Wraps `robfig/cron` as a Kratos `transport.Server` with opt-in panic catch, startup execution, plus read-lock protected shutdown sections.

---

<!-- TEMPLATE (EN) BEGIN: LANGUAGE NAVIGATION -->

## CHINESE README

[中文说明](README.zh.md)

<!-- TEMPLATE (EN) CLOSE: LANGUAGE NAVIGATION -->

## Main Features

- 🕐 **Kratos transport.Server** — Plug a cron schedule inline into `kratos.New(kratos.Server(...))`
- 🛡️ **Shutdown coordination** — Stops scheduling, waits on tasks and protected sections, then cancels ctx
- ⚡ **Protected sections** — `Stage.Do` holds a read-lock against shutdown cancellation
- 🚀 **Run-once-at-startup** — Task-scope `DoOnStartup()` option, no separate boot hook needed
- 🛟 **Opt-in panic catch** — Task-scope `Recoverable()` / service-side `WithRecover()`
- 🔁 **Nested-Do deadlock-safe** — Nested `stage.Do` calls auto-detect the holding ctx and skip re-locking
- 📦 **Compact surface** — Lifecycle methods plus opt-in task settings

## Installation

```bash
go get github.com/yylego/kratos-cron/cronkratos
```

## Quick Start

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
			// business logic — protected against shutdown cancellation
		})
	}, cronkratos.DoOnStartup()))

	app := kratos.New(kratos.Server(srv))
	must.Done(app.Run())
}
```

## Usage

### 1. Bind a scheduled task

```go
srv.AddFunc("0 */5 * * * *", func(ctx context.Context, stage *cronkratos.Stage) {
    // runs each 5 minutes
})
```

The `cmd` signature is `func(ctx context.Context, stage *Stage)`. The Server injects its own ctx (cancelled on Stop) plus a shared `*Stage` instance.

### 2. Run the task once at startup

```go
srv.AddFunc("0 0 2 * * *", fn, cronkratos.DoOnStartup())
```

`fn` runs on the cron schedule **and** once in a goroutine when `Server.Start()` is called. Apt when a 2am sweep task should also drain unfinished work on each boot.

### 3. Panic catch

Catch is opt-in (matching Kratos gRPC/HTTP convention). Two ways:

```go
// Task-scope — wrap one specific task
srv.AddFunc(spec, fn, cronkratos.Recoverable())

// Service-side — wrap each task added through this Server
srv := cronkratos.NewServer(c, slog, cronkratos.WithRecover())
```

The two compose with **OR** semantics: one flag is enough. A task with no flag set runs raw — panics propagate as standard.

`WithName("sync-quotes")` adds a business name to panic and skip logs. Caught panics include a stack trace.
Both options are off by default. Skipped runs are not queued or replayed. Direct calls from an HTTP endpoint to the business function bypass this gate; put shared exclusion in the business entry point when needed. A task that never returns keeps its gate occupied; this option does not impose a timeout or cancel work.
`SkipIfRunning()` skips overlapping runs of the same registration within one process, including `DoOnStartup()` runs. It logs the elapsed time of active work and releases the gate on return / panic. Active work continues unchanged. Separate registrations remain independent even with matching names; this is not a distributed lock. Work launched in a separate goroutine must be awaited inside the task to remain within the gate.

```go
rese.C1(srv.AddFunc(spec, fn, cronkratos.WithName("sync-quotes"), cronkratos.SkipIfRunning(), cronkratos.Recoverable()))
```

### 4. Clean shutdown coordination — `Stage.Do`

Wrap hot sections with `stage.Do(ctx, fn)`:

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

`stage.Do` acquires the Server's read-lock during `fn`. `Server.Stop()` must acquire the write lock before cancelling its context, so it waits for active protected sections. Check `ctx.Err()` on entry: shutdown may have already cancelled it before entry, and an independently derived context may expire on its own.

`Stop` first waits for scheduled tasks to return or its context to expire, then takes the write lock. A task must not rely solely on this context being cancelled to finish. The stop deadline does not bound the subsequent lock wait.

Holding tags distinguish each `Stage`. Pass the derived context only through synchronous nested calls while the outer call is active; do not reuse it after that call returns or pass it to independent goroutines.

### 5. Nested `stage.Do` is safe

Aid functions can invoke `stage.Do` again — as long as `ctx` flows through the invocation chain, the nested invocation detects the holding tag on `ctx` and skips re-locking, avoiding the classic `RWMutex` reentrance deadlock with a concurrent `Lock()`:

```go
stage.Do(ctx, func(ctx context.Context) {
    helperUsingStage(ctx, stage)  // nested stage.Do sees the tag, skips re-locking
})
```

### 6. On-demand paths (`NonStage`)

When the same business method is invoked outside the cron schedule (e.g. an RPC endpoint that runs the job on demand), the invoking code has no `*Stage` to pass in. Use `cronkratos.NonStage()` to get a no-op Stage — the business method's signature stays clean (`*Stage` is non-nil), and the on-demand path skips shutdown coordination on purpose:

```go
func (s *Service) RunSomething(ctx context.Context) error {
    return s.runSomething(ctx, cronkratos.NonStage())
}
```

When advanced cases need a custom `sync.Locker`, use `cronkratos.NewStage(customLock)` — the same approach `Server` uses inside.

### 7. Plug into Kratos

`*Server` implements `transport.Server` inline:

```go
app := kratos.New(
    kratos.Name("app-service"),
    kratos.Server(httpSrv, grpcSrv, srv),
)
```

`Start` and `Stop` are driven via the Kratos lifecycle. Register tasks before starting. Use one start/stop cycle per instance; registration and lifecycle calls must not run concurrently. Create a new instance to start again after stopping.

Startup tasks run outside cron's own tracking. Only their active `Stage.Do` sections participate in the lock wait during shutdown. Options passed to `cron.WithChain` apply to scheduled runs, not startup runs; this package's task options apply to both.

## Design

### Three-step clean shutdown

`Server.Stop` runs three steps in sequence:

1. **`cron.Stop()`** — stops scheduling new tasks. Returns a ctx that completes when running cron tasks finish. Listens on `Stop`'s own ctx — on timeout it logs a warn but continues to step 2 (`mutex.Lock` does not check ctx; it waits as long as needed).
2. **`mutex.Lock()`** — acquires the write lock, waiting on each holding goroutine (active `stage.Do` callbacks) to release.
3. **`cancel()`** — invalidates the Server's ctx.

The write lock prevents shutdown cancellation during an active protected section. It does not revive a context already cancelled or prevent a deadline set by the caller.

### Stage.Do — picked instead of a raw mutex

Two reasons:

- **Hides the lock.** Business code does not touch `sync.RWMutex` / `RLocker` / write-lock semantics. The sole API surface is `stage.Do(ctx, fn)`.
- **Solves nested reentrance.** `sync.RWMutex.RLock` is _not_ reentrant — with a concurrent `Lock`, a second `RLock` on the same goroutine deadlocks (`Lock`-starvation prevention). `Stage.Do` carries a private tag on `ctx`; nested calls see the tag and skip re-locking, eliminating the deadlock.

### What `cronkratos` hides from the business side

| Underlying concern                         | Handled in here                                        |
| ------------------------------------------ | ------------------------------------------------------ |
| Task panic and skip logs                   | Written through the supplied `*slog.Logger`           |
| `cron.WithChain(cron.Recover(...))` wiring | Replaced via `Recoverable()` / `WithRecover()` options |
| `defer recover()` boilerplate              | Done via `wrapRecoverable` when one flag is set        |
| `sync.RWMutex` + `RLocker` plumbing        | Encapsulated in `Stage.Do`                             |
| `context.WithCancel` lifetime              | Created in `NewServer`, cancelled in `Stop`            |
| Run-once-at-boot bookkeeping               | Driven via `DoOnStartup()` option                      |

The underlying cron instance retains its own logging configuration; this package does not install a `cron.Logger` adapter.

## Demo

See [kratos-cron-demos](https://github.com/yylego/kratos-cron-demos) — a complete Kratos integration example.

---

<!-- TEMPLATE (EN) BEGIN: STANDARD PROJECT FOOTER -->
<!-- VERSION 2025-11-25 03:52:28.131064 +0000 UTC -->

## 📄 License

MIT License - see [LICENSE](LICENSE).

---

## 💬 Contact & Feedback

Contributions are welcome! Report bugs, suggest features, and contribute code:

- 🐛 **Mistake reports?** Open an issue on GitHub with reproduction steps
- 💡 **Fresh ideas?** Create an issue to discuss
- 📖 **Documentation confusing?** Report it so we can improve
- 🚀 **Need new features?** Share the use cases to help us understand requirements
- ⚡ **Performance issue?** Help us optimize through reporting slow operations
- 🔧 **Configuration problem?** Ask questions about complex setups
- 📢 **Follow project progress?** Watch the repo to get new releases and features
- 🌟 **Success stories?** Share how this package improved the workflow
- 💬 **Feedback?** We welcome suggestions and comments

---

## 🔧 Development

New code contributions, follow this process:

1. **Fork**: Fork the repo on GitHub (using the webpage UI).
2. **Clone**: Clone the forked project (`git clone https://github.com/yourname/repo-name.git`).
3. **Navigate**: Navigate to the cloned project (`cd repo-name`)
4. **Branch**: Create a feature branch (`git checkout -b feature/xxx`).
5. **Code**: Implement the changes with comprehensive tests
6. **Testing**: (Golang project) Ensure tests pass (`go test ./...`) and follow Go code style conventions
7. **Documentation**: Update documentation to support client-facing changes
8. **Stage**: Stage changes (`git add .`)
9. **Commit**: Commit changes (`git commit -m "Add feature xxx"`) ensuring backward compatible code
10. **Push**: Push to the branch (`git push origin feature/xxx`).
11. **PR**: Open a merge request on GitHub (on the GitHub webpage) with detailed description.

Please ensure tests pass and include relevant documentation updates.

---

## 🌟 Support

Welcome to contribute to this project via submitting merge requests and reporting issues.

**Project Support:**

- ⭐ **Give GitHub stars** if this project helps you
- 🤝 **Share with teammates** and (golang) programming friends
- 📝 **Write tech blogs** about development tools and workflows - we provide content writing support
- 🌟 **Join the ecosystem** - committed to supporting open source and the (golang) development scene

**Have Fun Coding with this package!** 🎉🎉🎉

<!-- TEMPLATE (EN) CLOSE: STANDARD PROJECT FOOTER -->

---

<!-- TEMPLATE (EN) BEGIN: GITHUB STARS -->
## GitHub Stars

[![Stargazers](https://starchart.cc/yylego/kratos-cron.svg?variant=adaptive)](https://starchart.cc/yylego/kratos-cron)
<!-- TEMPLATE (EN) CLOSE: GITHUB STARS -->
