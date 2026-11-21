package cronkratos

import (
	"bytes"
	"context"
	"log/slog"
	"regexp"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

// elapsedSuffix matches the "(elapsed)" at the end of one activeRuns item
// elapsedSuffix 匹配 activeRuns 每一项末尾的 "(已运行时长)"
var elapsedSuffix = regexp.MustCompile(`\([^()]*\)$`)

// newStableLogs returns a slog instance whose text output stays the same on each run, plus the output:
// the time is dropped, each duration reads DURATION, the panic stack reads STACK.
//
// newStableLogs 返回一个每次运行输出都相同的 slog 实例和它的输出：
// 去掉时间，时长一律写成 DURATION，panic 的调用栈写成 STACK。
func newStableLogs() (*slog.Logger, *bytes.Buffer) {
	output := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(output, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, field slog.Attr) slog.Attr {
			if field.Key == slog.TimeKey {
				return slog.Attr{}
			}
			if field.Key == "stack" {
				return slog.String(field.Key, "STACK")
			}
			if field.Value.Kind() == slog.KindDuration {
				return slog.String(field.Key, "DURATION")
			}
			if runs, ok := field.Value.Any().([]string); ok {
				stable := make([]string, 0, len(runs))
				for _, run := range runs {
					stable = append(stable, elapsedSuffix.ReplaceAllString(run, "(DURATION)"))
				}
				return slog.Any(field.Key, stable)
			}
			return field
		},
	})), output
}

// TestActiveRunsAndStopReport checks the active list (WithName name, else spec) while tasks run,
// then the Stop logs: start, cancellation, each exit, periodic report and the close.
//
// TestActiveRunsAndStopReport 先验证任务运行期间的名单（有名称用名称，没有用 spec），
// 再验证 Stop 的日志：开始、取消、每个任务退出、定时汇报和结束。
func TestActiveRunsAndStopReport(t *testing.T) {
	logger, logs := newStableLogs()
	c := cron.New()
	srv := NewServer(c, logger)
	entered := make(chan struct{})
	// The named task starts with the service, and keeps going past one periodic report once ctx is done
	// 带名称的任务随服务启动，ctx 取消后还要跑过一次定时汇报
	_, err := srv.AddFunc("0 0 1 1 *", func(ctx context.Context, _ *Stage) {
		entered <- struct{}{}
		<-ctx.Done()
		time.Sleep(awaitReportDuration + 300*time.Millisecond)
	}, DoOnStartup(), WithName("slow-task"))
	require.NoError(t, err)
	// The task without a name runs as a scheduled run, and exits soon once ctx is done
	// 没有名称的任务按定时触发执行，ctx 取消后很快退出
	id, err := srv.AddFunc("0 0 2 1 *", func(ctx context.Context, _ *Stage) {
		entered <- struct{}{}
		<-ctx.Done()
		time.Sleep(300 * time.Millisecond)
	})
	require.NoError(t, err)

	require.NoError(t, srv.Start(context.Background()))
	<-entered
	go c.Entry(id).Job.Run() // Starts once the named task is active, so the list has a fixed sequence // 等带名称的任务跑起来再触发，名单顺序才固定
	<-entered
	var names []string
	for _, item := range srv.activeRuns() {
		names = append(names, elapsedSuffix.ReplaceAllString(item, ""))
	}
	require.Equal(t, []string{"slow-task", "0 0 2 1 *"}, names)

	require.NoError(t, srv.Stop(context.Background()))
	require.Empty(t, srv.activeRuns())
	require.Equal(t, `level=INFO msg="cron starting"
level=INFO msg="cron triggered startup tasks" count=1
level=INFO msg="cron stopping" active="[slow-task(DURATION) 0 0 2 1 *(DURATION)]"
level=INFO msg="cron context cancelled" waited=DURATION active="[slow-task(DURATION) 0 0 2 1 *(DURATION)]"
level=INFO msg="cron task exited" spec="0 0 2 1 *" name="" elapsed=DURATION remaining=1
level=INFO msg="cron awaiting tasks" waited=DURATION active=[slow-task(DURATION)]
level=INFO msg="cron task exited" spec="0 0 1 1 *" name=slow-task elapsed=DURATION remaining=0
level=INFO msg="cron shutdown complete" waited=DURATION
`, logs.String())
}
