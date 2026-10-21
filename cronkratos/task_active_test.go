package cronkratos

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

// TestActiveRunsAndStopReport checks the active list (WithName name, else spec) while tasks run,
// then the Stop logs: start, cancellation, periodic report, each exit and the close.
//
// TestActiveRunsAndStopReport 先验证任务运行期间的名单（有名称用名称，没有用 spec），
// 再验证 Stop 的日志：开始、取消、定时汇报、每个任务退出和结束。
func TestActiveRunsAndStopReport(t *testing.T) {
	var logs bytes.Buffer
	srv := NewServer(cron.New(), slog.New(slog.NewTextHandler(&logs, nil)))
	entered := make(chan struct{}, 2)
	_, err := srv.AddFunc("0 0 1 1 *", func(ctx context.Context, _ *Stage) {
		entered <- struct{}{}
		<-ctx.Done()
		time.Sleep(awaitReportDuration + 300*time.Millisecond)
	}, DoOnStartup(), WithName("slow-task"))
	require.NoError(t, err)
	_, err = srv.AddFunc("0 0 2 1 *", func(ctx context.Context, _ *Stage) {
		entered <- struct{}{}
		<-ctx.Done()
	}, DoOnStartup())
	require.NoError(t, err)

	require.NoError(t, srv.Start(context.Background()))
	<-entered
	<-entered
	var names []string
	for _, item := range srv.activeRuns() {
		name, _, _ := strings.Cut(item, "(")
		names = append(names, name)
	}
	require.ElementsMatch(t, []string{"slow-task", "0 0 2 1 *"}, names)

	require.NoError(t, srv.Stop(context.Background()))
	require.Empty(t, srv.activeRuns())
	text := logs.String()
	for _, part := range []string{
		`msg="cron stopping"`,
		`msg="cron context cancelled"`,
		`msg="cron awaiting tasks"`,
		`msg="cron task exited" spec="0 0 2 1 *" name="" `,
		`msg="cron task exited" spec="0 0 1 1 *" name=slow-task `,
		`remaining=0`,
		`msg="cron shutdown complete"`,
	} {
		require.Contains(t, text, part)
	}
}
