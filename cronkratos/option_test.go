package cronkratos_test

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"github.com/yylego/erero"
	"github.com/yylego/kratos-cron/cronkratos"
	"github.com/yylego/rese"
)

// TestRecoverable checks a task added with Recoverable() gets its panics caught on a Server that has no WithRecover(),
// so the process stays up and the schedule keeps firing
//
// TestRecoverable 检查 Server 不带 WithRecover() 时，带 Recoverable() 的 task 发生 panic 也被兜住，进程不崩，调度照常
func TestRecoverable(t *testing.T) {
	srv := cronkratos.NewServer(cron.New(cron.WithSeconds()), slog.Default())

	var fired atomic.Int32
	rese.C1(srv.AddFunc("* * * * * *", func(_ context.Context, _ *cronkratos.Stage) {
		fired.Add(1)
		panic(erero.New("recoverable task panic"))
	}, cronkratos.Recoverable()))

	require.NoError(t, srv.Start(context.Background()))
	time.Sleep(2500 * time.Millisecond)
	require.NoError(t, srv.Stop(context.Background()))

	require.GreaterOrEqual(t, fired.Load(), int32(2))
}
