package cronkratos

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

func TestTaskGateConcurrent(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			c := cron.New()
			srv := NewServer(c, slog.New(slog.NewTextHandler(io.Discard, nil)))
			entered := make(chan struct{}, 33)
			release := make(chan struct{})
			var cleanup sync.Once
			defer cleanup.Do(func() { close(release) })
			var calls atomic.Int32
			opts := []TaskOption{WithName("sample")}
			if enabled {
				opts = append(opts, SkipIfRunning())
			}
			id, err := srv.AddFunc("0 0 * * *", func(context.Context, *Stage) {
				calls.Add(1)
				entered <- struct{}{}
				<-release
			}, opts...)
			require.NoError(t, err)
			job := c.Entry(id).Job
			var runs sync.WaitGroup
			runs.Add(1)
			go func() { defer runs.Done(); job.Run() }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("task did not start")
			}
			var attempts sync.WaitGroup
			for n := 0; n < 32; n++ {
				attempts.Add(1)
				go func() { defer attempts.Done(); job.Run() }()
			}
			done := make(chan struct{})
			go func() { attempts.Wait(); close(done) }()
			if enabled {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("overlapping runs did not skip")
				}
				require.EqualValues(t, 1, calls.Load())
			} else {
				for n := 0; n < 32; n++ {
					select {
					case <-entered:
					case <-time.After(time.Second):
						t.Fatal("default task did not allow overlap")
					}
				}
				require.EqualValues(t, 33, calls.Load())
			}
			cleanup.Do(func() { close(release) })
			runs.Wait()
			<-done
			before := calls.Load()
			job.Run()
			require.Equal(t, before+1, calls.Load())
		})
	}
}

func TestTaskGateStartupAndSchedule(t *testing.T) {
	var logs bytes.Buffer
	c := cron.New()
	srv := NewServer(c, slog.New(slog.NewTextHandler(&logs, nil)))
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	id, err := srv.AddFunc("0 0 * * *", func(context.Context, *Stage) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
	}, DoOnStartup(), SkipIfRunning(), WithName("sample"))
	require.NoError(t, err)
	go func() { srv.startupRuns[0](); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("startup task did not run")
	}
	c.Entry(id).Job.Run()
	require.EqualValues(t, 1, calls.Load())
	require.Contains(t, logs.String(), "cron task skipped")
	require.Contains(t, logs.String(), "sample")
	// A distinct registration with the same name has its own gate.
	// 同名的不同注册项各有独立门控。
	nextID, err := srv.AddFunc("0 0 * * *", func(context.Context, *Stage) { calls.Add(1) }, SkipIfRunning(), WithName("sample"))
	require.NoError(t, err)
	c.Entry(nextID).Job.Run()
	require.EqualValues(t, 2, calls.Load())
	release <- struct{}{}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("startup task did not complete")
	}
	c.Entry(id).Job.Run()
	require.EqualValues(t, 3, calls.Load())
}

func TestTaskGatePanicRelease(t *testing.T) {
	var logs bytes.Buffer
	c := cron.New()
	srv := NewServer(c, slog.New(slog.NewTextHandler(&logs, nil)), WithRecover())
	var calls int
	id, err := srv.AddFunc("0 0 * * *", func(context.Context, *Stage) {
		calls++
		panic(fmt.Errorf("boom"))
	}, SkipIfRunning(), WithName("panic-case"))
	require.NoError(t, err)
	c.Entry(id).Job.Run()
	c.Entry(id).Job.Run()
	require.Equal(t, 2, calls)
	require.Contains(t, logs.String(), "panic-case")
	require.Contains(t, logs.String(), "stack=")
}
