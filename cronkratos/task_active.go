package cronkratos

import (
	"fmt"
	"slices"
	"time"
)

// awaitReportDuration paces the "cron awaiting tasks" logs while Stop waits; it is
// a fifth of the common 10-second grace that process supervisors give.
// awaitReportDuration 是 Stop 等待期间重复报出剩余任务的间隔，远小于进程托管常见的 10 秒宽限期。
const awaitReportDuration = 2 * time.Second

// activeRun records one run in progress of a registered task.
// activeRun 记录注册任务的一次正在进行的执行。
type activeRun struct {
	spec    string
	name    string
	started time.Time
}

// wrapActive tracks each run so Stop can report the runs it waits on.
// wrapActive 登记每次执行，供 Stop 报出它在等待的任务。
func (s *Server) wrapActive(spec, name string, cmd func()) func() {
	return func() {
		run := &activeRun{spec: spec, name: name, started: time.Now()}
		s.activeMutex.Lock()
		s.active[run] = struct{}{}
		s.activeMutex.Unlock()
		defer func() {
			s.activeMutex.Lock()
			delete(s.active, run)
			stopping, count := s.stopping, len(s.active)
			s.activeMutex.Unlock()
			if stopping {
				s.slog.Info("cron task exited", "spec", spec, "name", name, "elapsed", time.Since(run.started), "remaining", count)
			}
		}()
		cmd()
	}
}

// activeRuns lists the runs in progress, oldest first, as "name(elapsed)", using spec when the name is blank.
// activeRuns 按开始时间从早到晚列出正在执行的任务，格式为"名称(已运行时长)"，没有名称时用 spec。
func (s *Server) activeRuns() []string {
	s.activeMutex.Lock()
	runs := make([]*activeRun, 0, len(s.active))
	for run := range s.active {
		runs = append(runs, run)
	}
	s.activeMutex.Unlock()
	slices.SortFunc(runs, func(a, b *activeRun) int { return a.started.Compare(b.started) })
	names := make([]string, 0, len(runs))
	for _, run := range runs {
		name := run.name
		if name == "" {
			name = run.spec
		}
		names = append(names, fmt.Sprintf("%s(%s)", name, time.Since(run.started).Truncate(time.Millisecond)))
	}
	return names
}
