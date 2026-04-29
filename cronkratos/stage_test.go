package cronkratos_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yylego/kratos-cron/cronkratos"
)

// TestNonStage_DoesNotBlock verifies NonStage's Do runs fn inline without
// blocking — noopMutex.Lock/Unlock are no-ops
//
// TestNonStage_DoesNotBlock 验证 NonStage 的 Do 不会阻塞 — noopMutex 的 Lock/Unlock 都是空操作
func TestNonStage_DoesNotBlock(t *testing.T) {
	stage := cronkratos.NonStage()
	var ran int32
	stage.Do(context.Background(), func(_ context.Context) {
		atomic.StoreInt32(&ran, 1)
	})
	require.Equal(t, int32(1), atomic.LoadInt32(&ran))
}

// TestNonStage_NestedDoOK verifies nested Do on a NonStage works -
// the ctx-tag nested-Do guard kicks in same as on a standard Stage
//
// TestNonStage_NestedDoOK 验证 NonStage 嵌套 Do 没问题 — ctx 标记防嵌套机制照常生效
func TestNonStage_NestedDoOK(t *testing.T) {
	stage := cronkratos.NonStage()
	var nestRan int32
	stage.Do(context.Background(), func(ctx context.Context) {
		stage.Do(ctx, func(_ context.Context) {
			atomic.StoreInt32(&nestRan, 1)
		})
	})
	require.Equal(t, int32(1), atomic.LoadInt32(&nestRan))
}
