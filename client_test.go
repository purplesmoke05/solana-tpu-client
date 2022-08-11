package solana_tpu_client

import (
	"context"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/ws"
	"github.com/test-go/testify/assert"
	"golang.org/x/sync/errgroup"
	"testing"
	"time"
)

const TEST_RPC_ENDPOINT = "https://api.mainnet-beta.solana.com/"
const TEST_RPC_WS_ENDPOINT = "ws://api.mainnet-beta.solana.com/"

func TestLeaderTpuCache(t *testing.T) {
	ctx := context.Background()
	rpcClient := rpc.New(TEST_RPC_ENDPOINT)

	t.Run("fetchSlotLeaders", func(t *testing.T) {
		info, err := rpcClient.GetEpochInfo(ctx, "")
		assert.Nil(t, err)

		startSlots := info.AbsoluteSlot
		leaderTpuCache := NewLeaderTpuCache(rpcClient, startSlots)
		leaders, err := leaderTpuCache.fetchSlotLeaders(ctx, startSlots, info.SlotsInEpoch)
		assert.Nil(t, err)
		assert.Equal(t, 200, len(leaders))
	})
	t.Run("Load", func(t *testing.T) {
		info, err := rpcClient.GetEpochInfo(ctx, "")
		assert.Nil(t, err)

		leaderTpuCache, err := LeaderTpuCacheLoad(ctx, rpcClient, info.AbsoluteSlot)
		assert.Nil(t, err)
		assert.Equal(t, 200, len(leaderTpuCache.leaders))
	})
	t.Run("lastSlot", func(t *testing.T) {
		info, err := rpcClient.GetEpochInfo(ctx, "")
		assert.Nil(t, err)

		startSlots := info.AbsoluteSlot
		leaderTpuCache := NewLeaderTpuCache(rpcClient, startSlots)
		lastSlot := leaderTpuCache.lastSlot()
		assert.Equal(t, lastSlot, startSlots+uint64(len(leaderTpuCache.leaders))-1)
	})
	t.Run("getSlotLeader", func(t *testing.T) {
		info, err := rpcClient.GetEpochInfo(ctx, "")
		assert.Nil(t, err)

		leaderTpuCache, err := LeaderTpuCacheLoad(ctx, rpcClient, info.AbsoluteSlot)
		nextLeader := leaderTpuCache.getSlotLeader(info.AbsoluteSlot + 1)
		assert.NotNil(t, nextLeader)
	})
	t.Run("getLeaderSockets", func(t *testing.T) {
		info, err := rpcClient.GetEpochInfo(ctx, "")
		assert.Nil(t, err)

		leaderTpuCache, err := LeaderTpuCacheLoad(ctx, rpcClient, info.AbsoluteSlot)
		tpuEndpoints := leaderTpuCache.getLeaderSockets(0)
		assert.NotZero(t, len(tpuEndpoints))
	})
}

func TestRecentLeaderSlots(t *testing.T) {
	ctx := context.Background()
	rpcClient := rpc.New(TEST_RPC_ENDPOINT)

	info, err := rpcClient.GetEpochInfo(ctx, "")
	assert.Nil(t, err)

	t.Run("Constructor", func(t *testing.T) {
		_ = NewRecentLeaderSlots(info.AbsoluteSlot)
	})
}

func TestLeaderTpuService(t *testing.T) {
	ctx := context.Background()
	rpcClient := rpc.New(TEST_RPC_ENDPOINT)
	wsClient, err := ws.Connect(ctx, TEST_RPC_WS_ENDPOINT)
	assert.Nil(t, err)

	t.Run("Load", func(t *testing.T) {
		ctxChild, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()
		eg := &errgroup.Group{}
		leaderTpuService, _ := LeaderTpuServiceLoad(eg, ctxChild, rpcClient, wsClient, TEST_RPC_WS_ENDPOINT)
		assert.NotNil(t, leaderTpuService)
		err := eg.Wait()
		assert.Nil(t, err)
	})
}

func TestTpuClient(t *testing.T) {
	ctx := context.Background()
	rpcClient := rpc.New(TEST_RPC_ENDPOINT)
	wsClient, err := ws.Connect(ctx, TEST_RPC_WS_ENDPOINT)
	assert.Nil(t, err)

	t.Run("Load", func(t *testing.T) {
		ctxChild, cancelFunc := context.WithTimeout(ctx, 70*time.Second)
		defer cancelFunc()
		eg := &errgroup.Group{}
		cfg := &TpuClientConfig{
			fanoutSlots: 400,
		}
		tpuClient, err := TpuClientLoad(eg, ctxChild, rpcClient, wsClient, TEST_RPC_WS_ENDPOINT, cfg)
		assert.NotNil(t, tpuClient)
		assert.Nil(t, err)
		err = eg.Wait()
		assert.Nil(t, err)
	})

}
