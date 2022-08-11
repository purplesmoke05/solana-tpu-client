package solana_tpu_client

import (
	"context"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/test-go/testify/assert"
	"testing"
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
