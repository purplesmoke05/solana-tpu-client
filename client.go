package solana_tpu_client

import (
	"context"
	"github.com/edwingeng/deque/v2"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/ws"
	"golang.org/x/sync/errgroup"
	"gopkg.in/errgo.v2/errors"
	"math"
	"net"
	"sync"
	"time"
)

type LeaderTpuCache struct {
	firstSlot         uint64
	slotsInEpoch      uint64
	lastEpochInfoSlot uint64
	leaders           []solana.PublicKey
	client            *rpc.Client
	leaderTpuMapLock  sync.Mutex
	leaderTpuMap      map[solana.PublicKey]string
}

const MaxSlotSkipDistance = 48
const DefaultFanoutSlots = 12
const MaxFanoutSlots = 100

func NewLeaderTpuCache(client *rpc.Client, startSlot uint64) *LeaderTpuCache {
	return &LeaderTpuCache{
		firstSlot:        startSlot,
		client:           client,
		leaderTpuMapLock: sync.Mutex{},
	}
}

func (l *LeaderTpuCache) fetchSlotLeaders(ctx context.Context, startSlot uint64, slotsInEpoch uint64) ([]solana.PublicKey, error) {
	fanout := math.Min(2*MaxFanoutSlots, float64(slotsInEpoch))
	leaders, err := l.client.GetSlotLeaders(ctx, startSlot, uint64(fanout))
	if err != nil {
		return nil, err
	}
	return leaders, nil
}

func LeaderTpuCacheLoad(ctx context.Context, client *rpc.Client, startSlot uint64) (*LeaderTpuCache, error) {
	leaderTpuCache := NewLeaderTpuCache(client, startSlot)
	info, err := leaderTpuCache.client.GetEpochInfo(ctx, "")
	if err != nil {
		return nil, err
	}
	leaderTpuCache.slotsInEpoch = info.SlotsInEpoch
	leaders, err := leaderTpuCache.fetchSlotLeaders(ctx, leaderTpuCache.firstSlot, leaderTpuCache.slotsInEpoch)
	if err != nil {
		return nil, err
	}
	leaderTpuCache.leaders = leaders
	socks, err := leaderTpuCache.fetchClusterTpuSockets(ctx)
	if err != nil {
		return nil, err
	}
	leaderTpuCache.leaderTpuMapLock.Lock()
	leaderTpuCache.leaderTpuMap = socks
	leaderTpuCache.leaderTpuMapLock.Unlock()
	return leaderTpuCache, nil
}

func (l *LeaderTpuCache) fetchClusterTpuSockets(ctx context.Context) (map[solana.PublicKey]string, error) {
	nodes, err := l.client.GetClusterNodes(ctx)
	if err != nil {
		return nil, err
	}
	ret := make(map[solana.PublicKey]string)
	for _, n := range nodes {
		if n.TPU != nil {
			ret[n.Pubkey] = *n.TPU
		}
	}
	return ret, nil
}

func (l *LeaderTpuCache) lastSlot() uint64 {
	return l.firstSlot + uint64(len(l.leaders)) - 1
}

func (l *LeaderTpuCache) getSlotLeader(slot uint64) *solana.PublicKey {
	if slot > l.firstSlot {
		index := slot - l.firstSlot
		for i := range l.leaders {
			if uint64(i) == index {
				return &l.leaders[index]
			}
		}
	}
	return nil
}

func (l *LeaderTpuCache) getLeaderSockets(fanoutSlots uint64) []string {
	uniqueLeaderList := make([]string, 0)
	leaderSockets := make([]string, 0)
	checkedSlots := uint64(0)
	for _, leader := range l.leaders {
		tpuSocket, ok := l.leaderTpuMap[leader]
		if ok {
			if !contains(uniqueLeaderList, leader.String()) {
				uniqueLeaderList = append(uniqueLeaderList, leader.String())
				leaderSockets = append(leaderSockets, tpuSocket)
			}
		} else {

		}
		checkedSlots++
		if checkedSlots == fanoutSlots {
			break
		}
	}
	return leaderSockets
}

type RecentLeaderSlots struct {
	recentSlots *deque.Deque[uint64]
}

func NewRecentLeaderSlots(currentSlot uint64) *RecentLeaderSlots {
	de := deque.NewDeque[uint64]()
	de.PushFront(currentSlot)
	return &RecentLeaderSlots{recentSlots: de}
}

func (r *RecentLeaderSlots) recordSlot(currentSlot uint64) {
	r.recentSlots.PushBack(currentSlot)
	for r.recentSlots.Len() > 12 {
		r.recentSlots.PopBack()
	}
}

func (r *RecentLeaderSlots) estimateCurrentSlot() (uint64, error) {
	if r.recentSlots.IsEmpty() {
		return 0, errors.New("recent slots is empty")
	}
	sortedRecentSlots := r.recentSlots.Dump()
	sortSlice[uint64](sortedRecentSlots)
	maxIndex := uint64(len(sortedRecentSlots)) - 1
	medianIndex := maxIndex / 2
	medianRecentSlot := sortedRecentSlots[medianIndex]
	expectedCurrentSLot := medianRecentSlot + (maxIndex - medianIndex)
	maxReasonableCurrentSlot := expectedCurrentSLot + MaxSlotSkipDistance
	reverseSlice[uint64](sortedRecentSlots)
	for _, slot := range sortedRecentSlots {
		if slot <= maxReasonableCurrentSlot {
			return slot, nil
		}
	}
	return 0, errors.New("failed to find reasonable slot")
}

type LeaderTpuService struct {
	recentSlots    *RecentLeaderSlots
	leaderTpuCache *LeaderTpuCache
	subscription   uint64
	client         *rpc.Client
}

func NewLeaderTpuService(client *rpc.Client) *LeaderTpuService {
	return &LeaderTpuService{client: client}
}

func LeaderTpuServiceLoad(wg *errgroup.Group, ctx context.Context, client *rpc.Client, wsClient *ws.Client, websocketUrl string) (*LeaderTpuService, error) {
	leaderTpuService := NewLeaderTpuService(client)
	slot, err := client.GetSlot(ctx, "processed")
	if err != nil {
		return nil, err
	}
	leaderTpuService.recentSlots = NewRecentLeaderSlots(slot)
	leaderTpuCache, err := LeaderTpuCacheLoad(ctx, client, slot)
	if err != nil {
		return nil, err
	}
	leaderTpuService.leaderTpuCache = leaderTpuCache
	if websocketUrl != "" {
		wg.Go(
			func() error {
				return func(ctx2 context.Context, client2 *ws.Client) error {
					subscription, err := client2.SlotsUpdatesSubscribe()
					if err != nil {
						return err
					}
					defer subscription.Unsubscribe()

					for {
						select {
						case <-ctx2.Done():
							return nil
						default:
							res, err := subscription.Recv()
							if err != nil {
								return err
							}
							if res.Type == "completed" {
								res.Slot += 1
							}
							leaderTpuService.recentSlots.recordSlot(res.Slot)
						}
					}
				}(ctx, wsClient)
			})
	}
	wg.Go(func() error {
		leaderTpuService.run(ctx)
		return nil
	})

	return leaderTpuService, nil
}

func (l *LeaderTpuService) leaderTpuSockets(fanoutSlots uint64) []string {
	return l.leaderTpuCache.getLeaderSockets(fanoutSlots)
}

func (l *LeaderTpuService) run(ctx context.Context) {
	lastClusterRefresh := time.Now()
	t := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case <-t.C:
			if time.Now().UnixMilli()-lastClusterRefresh.UnixMilli() > 1000*6*10 {
				socks, err := l.leaderTpuCache.fetchClusterTpuSockets(ctx)
				if err != nil {
					Get().Warn("failed to fetch cluster tpu sockets")
					continue
				}
				l.leaderTpuCache.leaderTpuMapLock.Lock()
				l.leaderTpuCache.leaderTpuMap = socks
				l.leaderTpuCache.leaderTpuMapLock.Unlock()
				lastClusterRefresh = time.Now()
			}
			estimatedCurrentSlot, err := l.recentSlots.estimateCurrentSlot()
			if err != nil {
				Get().Error(err)
				continue
			}
			if estimatedCurrentSlot >= l.leaderTpuCache.lastEpochInfoSlot-l.leaderTpuCache.slotsInEpoch {
				epochInfo, err := l.client.GetEpochInfo(ctx, "recent")
				if err != nil {
					Get().Warn("failed to get epoch info: %v", err)
				} else {
					l.leaderTpuCache.slotsInEpoch = epochInfo.SlotsInEpoch
					l.leaderTpuCache.lastEpochInfoSlot = estimatedCurrentSlot
				}
			}
			if estimatedCurrentSlot >= (l.leaderTpuCache.lastSlot() - MaxFanoutSlots) {
				slotLeaders, err := l.leaderTpuCache.fetchSlotLeaders(ctx, estimatedCurrentSlot, l.leaderTpuCache.slotsInEpoch)
				if err != nil {
					Get().Warn("failed to fetch slot leaders (current estimated slot: %d): %v", estimatedCurrentSlot, err)
					continue
				}
				l.leaderTpuCache.firstSlot = estimatedCurrentSlot
				l.leaderTpuCache.leaders = slotLeaders
			}
		case <-ctx.Done():
			return
		}
	}
}

type TpuClientConfig struct {
	fanoutSlots uint64
}

type TpuClient struct {
	fanoutSlots      uint64
	leaderTpuService *LeaderTpuService
	exit             bool
	client           *rpc.Client
}

func NewTpuClient(client *rpc.Client, cfg *TpuClientConfig) *TpuClient {
	fanoutSlots := uint64(math.Max(math.Min(float64(cfg.fanoutSlots), MaxFanoutSlots), 1))

	return &TpuClient{fanoutSlots: fanoutSlots, client: client, exit: false}
}

func TpuClientLoad(eg *errgroup.Group, ctx context.Context, client *rpc.Client, wsClient *ws.Client, websocketUrl string, cfg *TpuClientConfig) (*TpuClient, error) {
	tpuClient := NewTpuClient(client, cfg)
	leaderTpuService, err := LeaderTpuServiceLoad(eg, ctx, client, wsClient, websocketUrl)
	if err != nil {
		return nil, err
	}
	tpuClient.leaderTpuService = leaderTpuService
	return tpuClient, nil
}

func (t *TpuClient) SendTransaction(ctx context.Context, instructions []solana.Instruction, signers []solana.PrivateKey, payer solana.PublicKey) (string, error) {
	recent, err := t.client.GetRecentBlockhash(ctx, "")
	if err != nil {
		return "", err
	}
	tx, err := solana.NewTransaction(instructions, recent.Value.Blockhash, solana.TransactionPayer(payer))
	if err != nil {
		return "", err
	}
	tx.Signatures = []solana.Signature{}
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		for _, candidate := range signers {
			if candidate.PublicKey().Equals(key) {
				return &candidate
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return t.sendRawTransaction(tx)
}

func (t *TpuClient) sendRawTransaction(tx *solana.Transaction) (string, error) {
	rawTransaction, err := tx.MarshalBinary()
	if err != nil {
		return "", err
	}
	tpuAddresses := t.leaderTpuService.leaderTpuSockets(t.fanoutSlots)
	for _, address := range tpuAddresses {
		udpAddress, err := net.ResolveUDPAddr("udp4", address)
		if err != nil {
			Get().Warn(err)
			continue
		}
		conn, err := net.DialUDP("udp", nil, udpAddress)
		if err != nil {
			Get().Warn(err)
			continue
		}
		_, err = conn.Write(rawTransaction)
		if err != nil {
			Get().Warn(err)
			continue
		}
		return tx.Signatures[0].String(), nil
	}
	return "", errors.New("failed to send rawTransaction")
}

type TpuConnection struct {
	tpuClient *TpuClient
}

func NewTpuConnection(eg *errgroup.Group, ctx context.Context, rpcEndpoint string, wsEndpoint string) (*TpuConnection, error) {
	rpcClient := rpc.New(rpcEndpoint)
	wsClient, err := ws.Connect(ctx, wsEndpoint)
	if err != nil {
		return nil, err
	}
	tpuClient, err := TpuClientLoad(eg, ctx, rpcClient, wsClient, wsEndpoint, &TpuClientConfig{fanoutSlots: DefaultFanoutSlots})
	if err != nil {
		return nil, err
	}
	return &TpuConnection{tpuClient: tpuClient}, nil
}

func (t *TpuConnection) SendTransaction(ctx context.Context, instructions []solana.Instruction, signers []solana.PrivateKey, payer solana.PublicKey) (string, error) {
	return t.tpuClient.SendTransaction(ctx, instructions, signers, payer)
}

func TpuConnectionLoad(eg *errgroup.Group, ctx context.Context, rpcEndpoint, wsEndpoint string) (*TpuConnection, error) {
	return NewTpuConnection(eg, ctx, rpcEndpoint, wsEndpoint)
}
