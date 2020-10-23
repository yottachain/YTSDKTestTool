package conn

import (
	"context"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	hi "github.com/yottachain/YTHost/interface"
	"time"
)

func Connect (hst hi.Host, pid peer.ID, mas []multiaddr.Multiaddr) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
	defer cancel()
	_, _ = hst.ClientStore().Get(ctx, pid, mas)
}
