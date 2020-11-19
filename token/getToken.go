package token

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTHost/client"
	hi "github.com/yottachain/YTHost/interface"
	"github.com/yottachain/YTSDKTestTool/conn"
	cm "github.com/yottachain/YTStTool/ClientManage"
	st "github.com/yottachain/YTStTool/stat"
	"math/rand"
	"sync"
	"time"
)

type IdToToken struct {
	pid peer.ID
	addrs []multiaddr.Multiaddr
	tk *message.NodeCapacityResponse
}

func (idt *IdToToken) GetPid () peer.ID{
	return idt.pid
}

func (idt *IdToToken) GetAddrs () []multiaddr.Multiaddr{
	return idt.addrs
}

func (idt *IdToToken) GetToken () *message.NodeCapacityResponse{
	return idt.tk
}

func gettoken (hst hi.Host, ab *cm.AddrsBook, gtkQ chan struct{}, tkpool chan *IdToToken,
			wg *sync.WaitGroup, cst *st.Ccstat, nst *st.NodeStat, tpl *sync.Mutex, connNowait bool) {
	defer func() {
		<- gtkQ
		wg.Done()
	}()
	wg.Add(1)


	var r = rand.New(rand.NewSource(time.Now().UnixNano()))

	abLen := ab.GetWeightsLen()
	if abLen == 0 {
		log.WithFields(log.Fields{
			"len": 0,
		}).Info("addrbook len error")
		return
	}
	idx := r.Intn(abLen)

	nId := ab.GetWeightId(idx)
	nst.SetWeight(nId, ab.GetIdWeight(nId))
	addrs, ok := ab.Get(nId)
	if !ok {
		log.WithFields(log.Fields{
			"peer id": nId,
		}).Error("get addr error")
		return
	}

	var clt *client.YTHostClient
	if connNowait {
		clt = hst.ClientStore().GetUsePid(nId)
		if clt == nil {
			go conn.Connect(hst, nId, addrs)
			nst.ConnErrAdd(nId)
			return
		}
	}else {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
		defer cancel()
		c, err := hst.ClientStore().Get(ctx, nId, addrs)
		clt = c
		if err != nil {
			ADDRs := make([]string, len(addrs))
			for k, m := range addrs {
				ADDRs[k] = m.String()
			}
			var sAddrs string
			for _, v := range ADDRs {
				sAddrs = sAddrs + v + " "
			}
			log.WithFields(log.Fields{
				"nodeid": peer.Encode(nId),
				"addrs":  sAddrs,
			}).Error("get token connect fail error=", err)

			nst.ConnErrAdd(nId)
			return
		}
	}

	nst.ConnSuccAdd(nId)

	var getToken message.NodeCapacityRequest
	getTokenData, err := proto.Marshal(&getToken)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("request proto marshal error")

		return
	}

	cst.GtccAdd()
	cst.GtsAdd()

	ctx1, cal := context.WithTimeout(context.Background(), time.Second*1)
	defer cal()
	ssTime := time.Now()
	res, err := clt.SendMsg(ctx1, message.MsgIDNodeCapacityRequest.Value(), getTokenData)
	if err != nil {
		//log.WithFields(log.Fields{
		//	"nodeid": peer.Encode(nId),
		//	"error": err,
		//}).Error("get token fail")

		nst.GtDelay(nId, time.Now().Sub(ssTime))
		nst.GtErrAdd(nId)
		cst.GtccSub()
		return
	}

	dly := time.Now().Sub(ssTime)
	nst.GtDelay(nId, dly)

	var resGetToken message.NodeCapacityResponse
	err = proto.Unmarshal(res[2:], &resGetToken)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
			"err": err,
		}).Error("get token response proto Unmarshal error")

		nst.GtErrAdd(nId)
		cst.GtccSub()
		return
	}

	if !resGetToken.Writable  {
		nst.GtErrAdd(nId)
		cst.GtccSub()
		//log.WithFields(log.Fields{
		//	"nodeid": peer.Encode(nId),
		//}).Error("get token fail, token unavailable")
		return
	}else {
		log.WithFields(log.Fields{
			"矿机": peer.Encode(nId),
		}).Info("获取token成功")
	}

	nst.GtSuccAdd(nId)
	cst.GtccSub()
	cst.GtSucsAdd()

	tpl.Lock()
	tkpoolLen := len(tkpool)
	if tkpoolLen < cap(tkpool) {
		cst.SetTkPoolLen(tkpoolLen)
		tkpool <- &IdToToken{nId, addrs, &resGetToken}
	}
	tpl.Unlock()

	return
}


func GetTkToPool(hst hi.Host, ab *cm.AddrsBook, gtkQ chan struct{},
				tkPool chan *IdToToken, isStop *bool, wg *sync.WaitGroup, cst *st.Ccstat, nst *st.NodeStat, connNowait bool) {
	tKPoollck := &sync.Mutex{}
	for {
		if *isStop {
			break
		}
		gtkQ <- struct{}{}
		go gettoken(hst, ab, gtkQ, tkPool, wg, cst, nst, tKPoollck, connNowait)
	}
}