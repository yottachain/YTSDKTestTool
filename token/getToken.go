package token

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"
	"github.com/yottachain/YTDataNode/message"
	hi "github.com/yottachain/YTHost/interface"
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
			wg *sync.WaitGroup, cst *st.Ccstat, nst *st.NodeStat, tpl *sync.Mutex) {
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
	//log.WithFields(log.Fields{
	//	"index": idx,
	//}).Error("get addr index")

	nId := ab.GetWeightId(idx)
	nst.SetWeight(nId, ab.GetIdWeight(nId))

	addrs, ok := ab.Get(nId)
	if !ok {
		log.WithFields(log.Fields{
			"peer id": nId,
		}).Error("get addr error")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
	defer cancel()
	clt, err := hst.ClientStore().Get(ctx, nId, addrs)
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
			"addrs": sAddrs,
		}).Error("connect fail error=", err)

		nst.ConnErrAdd(nId)
		return
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
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
		}).Error("get token fail")

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
	//log.WithFields(log.Fields{
	//	"cap": cap(tkpool),
	//	"len": tkpoolLen,
	//}).Info("token pool cap and len")

	if tkpoolLen < cap(tkpool) {
		cst.SetTkPoolLen(tkpoolLen)
		tkpool <- &IdToToken{nId, addrs, &resGetToken}
	}
	tpl.Unlock()

	return
}


func GetTkToPool(hst hi.Host, ab *cm.AddrsBook, gtkQ chan struct{},
				tkPool chan *IdToToken, isStop *bool, wg *sync.WaitGroup, cst *st.Ccstat, nst *st.NodeStat) {
	tKPoollck := &sync.Mutex{}
	for {
		if *isStop {
			break
		}
		gtkQ <- struct{}{}
		go gettoken(hst, ab, gtkQ, tkPool, wg, cst, nst, tKPoollck)
	}
}