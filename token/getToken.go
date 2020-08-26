package token

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	log "github.com/sirupsen/logrus"
	"github.com/yottachain/YTDataNode/message"
	hi "github.com/yottachain/YTHost/hostInterface"
	cm "github.com/yottachain/YTStTool/ClientManage"
	st "github.com/yottachain/YTStTool/stat"
	"math/rand"
	"sync"
	"time"
)

type IdToToken struct {
	pid peer.ID
	tk *message.NodeCapacityResponse
}

func (idt *IdToToken) GetPid () peer.ID{
	return idt.pid
}

func (idt *IdToToken) GetToken () *message.NodeCapacityResponse{
	return idt.tk
}

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func gettoken (hst hi.Host, ab *cm.AddrsBook, gtkQ chan struct{}, tkpool chan *IdToToken,
			wg *sync.WaitGroup, cst *st.Ccstat) {
	defer func() {
		<- gtkQ
		wg.Done()
	}()
	wg.Add(1)


	abLen := ab.GetWeightsLen()
	if abLen == 0 {
		log.WithFields(log.Fields{
			"len": 0,
		}).Info("addrbook len error")
		return
	}
	idx := r.Intn(abLen)
	nId := ab.GetWeightId(idx)
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

		//cst.GtccSub()
		return
	}

	var getToken message.NodeCapacityRequest
	getTokenData, err := proto.Marshal(&getToken)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("request proto marshal error")

		//cst.GtccSub()
		return
	}

	cst.GtccAdd()

	ctx1, cal := context.WithTimeout(context.Background(), time.Second*1)
	defer cal()
	res, err := clt.SendMsg(ctx1, message.MsgIDNodeCapacityRequest.Value(), getTokenData)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
		}).Error("get token fail")

		cst.GtccSub()
		return
	}

	var resGetToken message.NodeCapacityResponse
	err = proto.Unmarshal(res[2:], &resGetToken)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
			"err": err,
		}).Error("get token response proto Unmarshal error")

		cst.GtccSub()
		return
	}

	if !resGetToken.Writable  {
		cst.GtccSub()
		return
	}else {
		log.WithFields(log.Fields{
			"矿机": peer.Encode(nId),
		}).Info("获取token成功")
	}

	cst.GtccSub()
	tkpool <- &IdToToken{nId, &resGetToken}
	cst.SetTkPoolLen(len(tkpool))

	return
}


func GetTkToPool(hst hi.Host, ab *cm.AddrsBook, gtkQ chan struct{},
				tkPool chan *IdToToken, isStop *bool, wg *sync.WaitGroup, cst *st.Ccstat) {
	for {
		gtkQ <- struct{}{}
		if *isStop {
			break
		}
		go gettoken(hst, ab, gtkQ, tkPool, wg, cst)
	}
}