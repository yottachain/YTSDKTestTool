package file

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	log "github.com/sirupsen/logrus"
	"github.com/yottachain/YTDataNode/message"
	"github.com/yottachain/YTHost/client"
	hi "github.com/yottachain/YTHost/interface"
	"github.com/yottachain/YTSDKTestTool/conn"
	"github.com/yottachain/YTSDKTestTool/stat"
	tk "github.com/yottachain/YTSDKTestTool/token"
	cm "github.com/yottachain/YTStTool/ClientManage"
	st "github.com/yottachain/YTStTool/stat"
	"math/rand"
	"sync"
	"time"
)

type shard struct {
	sNum  	int
	sh		[] byte
	vhf 	[] byte
	bs58Vhf string
	upstatus shardUpstatus
	blk 	*block
}

type shardUpstatus int

const (
	SUNUPLOAD shardUpstatus = iota
	SUPLOADING
	SUPLOADED
)

func (sh *shard) Upload(hst hi.Host, ab *cm.AddrsBook, shdQ chan struct{},
	tkpool chan *tk.IdToToken, fName string, blkNum int,
	wg *sync.WaitGroup, cst *st.Ccstat, nst *st.NodeStat, connNowait bool) {
	sh.SetUploading()
	wg.Add(1)
	defer func() {
		wg.Done()
	}()

startup:
	token := <- tkpool
	cst.ConsumeTkAdd()
	nId := token.GetPid()
	addrs := token.GetAddrs()

	var clt *client.YTHostClient
	if connNowait {
		clt = hst.ClientStore().GetUsePid(nId)
		if clt == nil {
			go conn.Connect(hst, nId, addrs)
			nst.ConnErrAdd(nId)
			cst.ConsumeTkSub()
			goto startup
		}
	}else {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
		clt, err := hst.ClientStore().Get(ctx, nId, addrs)
		if clt == nil || err != nil {
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
			}).Error("upload shard connect error=", err)

			nst.ConnErrAdd(nId)
			cst.ConsumeTkSub()
			cancel()

			goto startup
		}
	}

	nst.ConnSuccAdd(nId)

	var uploadReqMsg message.UploadShardRequestTest

	uploadReqMsg.AllocId = token.GetToken().AllocId
	uploadReqMsg.VHF = sh.vhf
	uploadReqMsg.DAT = sh.sh
	uploadReqMsg.Sleep = 100

	uploadReqData, err := proto.Marshal(&uploadReqMsg)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"miner": peer.Encode(nId),
		}).Error("upload shard request proto marshal error")
		cst.ConsumeTkSub()
		goto startup
	}

	cst.SendccAdd()
	cst.IdccAdd(nId)

	ssTime := time.Now()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
	//defer cancel2()
	res, err := clt.SendMsg(ctx2, message.MsgIDSleepReturn.Value(), uploadReqData)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
			"err":    err,
		}).Error("message send error")

		nst.SendDelay(nId, time.Now().Sub(ssTime))
		nst.SendErrAdd(nId)
		cst.SendccSub()
		cst.ConsumeTkSub()
		cst.IdccSub(nId)
		cancel2()
		goto startup
	}

	//nst.SendSuccAdd(nId)
	nst.SendDelay(nId, time.Now().Sub(ssTime))
	cst.SendccSub()
	cst.ConsumeTkSub()
	cst.IdccSub(nId)

	var resmsg message.UploadShardResponse
	err = proto.Unmarshal(res[2:], &resmsg)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
			"err": err,
		}).Error("msg response proto Unmarshal error")

		goto startup
	}

	nst.SendSuccAdd(nId)

	log.WithFields(log.Fields{
		"filename": fName,
		"block": blkNum,
		"shard": sh.sNum,
		"VHF": sh.bs58Vhf,
		"miner": peer.Encode(nId),
	}).Info("shard uploaded")

	sh.SetUploaded()
	<-shdQ
}

func (sh *shard) IsNidAvailable(nId peer.ID, nodeshs int) bool {
	sh.blk.Lock()
	defer sh.blk.Unlock()
	_, ok := sh.blk.nodeShards[nId]
	if ok {
		if sh.blk.nodeShards[nId] >= nodeshs {
			return false
		}
		sh.blk.nodeShards[nId] = sh.blk.nodeShards[nId] + 1
	}else {
		sh.blk.nodeShards[nId] = 1
	}
	return true
}

func (sh *shard) nodeShardsSub(nId peer.ID) {
	sh.blk.Lock()
	defer sh.blk.Unlock()
	_, ok := sh.blk.nodeShards[nId]
	if ok {
		if sh.blk.nodeShards[nId] <= 0 {
			return
		}
		sh.blk.nodeShards[nId] = sh.blk.nodeShards[nId] - 1
	}
}

func (sh *shard) UploadBK(hst hi.Host, ab *cm.AddrsBook, shdQ chan struct{}, fName string, blkNum int,
			wg *sync.WaitGroup, cst *st.Ccstat, nst *st.NodeStat, nodeshs int, dst *stat.DelayStat, connNowait bool) {
	sh.SetUploading()
	cst.ShccAdd()
	wg.Add(1)
	defer func() {
		cst.ShccSub()
		wg.Done()
	}()

	getNstartT := time.Now()
startup:
	getNIdstartT := time.Now()
	abLen := ab.GetWeightsLen()
	if abLen == 0 {
		log.WithFields(log.Fields{
			"len": 0,
		}).Info("addrbook len error")
		goto startup
	}
	var r = rand.New(rand.NewSource(time.Now().UnixNano()))
	idx := r.Intn(abLen)
	nId := ab.GetWeightId(idx)
	nst.SetWeight(nId, ab.GetIdWeight(nId))

	dst.CalcDly(stat.GETNODEIDDEALY, time.Now().Sub(getNIdstartT))

	getNastartT := time.Now()
	if !sh.IsNidAvailable(nId, nodeshs) {
		dst.CalcDly(stat.GETAVAILABLEDEALY, time.Now().Sub(getNastartT))
		goto startup
	}
	dst.CalcDly(stat.GETAVAILABLEDEALY, time.Now().Sub(getNastartT))
	dst.CalcDly(stat.GETNODEDELAY, time.Now().Sub(getNstartT))

	addrs, ok := ab.Get(nId)
	if !ok {
		log.WithFields(log.Fields{
			"peer id": nId,
		}).Error("get addr error")

		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		goto startup
	}

	var clt *client.YTHostClient
	if connNowait {
		clt = hst.ClientStore().GetUsePid(nId)
		if clt == nil {
			go conn.Connect(hst, nId, addrs)

			nst.ConnErrAdd(nId)
			sh.nodeShardsSub(nId)
			getNstartT = time.Now()		//这个时间要重置
			goto startup
		}
	}else {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
		clt, err := hst.ClientStore().Get(ctx, nId, addrs)
		if clt == nil || err != nil {
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
			}).Error(err)

			nst.ConnErrAdd(nId)
			cancel()

			sh.nodeShardsSub(nId)
			getNstartT = time.Now() //这个时间要重置

			goto startup
		}
	}

	nst.ConnSuccAdd(nId)

	var getToken message.NodeCapacityRequest
	getTokenData, err := proto.Marshal(&getToken)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("request proto marshal error")

		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		goto startup
	}

	cst.GtccAdd()
	cst.GtsAdd()

	ssTime := time.Now()
	ctx1, cal := context.WithTimeout(context.Background(), time.Second*1)
	res, err := clt.SendMsg(ctx1, message.MsgIDNodeCapacityRequest.Value(), getTokenData)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
		}).Error("get token fail")

		cal()
		cst.GtccSub()
		nst.GtDelay(nId, time.Now().Sub(ssTime))
		nst.GtErrAdd(nId)

		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		goto startup
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
		cst.GtccSub()
		nst.GtErrAdd(nId)

		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		goto startup
	}

	if !resGetToken.Writable  {
		cst.GtccSub()
		nst.GtErrAdd(nId)

		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		goto startup
	}else {
		log.WithFields(log.Fields{
			"矿机": peer.Encode(nId),
		}).Info("获取token成功")
	}

	nst.GtSuccAdd(nId)
	cst.GtccSub()
	cst.GtSucsAdd()

	var uploadReqMsg message.UploadShardRequestTest

	uploadReqMsg.AllocId = resGetToken.AllocId
	uploadReqMsg.VHF = sh.vhf
	uploadReqMsg.DAT = sh.sh
	uploadReqMsg.Sleep = 100

	uploadReqData, err := proto.Marshal(&uploadReqMsg)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"miner": peer.Encode(nId),
		}).Error("upload request proto marshal error")

		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		goto startup
	}

	log.WithFields(log.Fields{
		"filename": fName,
		"block": blkNum,
		"shard": sh.sNum,
		"VHF": sh.bs58Vhf,
		"miner": peer.Encode(nId),
	}).Info("uploadbk shard uploading")

	cst.SendccAdd()
	cst.IdccAdd(nId)

	ssTime = time.Now()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
	defer cancel2()
	res, err = clt.SendMsg(ctx2, message.MsgIDSleepReturn.Value(), uploadReqData)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
			"err":    err,
		}).Error("message send error")

		nst.SendDelay(nId, time.Now().Sub(ssTime))
		nst.SendErrAdd(nId)
		cst.SendccSub()
		cst.IdccSub(nId)

		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		goto startup
	}

	nst.SendDelay(nId, time.Now().Sub(ssTime))
	cst.SendccSub()
	cst.IdccSub(nId)
	nst.SendSuccAdd(nId)

	var resmsg message.UploadShardResponse
	err = proto.Unmarshal(res[2:], &resmsg)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
			"err": err,
		}).Error("msg response proto Unmarshal error")

		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		goto startup
	}

	log.WithFields(log.Fields{
		"filename": fName,
		"block": blkNum,
		"shard": sh.sNum,
		"VHF": sh.bs58Vhf,
		"miner": peer.Encode(nId),
	}).Info("shard uploaded")

	sh.SetUploaded()
	<-shdQ
}

func (sh *shard) SetUploaded() {
	sh.upstatus = SUPLOADED
}

func (sh *shard) SetUploading() {
	sh.upstatus = SUPLOADING
}

func (sh *shard) IsUnUpload() bool {
	return sh.upstatus == SUNUPLOAD
}

func (sh *shard) IsUploaded() bool {
	return sh.upstatus == SUPLOADED
}