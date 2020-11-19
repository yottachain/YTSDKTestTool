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
	wg *sync.WaitGroup, cst *st.Ccstat, nst *st.NodeStat, connNowait bool, dst *stat.DelayStat) {
	sh.SetUploading()
	cst.ShccAdd()
	wg.Add(1)
	defer func() {
		wg.Done()
	}()

startup:
	cst.GnccAdd()
	gtNodeTime := time.Now()
	token := <- tkpool
	dst.CalcDly(stat.GETTOKENDELAY, time.Now().Sub(gtNodeTime))
	cst.ConsumeTkAdd()
	nId := token.GetPid()
	addrs := token.GetAddrs()
	dst.CalcDly(stat.GETNODEDELAY, time.Now().Sub(gtNodeTime))

	cst.CccAdd()
	cst.GnccSub()

	var clt *client.YTHostClient
	if connNowait {
		clt = hst.ClientStore().GetUsePid(nId)
		if clt == nil {
			go conn.Connect(hst, nId, addrs)
			nst.ConnErrAdd(nId)
			cst.ConsumeTkSub()

			cst.CccSub()
			goto startup
		}
	}else {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
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
			}).Error("upload shard connect error=", err)

			nst.ConnErrAdd(nId)
			cst.ConsumeTkSub()
			cst.CccSub()
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
		cst.CccSub()
		goto startup
	}

	cst.SendccAdd()
	cst.CccSub()
	cst.IdccAdd(nId)

	ssTime := time.Now()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
	res, err := clt.SendMsg(ctx2, message.MsgIDSleepReturn.Value(), uploadReqData)
	if err != nil {
		//log.WithFields(log.Fields{
		//	"nodeid": peer.Encode(nId),
		//	"err":    err,
		//}).Error("message send error")

		nst.SendDelay(nId, time.Now().Sub(ssTime))
		nst.SendErrAdd(nId)
		cst.ConsumeTkSub()
		cst.IdccSub(nId)
		cancel2()

		cst.SendccSub()
		goto startup
	}

	nst.SendDelay(nId, time.Now().Sub(ssTime))
	cst.ConsumeTkSub()
	cst.IdccSub(nId)

	var resmsg message.UploadShardResponse
	err = proto.Unmarshal(res[2:], &resmsg)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
			"err": err,
		}).Error("msg response proto Unmarshal error")
		cst.SendccSub()
		goto startup
	}

	nst.SendSuccAdd(nId)
	cst.SendccSub()

	//log.WithFields(log.Fields{
	//	"filename": fName,
	//	"block": blkNum,
	//	"shard": sh.sNum,
	//	"VHF": sh.bs58Vhf,
	//	"miner": peer.Encode(nId),
	//}).Info("shard uploaded")

	sh.SetUploaded()
	cst.ShccSub()
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
			wg *sync.WaitGroup, cst *st.Ccstat, nst *st.NodeStat, nodeshs int, dst *stat.DelayStat, connNowait bool,
			NodeSequence bool) {
	sh.SetUploading()
	cst.ShccAdd()
	wg.Add(1)
	defer func() {
		wg.Done()
	}()

	getNstartT := time.Now()
startup:
	cst.GnccAdd()

	getNIdstartT := time.Now()
	abLen := ab.GetWeightsLen()
	if abLen == 0 {
		log.WithFields(log.Fields{
			"len": 0,
		}).Info("addrbook len error")

		cst.GnccSub()
		goto startup
	}

	var nId peer.ID
	if NodeSequence {
		idx := sh.blk.NodeIdxPlus(abLen)
		nId = ab.GetWeightId(idx)
		//nst.SetWeight(nId, ab.GetIdWeight(nId))
	}else {
		var r = rand.New(rand.NewSource(time.Now().UnixNano()))
		idx := r.Intn(abLen)
		nId = ab.GetWeightId(idx)
		//nst.SetWeight(nId, ab.GetIdWeight(nId))
		dst.CalcDly(stat.GETNODEIDDEALY, time.Now().Sub(getNIdstartT))
	}

	getNastartT := time.Now()
	if !sh.IsNidAvailable(nId, nodeshs) {
		dst.CalcDly(stat.GETAVAILABLEDEALY, time.Now().Sub(getNastartT))

		cst.GnccSub()
		goto startup
	}
	dst.CalcDly(stat.GETAVAILABLEDEALY, time.Now().Sub(getNastartT))
	dst.CalcDly(stat.GETNODEDELAY, time.Now().Sub(getNstartT))

	cst.GnccSub()
	cst.CccAdd()

	var clt *client.YTHostClient
	if connNowait {
		clt = hst.ClientStore().GetUsePid(nId)
		if clt == nil {
			addrs, ok := ab.Get(nId)
			if !ok {
				log.WithFields(log.Fields{
					"peer id": nId,
				}).Error("get addr error")

				sh.nodeShardsSub(nId)
				getNstartT = time.Now()		//这个时间要重置

				cst.GnccSub()
				goto startup
			}
			go conn.Connect(hst, nId, addrs)

			nst.ConnErrAdd(nId)
			sh.nodeShardsSub(nId)
			getNstartT = time.Now()		//这个时间要重置

			cst.CccSub()
			goto startup
		}
	}else {
		addrs, ok := ab.Get(nId)
		if !ok {
			log.WithFields(log.Fields{
				"peer id": nId,
			}).Error("get addr error")

			sh.nodeShardsSub(nId)
			getNstartT = time.Now()		//这个时间要重置

			cst.GnccSub()
			goto startup
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
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
			//log.WithFields(log.Fields{
			//	"nodeid": peer.Encode(nId),
			//	"addrs":  sAddrs,
			//}).Error(err)

			nst.ConnErrAdd(nId)
			cancel()

			sh.nodeShardsSub(nId)
			getNstartT = time.Now() //这个时间要重置

			cst.CccSub()
			goto startup
		}
	}

	nst.ConnSuccAdd(nId)
	//cst.OtrccAdd()
	//cst.CccSub()
	cst.CccSub()
	cst.OtrccAdd()

	gtReqTime := time.Now()
	var getToken message.NodeCapacityRequest
	getTokenData, err := proto.Marshal(&getToken)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("request proto marshal error")

		dst.CalcDly(stat.PROTOGTREQDEALY, time.Now().Sub(gtReqTime))
		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		cst.OtrccSub()
		goto startup
	}

	dst.CalcDly(stat.PROTOGTREQDEALY, time.Now().Sub(gtReqTime))
	cst.GtsAdd()

	//cst.GtccAdd()
	//cst.OtrccSub()
	cst.OtrccSub()
	cst.GtccAdd()

	ssTime := time.Now()
	ctx1, cal := context.WithTimeout(context.Background(), time.Second*1)
	res, err := clt.SendMsg(ctx1, message.MsgIDNodeCapacityRequest.Value(), getTokenData)
	if err != nil {
		//gtFtime := time.Now()
		//log.WithFields(log.Fields{
		//	"nodeid": peer.Encode(nId),
		//}).Error("get token fail")
		//dst.CalcDly(stat.LOGDELAY, time.Now().Sub(gtFtime))

		cal()

		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		nst.GtDelay(nId, time.Now().Sub(ssTime))
		nst.GtErrAdd(nId)

		cst.GtccSub()
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

		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		nst.GtErrAdd(nId)

		cst.GtccSub()
		goto startup
	}

	if !resGetToken.Writable  {
		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		nst.GtErrAdd(nId)

		cst.GtccSub()
		goto startup
	}else {
		gtsucTime := time.Now()
		log.WithFields(log.Fields{
			"矿机": peer.Encode(nId),
		}).Info("获取token成功")
		dst.CalcDly(stat.LOGDELAY, time.Now().Sub(gtsucTime))
	}

	nst.GtSuccAdd(nId)
	cst.GtSucsAdd()

	//cst.OtrccAdd()
	//cst.GtccSub()
	cst.GtccSub()
	cst.OtrccAdd()

	sdReqTime := time.Now()
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

		dst.CalcDly(stat.PROTOSDREQDEALY, time.Now().Sub(sdReqTime))
		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		cst.OtrccSub()
		goto startup
	}

	dst.CalcDly(stat.PROTOSDREQDEALY, time.Now().Sub(sdReqTime))

	//log.WithFields(log.Fields{
	//	"filename": fName,
	//	"block": blkNum,
	//	"shard": sh.sNum,
	//	"VHF": sh.bs58Vhf,
	//	"miner": peer.Encode(nId),
	//}).Info("uploadbk shard uploading")

	cst.IdccAdd(nId)

	//cst.SendccAdd()
	//cst.OtrccSub()
	cst.OtrccSub()
	cst.SendccAdd()

	ssTime = time.Now()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
	defer cancel2()
	res, err = clt.SendMsg(ctx2, message.MsgIDSleepReturn.Value(), uploadReqData)
	if err != nil {
		sdTime := time.Now()
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
			"err":    err,
		}).Error("message send error")
		dst.CalcDly(stat.LOGDELAY, time.Now().Sub(sdTime))

		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		nst.SendDelay(nId, time.Now().Sub(ssTime))
		nst.SendErrAdd(nId)

		cst.IdccSub(nId)
		cst.SendccSub()
		goto startup
	}

	nst.SendDelay(nId, time.Now().Sub(ssTime))
	cst.IdccSub(nId)
	nst.SendSuccAdd(nId)

	//cst.OtrccAdd()
	//cst.SendccSub()
	cst.SendccSub()
	cst.OtrccAdd()

	sdRespTime := time.Now()
	var resmsg message.UploadShardResponse
	err = proto.Unmarshal(res[2:], &resmsg)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
			"err": err,
		}).Error("msg response proto Unmarshal error")

		dst.CalcDly(stat.PROTOSDRSPDEALY, time.Now().Sub(sdRespTime))
		sh.nodeShardsSub(nId)
		getNstartT = time.Now()		//这个时间要重置

		cst.OtrccSub()
		goto startup
	}

	dst.CalcDly(stat.PROTOSDRSPDEALY, time.Now().Sub(sdRespTime))

	//logTime := time.Now()
	//log.WithFields(log.Fields{
	//	"filename": fName,
	//	"block": blkNum,
	//	"shard": sh.sNum,
	//	"VHF": sh.bs58Vhf,
	//	"miner": peer.Encode(nId),
	//}).Info("shard uploaded")
	//dst.CalcDly(stat.LOGDELAY, time.Now().Sub(logTime))

	sh.nodeShardsSub(nId)
	cst.OtrccSub()

	sh.SetUploaded()
	cst.ShccSub()
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