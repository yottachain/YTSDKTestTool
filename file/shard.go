package file

import (
	"context"
	"crypto/md5"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/mr-tron/base58"
	log "github.com/sirupsen/logrus"
	"github.com/yottachain/YTDataNode/message"
	hi "github.com/yottachain/YTHost/hostInterface"
	cm "github.com/yottachain/YTStTool/ClientManage"
	"math/rand"
	"sync"
	"time"
)

type shard struct {
	sNum  	int
	sh		[] byte
	upstatus shardUpstatus
}

type shardUpstatus int
var r = rand.New(rand.NewSource(time.Now().UnixNano()))

const (
	SUNUPLOAD shardUpstatus = iota
	SUPLOADING
	SUPLOADED
)

func (sh *shard) Upload(hst hi.Host, ab *cm.AddrsBook, shdQ chan struct{}, fName string, blkNum int, wg *sync.WaitGroup) {
	sh.SetUploading()
	wg.Add(1)
	defer func() {
		wg.Done()
	}()

	startup:
	abLen := ab.GetWeightsLen()
	if abLen == 0 {
		log.WithFields(log.Fields{
			"len": 0,
		}).Info("addrbook len error")
		goto startup
	}
	idx := r.Intn(abLen)
	//log.WithFields(log.Fields{"lenth": abLen, "index": idx,}).Info("addrs list lenth")
	nId := ab.GetWeightId(idx)
	addrs, ok := ab.Get(nId)
	if !ok {
		log.WithFields(log.Fields{
			"peer id": nId,
		}).Error("get addr error")
		goto startup
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
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
		}).Error(err)

		cancel()
		goto startup
	}

	var getToken message.NodeCapacityRequest
	getTokenData, err := proto.Marshal(&getToken)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("request proto marshal error")
		goto startup
	}

	ctx1, cal := context.WithTimeout(context.Background(), time.Second*1)
	res, err := clt.SendMsg(ctx1, message.MsgIDNodeCapacityRequest.Value(), getTokenData)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
		}).Error("get token fail")

		cal()
		goto startup
	}

	var resGetToken message.NodeCapacityResponse
	err = proto.Unmarshal(res[2:], &resGetToken)
	if err != nil {
		log.WithFields(log.Fields{
		"nodeid": peer.Encode(nId),
		"err": err,
		}).Error("get token response proto Unmarshal error")
		goto startup
	}

	if !resGetToken.Writable  {
		goto startup
	}else {
		log.WithFields(log.Fields{
			"矿机": peer.Encode(nId),
		}).Info("获取token成功")
	}

	m5 := md5.New()
	m5.Reset()
	m5.Write(sh.sh)
	var vhf = m5.Sum(nil)
	var b58vhf = base58.Encode(vhf)
	var uploadReqMsg message.UploadShardRequestTest

	uploadReqMsg.AllocId = resGetToken.AllocId
	uploadReqMsg.VHF = vhf
	uploadReqMsg.DAT = sh.sh
	uploadReqMsg.Sleep = 100

	uploadReqData, err := proto.Marshal(&uploadReqMsg)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"miner": peer.Encode(nId),
		}).Error("upload request proto marshal error")
		goto startup
	}

	log.WithFields(log.Fields{
		"filename": fName,
		"block": blkNum,
		"shard": sh.sNum,
		"VHF": b58vhf,
		"miner": peer.Encode(nId),
	}).Info("shard uploading")

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
	defer cancel2()
	res, err = clt.SendMsg(ctx2, message.MsgIDSleepReturn.Value(), uploadReqData)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
			"err":    err,
		}).Error("message send error")

		goto startup
	}

	var resmsg message.UploadShardResponse
	err = proto.Unmarshal(res[2:], &resmsg)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(nId),
			"err": err,
		}).Error("msg response proto Unmarshal error")

		goto startup
	}

	log.WithFields(log.Fields{
		"filename": fName,
		"block": blkNum,
		"shard": sh.sNum,
		"VHF": b58vhf,
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