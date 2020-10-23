package file

import (
	"github.com/libp2p/go-libp2p-core/peer"
	log "github.com/sirupsen/logrus"
	hi "github.com/yottachain/YTHost/interface"
	"github.com/yottachain/YTSDKTestTool/stat"
	tk "github.com/yottachain/YTSDKTestTool/token"
	cm "github.com/yottachain/YTStTool/ClientManage"
	st "github.com/yottachain/YTStTool/stat"
	"sync"
	"time"
)

type blkUpstatus int

const (
	BUNUPLOAD blkUpstatus = iota
	BUPLOADING
	BUPLOADED
)

type block struct {
	bNum	int
	shards  [] *shard
	upStatus blkUpstatus
	shardSucs int	//上传分片成功的数量
	nodeShards map[peer.ID] int
	sync.Mutex
}

func (blk *block) GetUploadStatus() blkUpstatus {
	return blk.upStatus
}

func (blk *block) IsUnupload() bool {
	return blk.upStatus == BUNUPLOAD
}

func (blk *block) SetUploading () {
	blk.upStatus = BUPLOADING
}

func (blk *block) SetUploaded () {
	blk.upStatus = BUPLOADED
}

func (blk *block) IsUploaded() bool {
	return blk.upStatus == BUPLOADED
}

func (blk *block) ShardUpload(hst hi.Host, ab *cm.AddrsBook, blkQ chan struct{}, shdQ chan struct{},
	tkpool chan *tk.IdToToken, blkSucShards int, fName string, wg *sync.WaitGroup,
	cst *st.Ccstat, nst *st.NodeStat, nodeshs int, openTkPool bool, dst *stat.DelayStat) {
	blk.SetUploading()
	log.WithFields(log.Fields{
		"fileName": fName,
		"block": blk.bNum,
	}).Info("block uploading")

	for _, v := range blk.shards {
		if v.IsUnUpload() {
			shdQ <- struct{}{}
			if openTkPool {
				go v.Upload(hst, ab, shdQ, tkpool, fName, blk.bNum, wg, cst, nst)
			}else {
				go v.UploadBK(hst, ab, shdQ, fName, blk.bNum, wg, cst, nst, nodeshs, dst)
			}

		}
	}

	for {
		<- time.After(100*time.Millisecond)
		blk.CheckUploaded(blkSucShards, fName)
		if blk.IsUploaded() {
			<- blkQ
			break
		}
	}
}

func (blk *block) CheckUploaded(blkSucShards int, fn string) {
	blk.shardSucs = 0
	for _, v := range blk.shards {
		if v.IsUploaded() {
			blk.shardSucs++
		}
	}

	if blk.shardSucs >= blkSucShards {
		blk.SetUploaded()
		log.WithFields(log.Fields{
			"fileName": fn,
			"blks": blk.bNum,
			"shards":blk.shardSucs,
		}).Info("file block upload success")
	}
}