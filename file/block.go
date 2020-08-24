package file

import (
	log "github.com/sirupsen/logrus"
	hi "github.com/yottachain/YTHost/hostInterface"
	cm "github.com/yottachain/YTStTool/ClientManage"
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
		blkSucShards int, fName string, wg *sync.WaitGroup) {
	blk.SetUploading()
	log.WithFields(log.Fields{
		"fileName": fName,
		"block": blk.bNum,
	}).Info("block uploading")

	for _, v := range blk.shards {
		if v.IsUnUpload() {
			shdQ <- struct{}{}
			go v.Upload(hst, ab, shdQ, fName, blk.bNum, wg)
		}
	}

	for {
		<- time.After(100*time.Millisecond)
		blk.CheckUploaded(blkSucShards)
		if blk.IsUploaded() {
			<- blkQ
			break
		}
	}
}

func (blk *block) CheckUploaded(blkSucShards int) {
	blk.shardSucs = 0
	for _, v := range blk.shards {
		if v.IsUploaded() {
			blk.shardSucs++
		}
	}

	if blk.shardSucs >= blkSucShards {
		blk.SetUploaded()
	}
}