package main

import (
	log "github.com/sirupsen/logrus"
	hi "github.com/yottachain/YTHost/hostInterface"
	f "github.com/yottachain/YTSDKTestTool/file"
	cm "github.com/yottachain/YTStTool/ClientManage"
	"sync"
	"time"
)

type uploads struct {
	blkQueue	chan struct{}
	shardQueue	chan struct{}
	files 		[] *f.File
	upSucfiles	int
	shardSucs   uint
	fsize  		uint
	dataSrcName string
	totalfs		uint
}

func NewUploads(files [] *f.File, bcc uint, scc uint, fsize uint, shardSucs uint, dataSrcName string, totalfs uint) *uploads {
	return &uploads{
		make(chan struct{}, bcc),
		make(chan struct{}, scc),
		files,
		0,
		shardSucs,
		fsize,
		dataSrcName,
		totalfs,
	}
}

func (ups *uploads) FileUpload(hst hi.Host, ab *cm.AddrsBook, wg *sync.WaitGroup) {
	for {
		for _, v := range ups.files {
			//v.FilePrintInfo()
			if v.IsUnuse() {
				go v.BlockUpload(hst, ab, ups.blkQueue, ups.shardQueue, int(ups.shardSucs), wg)
			}
		}

		//TODO Whether all files have been uploaded
		<- time.After(100*time.Millisecond)
		for _, v := range ups.files {
			if v.IsUpFinish() {
				v.FilePrintInfo()
				ups.FileAddSuc(1)
				v.SetUnuse()
				if ups.upSucfiles >= int(ups.totalfs) {
					break
				}
				v.AppendFiles(int(ups.fsize), ups.dataSrcName)
			}
		}

		log.WithFields(log.Fields{
			"upSucfiles":ups.upSucfiles,
		}).Info("-------------")

		if ups.upSucfiles >= int(ups.totalfs) {
			break
		}
	}
}

func (ups *uploads) FileAddSuc(n int) {
	ups.upSucfiles = ups.upSucfiles + n
}