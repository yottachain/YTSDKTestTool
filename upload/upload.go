package main

import (
	log "github.com/sirupsen/logrus"
	hi "github.com/yottachain/YTHost/interface"
	f "github.com/yottachain/YTSDKTestTool/file"
	tk "github.com/yottachain/YTSDKTestTool/token"
	cm "github.com/yottachain/YTStTool/ClientManage"
	st "github.com/yottachain/YTStTool/stat"
	"sync"
	"time"
)

type uploads struct {
	fQueue		chan struct{}
	blkQueue	chan struct{}
	shardQueue	chan struct{}
	gtkQueue 	chan struct{}
	tkPool		chan *tk.IdToToken
	files 		[] *f.File
	upSucfiles	int
	shardSucs   uint
	fsize  		uint
	dataSrcName string
	totalfs		uint
}

func NewUploads(files [] *f.File, fcc uint, bcc uint, scc uint, gtcc uint, fsize uint, shardSucs uint, dataSrcName string, totalfs uint) *uploads {
	return &uploads{
		make(chan struct{}, fcc),
		make(chan struct{}, bcc),
		make(chan struct{}, scc),
		make(chan struct{}, gtcc),
		make(chan *tk.IdToToken, gtcc),
		files,
		0,
		shardSucs,
		fsize,
		dataSrcName,
		totalfs,
	}
}

func (ups *uploads) FileUpload(hst hi.Host, ab *cm.AddrsBook, wg *sync.WaitGroup, cst *st.Ccstat,
				nst *st.NodeStat, nodeshs int, openTkPool bool) time.Duration {
	startTime := time.Now()
	for {
		for _, v := range ups.files {
			if v.IsUnuse() {
				ups.fQueue <- struct{}{}
				go v.BlockUpload(hst, ab, ups.fQueue, ups.blkQueue, ups.shardQueue, ups.tkPool,
					int(ups.shardSucs), wg, cst, nst, nodeshs, openTkPool)
			}
		}

		//TODO Whether all files have been uploaded
		<- time.After(100*time.Millisecond)
		for _, v := range ups.files {
			if v.IsUpFinish() {
				//v.FilePrintInfo()
				v.SetInit()
				ups.FileAddSuc(1)
				if ups.upSucfiles >= int(ups.totalfs) {
					break
				}
				//v.SetUnuse()
				//v.AppendFiles(int(ups.fsize), ups.dataSrcName)
				//err := ups.AppendFile(int(ups.fsize), ups.dataSrcName)
				//if err != nil {
				//	log.Fatalf("ups append file fail, error=%s", err.Error())
				//}
			}
		}

		log.WithFields(log.Fields{
			"upSucfiles":ups.upSucfiles,
		}).Info("upload Suc files")

		if ups.upSucfiles >= int(ups.totalfs) {
			break
		}
	}
	return time.Now().Sub(startTime)
}

func (ups *uploads) FileAddSuc(n int) {
	ups.upSucfiles = ups.upSucfiles + n
}

func (ups *uploads) AppendFile(fs int, datasrcName string) error {
	file := &f.File{}
	err := file.FileInit(fs, datasrcName)
	if err != nil {
		return err
	}

	ups.files = append(ups.files, file)

	return nil
}