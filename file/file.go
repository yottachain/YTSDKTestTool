package file

import (
	"crypto/md5"
	"fmt"
	"github.com/mr-tron/base58"
	log "github.com/sirupsen/logrus"
	hi "github.com/yottachain/YTHost/hostInterface"
	"github.com/yottachain/YTSDKTestTool/rand"
	tk "github.com/yottachain/YTSDKTestTool/token"
	cm "github.com/yottachain/YTStTool/ClientManage"
	st "github.com/yottachain/YTStTool/stat"
	"os"
	"sync"
	"time"
)

type File struct {
	fd 			*os.File
	fileName 	string
	filesize	int
	upStatus	UploadStatus
	blocks   	[] *block
	blkSucs		int
}

type UploadStatus int

const (
	UPINIT UploadStatus = iota
	UPUNUSE
	UPUSED
	UPFINISH
)


var remainfs uint32 = 0
var m5 = md5.New()

func NewFileMage(fcc uint, fn uint, fs int, datasrcName string) ([] *File, error) {
	files := make([] *File, fcc)
	for i := uint(0); i < fcc; i++ {
		log.Printf("file_%d init\n", i)
		files[i] = &File{}
		err := files[i].FileInit(fs, datasrcName)
		if err != nil {
			return nil, err
		}
	}

	remainfs = uint32(fn - fcc)

	//go AppendFiles(&files, fs, datasrcName)

	return files, nil
}

func (f *File) AppendFiles(fs int, datasrcName string) {
	f.FileReset()
	err := f.FileInit(fs, datasrcName)
	if err != nil {
		log.WithFields(log.Fields{
			"fileNumber": remainfs,
		}).Info("file init fail")
		return
	}
}

func (f *File) openDataSource(filename string) error {
	dataReader, err := os.OpenFile(filename, os.O_RDONLY, 0644)

	if err == nil {
		f.fd = dataReader
		return err
	}else {
		f.fd = nil
		return nil
	}
}

func (f *File) closeDataSource() {
	if f.fd != nil {
		f.fd.Close()
		f.fd = nil
	}
}

func (f *File) FileInit(fs int, datasrcName string) error {
	err := f.openDataSource(datasrcName)
	if err != nil {
		return err
	}
	f.fileName = fmt.Sprintf("%s_%dM.txt", rand.RandString(15), fs)
	f.filesize = fs
	var bls int
	if fs % 2 == 0 {
		bls = fs/2
	}else {
		bls = fs/2 + 1
	}

	f.blocks = make([] *block, bls)
	for j := 0; j < bls; j++ {
		f.blocks[j] = &block{j, make([] *shard, 160), BUNUPLOAD, 0}
		for k := 0; k < 160; k++ {
			var data = make([]byte, 16*1024)
			f.fd.Read(data)
			m5.Reset()
			m5.Write(data)
			var vhf = m5.Sum(nil)
			var b58vhf = base58.Encode(vhf)
			f.blocks[j].shards[k] = &shard{k, data, vhf, b58vhf, SUNUPLOAD}
		}
	}
	f.upStatus = UPUNUSE

	return nil
}

func (f *File) FileReset() {
	f.closeDataSource()
	f.fileName = ""
	f.blocks = nil
	f.upStatus = UPINIT
	f.filesize = 0
}

func (f *File) IsUnuse() bool {
	return f.upStatus == UPUNUSE
}

func (f *File) SetUnuse() {
	f.upStatus = UPUNUSE
}

func (f *File) SetUsed() {
	f.upStatus = UPUSED
}

func (f *File) SetUpFinished() {
	f.upStatus = UPFINISH
}

func (f *File) IsUpFinish() bool {
	return f.upStatus == UPFINISH
}


func (f *File) FilePrintInfo() {
	log.WithFields(log.Fields{
		"filename":f.fileName,
		"blocks":len(f.blocks),
		"upstatus":f.upStatus,
		"filesize(M)":f.filesize,
	}).Info("file base info")
}

func (f *File) BlockUpload(hst hi.Host, ab *cm.AddrsBook, blkQ chan struct{}, shdQ chan struct{},
			tkpool chan *tk.IdToToken, shardSucs int, wg *sync.WaitGroup, cst *st.Ccstat, nst *st.NodeStat) {
	f.SetUsed()
	log.WithFields(log.Fields{
		"filename": f.fileName,
		"status": f.upStatus,
	}).Info("file uploading")

	for _, v := range f.blocks {
		if v.IsUnupload() {
			blkQ <- struct{}{}
			go v.ShardUpload(hst, ab, blkQ, shdQ, tkpool, shardSucs, f.fileName, wg, cst, nst)
		}
	}

	for {
		<- time.After(100*time.Millisecond)
		f.CheckUploaded()
		if f.IsUpFinish() {
			break
		}
	}
}

func (f *File) CheckUploaded() {
	f.blkSucs = 0
	for _, v := range f.blocks {
		if v.IsUploaded() {
			f.blkSucs++
		}
	}

	if f.blkSucs >= len(f.blocks) {
		f.SetUpFinished()
		log.WithFields(log.Fields{
			"fileName": f.fileName,
			"blks": f.blkSucs,
			"filesize(M)": f.filesize,
		}).Info("file upload success")
	}
}