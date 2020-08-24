package main

import (
	"flag"
	"github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"
	host "github.com/yottachain/YTHost"
	"github.com/yottachain/YTHost/option"
	f "github.com/yottachain/YTSDKTestTool/file"
	cm "github.com/yottachain/YTStTool/ClientManage"
	"os"
	"sync"
	"time"
)

func main()  {
	var listenaddr string
	var filecc	uint
	var blockcc uint
	var shardcc uint
	var miners  uint
	var files	uint
	var filesize uint
	var dataOringin string
	var shardeds uint
	var codeType string

	flag.StringVar(&listenaddr, "l", "/ip4/0.0.0.0/tcp/9003", "监听地址")
	flag.UintVar(&filecc, "fcc", 5, "上传文件的并发")
	flag.UintVar(&blockcc, "bcc", 100, "上传块的并发")
	flag.UintVar(&shardcc, "scc", 1000, "上传分片的并发")
	flag.UintVar(&miners, "m", 2000, "选取的矿机数量")
	flag.UintVar(&files, "fn", 10, "上传文件的数量")
	flag.UintVar(&filesize, "fs", 100, "上传文件的大小")
	flag.UintVar(&shardeds, "ssn", 144, "上传多少个分片成功认为块上传成功")
	flag.StringVar(&dataOringin, "d", "/dev/urandom", "数据源")
	flag.StringVar(&codeType, "sn", "java", "sn类型，java版本还是go版本")
	flag.Parse()

	log.SetOutput(os.Stdout)

	if files < filecc {
		filecc = files
	}

	lsma, err := multiaddr.NewMultiaddr(listenaddr)
	if err != nil {
		log.Fatal(err)
	}
	hst, err := host.NewHost(option.ListenAddr(lsma))
	if err != nil {
		log.Fatal(err)
	}

	ab, err := cm.NewAddBookFromServer(codeType, 1, int(miners))
	if err != nil {
		log.Fatal(err)
	}else {
		ab.UpdateWeights(0)
	}
	go ab.Keep(60, codeType, int(miners), 0)

	fs, err := f.NewFileMage(filecc, files, int(filesize), dataOringin)
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}

	ups := NewUploads(fs, blockcc, shardcc, filesize, shardeds, dataOringin, files)

	upStartTime := time.Now()
	ups.FileUpload(hst, ab, &wg)
	wg.Wait()
	totalTime := time.Now().Sub(upStartTime).Milliseconds()
	log.WithFields(log.Fields{
		"totalTime": totalTime,
		"speed(M/s)":float32(files*filesize)/float32(totalTime),
	}).Info("all files upload success")

	ab.PrintWeight()

}
