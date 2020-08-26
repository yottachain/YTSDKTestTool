package main

import (
	"flag"
	"github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"
	host "github.com/yottachain/YTHost"
	"github.com/yottachain/YTHost/option"
	f "github.com/yottachain/YTSDKTestTool/file"
	tk "github.com/yottachain/YTSDKTestTool/token"
	cm "github.com/yottachain/YTStTool/ClientManage"
	st "github.com/yottachain/YTStTool/stat"
	"os"
	"sync"
	"time"
)

func main()  {
	var listenaddr string
	var filecc	uint
	var blockcc uint
	var shardcc uint
	var gtkcc   uint
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
	flag.UintVar(&gtkcc, "gtcc", 2000, "获取token的并发")
	flag.UintVar(&miners, "m", 2000, "选取的矿机数量")
	flag.UintVar(&files, "fn", 10, "上传文件的数量")
	flag.UintVar(&filesize, "fs", 100, "上传文件的大小(单位M)")
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
		ab.UpdateWeights(1)
	}
	go ab.Keep(60, codeType, int(miners), 0)

	fs, err := f.NewFileMage(filecc, files, int(filesize), dataOringin)
	if err != nil {
		log.Fatal(err)
	}

	cst := st.NewCcStat()
	go cst.Print()

	wg := sync.WaitGroup{}
	wg1 := sync.WaitGroup{}

	ups := NewUploads(fs, blockcc, shardcc, gtkcc, filesize, shardeds, dataOringin, files)
	var IsStop = false
	go tk.GetTkToPool(hst, ab, ups.gtkQueue, ups.tkPool, &IsStop, &wg1, cst)

	upStartTime := time.Now()
	inDatabaseTime := ups.FileUpload(hst, ab, &wg, cst)
	wg.Wait()
	IsStop = true
	wg1.Wait()

	totalTime := time.Now().Sub(upStartTime).Seconds()

	<- time.After(5*time.Second)

	log.WithFields(log.Fields{
		"in database totalTime": inDatabaseTime.Seconds(),
		"in database speed(M/s)":float32(files*filesize)/float32(inDatabaseTime.Seconds()),
		"real totalTime": totalTime,
		"real speed(M/s)":float32(files*filesize)/float32(totalTime),
	}).Info("all files upload success")

	ab.PrintWeight()
}
