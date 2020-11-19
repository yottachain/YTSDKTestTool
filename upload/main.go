package main

import (
	"flag"
	"github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"
	host "github.com/yottachain/YTHost"
	"github.com/yottachain/YTHost/option"
	f "github.com/yottachain/YTSDKTestTool/file"
	stat "github.com/yottachain/YTSDKTestTool/stat"
	tk "github.com/yottachain/YTSDKTestTool/token"
	cm "github.com/yottachain/YTStTool/ClientManage"
	st "github.com/yottachain/YTStTool/stat"
	"os"
	"runtime"
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
	var divisor uint
	var openstat *bool
	var openTkPool *bool
	var connNowait *bool
	var nodeSeqence *bool
	var wgdivisor int
	var nodeShards int

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
	flag.UintVar(&divisor, "div", 2, "选择所有节点的1/div的数量作为发送节点, 默认是2也就是选取一半的节点")
	flag.IntVar(&wgdivisor, "wgdiv", 10, "矿机权重命中概率因子")
	flag.IntVar(&nodeShards, "ns", 8, "每个矿机存储块的最大分片数")
	openstat = flag.Bool("os", false, "是否开启成功率等相关统计")
	openTkPool = flag.Bool("otp", false, "是否开启token池")
	connNowait = flag.Bool("cnw", false, "与节点建立连接是否等待")
	nodeSeqence = flag.Bool("sns", false, "是否从节点列表中顺序选取节点")
	flag.Parse()

	log.SetOutput(os.Stdout)

	log.WithFields(log.Fields{
		"divisor": divisor,
		"wgdivisor": wgdivisor,
		"openstat": *openstat,
		"openTkPool": *openTkPool,
		"connNowait": *connNowait,
		"nodeSeqence": *nodeSeqence,
		"files": files,
		"filesize": filesize,
		"shardeds": shardeds,
		"gtkcc": gtkcc,
		"filecc": filecc,
		"blockcc": blockcc,
		"shardcc": shardcc,
		"miners": miners,
		"sn": codeType,
		"ns": nodeShards,
	}).Info("args info")

	runtime.GOMAXPROCS(16)
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)

	log.WithFields(log.Fields{
		"rt_cpus":runtime.NumCPU(),
	}).Info("go runtime os info")

	if files < filecc {
		filecc = files
	}

	lsma, err := multiaddr.NewMultiaddr(listenaddr)
	if err != nil {
		log.Fatal(err)
	}
	hst, err := host.NewHost(option.ListenAddr(lsma), option.OpenPProf("0.0.0.0:10000"))
	if err != nil {
		log.Fatal(err)
	}

	ab, err := cm.NewAddBookFromServer(codeType, divisor, int(miners))
	if err != nil {
		log.Fatal(err)
	}else {
		ab.UpdateWeights(1, wgdivisor)
	}
	go ab.Keep(60, codeType, int(miners), 1, wgdivisor)

	fs, err := f.NewFileMage(files, int(filesize), dataOringin)
	if err != nil {
		log.Fatal(err)
	}

	cst := st.NewCcStat()
	nst := &st.NodeStat{}
	nst.Init(*openstat)
	dst := stat.NewDelaystat("delay.log")

	go cst.Print()
	go dst.Print()

	wg := sync.WaitGroup{}
	wg1 := sync.WaitGroup{}

	ups := NewUploads(fs, filecc, blockcc, shardcc, gtkcc, filesize, shardeds, dataOringin, files)
	var IsStop = false
	if *openTkPool == true {
		go tk.GetTkToPool(hst, ab, ups.gtkQueue, ups.tkPool, &IsStop, &wg1, cst, nst, *connNowait)
	}

	upStartTime := time.Now()
	inDatabaseTime := ups.FileUpload(hst, ab, &wg, cst, nst, nodeShards, *openTkPool, dst, *connNowait, *nodeSeqence)
	//log.WithFields(log.Fields{
	//}).Info("indatabase upload success")

	wg.Wait()
	//log.WithFields(log.Fields{
	//}).Info("all upload success")
	totalTime := time.Now().Sub(upStartTime).Seconds()

	IsStop = true
	wg1.Wait()

	go cst.Print()
	nst.Print()
	dst.Clean()

	connRate, gtRate, sendRate, gtAvg, sendAvg, gtTotalSuc, sendTotalSuc := nst.GetTotalStat()

	<- time.After(5*time.Second)
	log.WithFields(log.Fields{
		"lenght": len(ups.files),
	}).Info("real files")

	log.WithFields(log.Fields{
		"connrate":connRate,
		"gtrate":gtRate,
		"sendrate":sendRate,
		"gtavg":gtAvg,
		"sendavg":sendAvg,
		"gtTotalSuc":gtTotalSuc,
		"sendTotalSuc":sendTotalSuc,

	}).Info("success rate stat")

	log.WithFields(log.Fields{
		"in database totalTime": inDatabaseTime.Seconds(),
		"in database speed(M/s)":float32(files*filesize)/float32(inDatabaseTime.Seconds()),
		"real totalTime": totalTime,
		"real speed(M/s)":float32(files*filesize)/float32(totalTime),
	}).Info("all files upload success")

	ab.PrintWeight()
}
