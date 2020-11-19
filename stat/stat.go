package stat

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type delay struct {
	totalDly	time.Duration
	times 		int32
	avgDlay     int32
}

type DelayStat struct {
	delays map[string] *delay
	sync.Mutex
	fd 	*os.File
}

const (
	GETNODEDELAY = "getNodeDelay"
	GETAVAILABLEDEALY = "getAvailbaleDelay"
	GETNODEIDDEALY = "getNodeIdDelay"
	PROTOGTREQDEALY = "protoGtReqDelay"
	PROTOSDREQDEALY = "protoSdReqDelay"
	PROTOSDRSPDEALY = "protoSdRespDelay"
	LOGDELAY = "logWriteDelay"
	GETTOKENDELAY = "getTokenDelay"
)

func NewDelaystat(logName string) (ds *DelayStat) {
	ds = new(DelayStat)
	ds.delays = map[string]*delay {}

	fd, err := os.OpenFile(logName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Fatalln("open stat.log fail  "+ err.Error())
	}
	ds.fd = fd

	return
}

func (ds *DelayStat) Clean() {
	ds.Lock()
	ds.delays = nil
	_ = ds.fd.Close()
	ds.fd = nil
	ds.Unlock()
}

func (ds *DelayStat) Print() {
	for {
		<- time.After(time.Second*1)
		ds.Lock()
		if ds.fd == nil || ds.delays == nil {
			ds.Unlock()
			return
		}
		gnd, ok := ds.delays[GETNODEDELAY]
		if !ok {
			ds.delays[GETNODEDELAY] = &delay{0, 0, 0}
			gnd = ds.delays[GETNODEDELAY]
		}
		gad, ok := ds.delays[GETAVAILABLEDEALY]
		if !ok {
			ds.delays[GETAVAILABLEDEALY] = &delay{0, 0, 0}
			gad = ds.delays[GETAVAILABLEDEALY]
		}
		gnidd, ok := ds.delays[GETNODEIDDEALY]
		if !ok {
			ds.delays[GETNODEIDDEALY] = &delay{0, 0, 0}
			gnidd = ds.delays[GETNODEIDDEALY]
		}
		gtreq, ok := ds.delays[PROTOGTREQDEALY]
		if !ok {
			ds.delays[PROTOGTREQDEALY] = &delay{0, 0, 0}
			gtreq = ds.delays[PROTOGTREQDEALY]
		}
		sdreq, ok := ds.delays[PROTOSDREQDEALY]
		if !ok {
			ds.delays[PROTOSDREQDEALY] = &delay{0, 0, 0}
			sdreq = ds.delays[PROTOSDREQDEALY]
		}
		sdresp, ok := ds.delays[PROTOSDRSPDEALY]
		if !ok {
			ds.delays[PROTOSDRSPDEALY] = &delay{0, 0, 0}
			sdresp = ds.delays[PROTOSDRSPDEALY]
		}
		logdly, ok := ds.delays[LOGDELAY]
		if !ok {
			ds.delays[LOGDELAY] = &delay{0, 0, 0}
			logdly = ds.delays[LOGDELAY]
		}
		gtdly, ok := ds.delays[GETTOKENDELAY]
		if !ok {
			ds.delays[GETTOKENDELAY] = &delay{0, 0, 0}
			gtdly = ds.delays[GETTOKENDELAY]
		}

		_, _ = fmt.Fprintf(ds.fd, "getNode-delay=%d getAvaialble-delay=%d getNodeId-delay=%d " +
					"proto-gtreq-delay=%d proto-sdreq-delay=%d proto-sdresp-delay=%d log-delay=%d gettoken-delay=%d\n",
					gnd.avgDlay, gad.avgDlay, gnidd.avgDlay, gtreq.avgDlay, sdreq.avgDlay, sdresp.avgDlay, logdly.avgDlay, gtdly.avgDlay)
		ds.Unlock()
	}
}

func (ds *DelayStat) CalcDly(key string, dly time.Duration) {
	ds.Lock()
	defer ds.Unlock()
	if ds.delays == nil {
		return
	}

	value, ok := ds.delays[key]
	if !ok {
		ds.delays[key] = &delay{0, 0, 0}
		value = ds.delays[key]
	}

	value.totalDly = value.totalDly + dly
	value.times++
	value.avgDlay = int32(value.totalDly.Milliseconds() / int64(value.times))
}