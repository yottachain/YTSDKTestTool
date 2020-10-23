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

		_, _ = fmt.Fprintf(ds.fd, "getNode-delay=%d getAvaialble-delay=%d getNodeId-delay=%d\n",
					gnd.avgDlay, gad.avgDlay, gnidd.avgDlay)
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