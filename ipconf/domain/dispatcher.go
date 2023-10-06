package domain

import (
	"github.com/hardcore-os/plato/ipconf/source"
	"sort"
	"sync"
)

type Dispatcher struct {
	candidatePool map[string]*Endport
	sync.RWMutex
}

var dp *Dispatcher

func Init() {
	dp = &Dispatcher{}
	dp.candidatePool = make(map[string]*Endport)
	go func() {
		for event := range source.EventChan() {
			switch event.Type {
			case source.AddEvent:
				dp.AddNode(event)
			case source.DelEvent:
				dp.DelNode(event)
			}
		}
	}()
}

func Dispatch(ctx *IpConfContext) []*Endport {
	// 获取候选candidate
	eds := dp.getCandidateEndPort(ctx)
	// 计算分数
	for _, ed := range eds {
		ed.CalculateScore(ctx)
	}
	// 全局排序，返回排序策略
	sort.Slice(eds, func(i, j int) bool {
		// 优先基于活跃分数进行排序
		if eds[i].ActiveScore > eds[j].ActiveScore {
			return true
		}
		// 如果活跃分数相同，则使用静态分数排序
		if eds[i].ActiveScore == eds[j].ActiveScore {
			return eds[i].StaticScore > eds[j].StaticScore
		}
		return false
	})
	return eds
}

func (d *Dispatcher) getCandidateEndPort(ctx *IpConfContext) []*Endport {
	dp.RLock()
	defer dp.RUnlock()
	candidateList := make([]*Endport, 0, len(dp.candidatePool))
	for _, ed := range dp.candidatePool {
		candidateList = append(candidateList, ed)
	}
	return candidateList
}

func (d *Dispatcher) DelNode(event *source.Event) {
	dp.Lock()
	delete(dp.candidatePool, event.Key())
	dp.Unlock()
}

func (d *Dispatcher) AddNode(event *source.Event) {
	dp.Lock()
	defer dp.Unlock()
	var (
		ed *Endport
		ok bool
	)
	if ed, ok = d.candidatePool[event.Key()]; !ok { // not exist, then create EndPort
		ed = NewEndport(event.IP, event.Port)
		dp.candidatePool[event.Key()] = ed
	}
	ed.UpdateStat(&Stat{
		ConnectNum:   event.ConnectNum,
		MessageBytes: event.MessageBytes,
	})
}
