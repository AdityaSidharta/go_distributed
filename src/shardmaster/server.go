package shardmaster

import (
	crypto "crypto/rand"
	"errors"
	"math/big"
	"net"
	"runtime"
	"sort"
	"strconv"
	"time"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug = 0
const SERVERTIME = 50

type MethodName string
type Err string

type GroupCount struct {
	count int
	gid   int64
}

const (
	Join  MethodName = "Join"
	Leave MethodName = "Leave"
	Move  MethodName = "Move"
	Query MethodName = "Query"
)

const (
	OK                Err = "OK"
	ErrSubmitError    Err = "ErrSubmitError"
	ErrInvalidMethod  Err = "ErrInvalidMethod"
	ErrDoSeqBehind    Err = "ErrDoSeqBehind"
	ErrConfigNotFound Err = "ErrConfigNotFound"
	ErrConfigNotSame  Err = "ErrConfigNotSame"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs    []Config // indexed by config num
	seqDone    int
	seqPropose int

	configNum int
	shards    [NShards]int64     // gid
	groups    map[int64][]string // gid -> servers[]
	//gidActive []int64
}

type Op struct {
	// Your data here.
	Id      string
	Method  MethodName
	GID     int64
	Servers []string
	Shard   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crypto.Int(crypto.Reader, max)
	x := bigx.Int64()
	return x
}

//func calcPosition(index float64) float64 {
//	if index == 0. {
//		return 0.
//	} else {
//		b := math.Floor(math.Log2(index)) + 1
//		c := math.Pow(2, math.Floor(math.Log2(index)))
//		a := ((index - c) * 2) + 1
//		return a * math.Pow(2, -b)
//	}
//}
//
//func getPosition(gidActive []int64) map[int64]float64 {
//	var idx int
//	var gid int64
//	gidPosition := make(map[int64]float64)
//	for idx, gid = range gidActive {
//		if gid != 0 {
//			gidPosition[gid] = calcPosition(float64(idx)) * 10.
//		}
//	}
//	return gidPosition
//}
//
//func getShards(gidPosition map[int64]float64) [NShards]int64 {
//	var bestGID int64
//	var diff float64
//	shards := [NShards]int64{}
//	for i := 0; i < 10; i++ {
//		shardPos := float64(i)
//		bestGID = 0
//		bestDiff := math.Pow(10, 99)
//		for gid, pos := range gidPosition {
//			if pos-shardPos >= 0 {
//				diff = pos - shardPos
//			} else {
//				diff = pos + float64(NShards) - shardPos
//			}
//			if diff < bestDiff {
//				bestGID = gid
//				bestDiff = diff
//			}
//		}
//		shards[i] = bestGID
//	}
//	return shards
//}

func sortGroupCount(groupCounts []GroupCount) {
	sort.SliceStable(groupCounts, func(i, j int) bool {
		mi, mj := groupCounts[i], groupCounts[j]
		switch {
		case mi.count != mj.count:
			return mi.count > mj.count
		default:
			return mi.gid < mj.gid
		}
	})
}

func contain(list []int64, value int64) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

func getMinMax(shards [NShards]int64, activeGroups []int64) (int, int, int64, int64) {
	counts := map[int64]int{}
	var groupCounts []GroupCount
	for _, g := range activeGroups {
		counts[g] = 0
	}
	for _, g := range shards {
		counts[g] += 1
	}
	if _, ok := counts[0]; ok {
		delete(counts, 0)
	}
	for g, v := range counts {
		groupCounts = append(groupCounts, GroupCount{
			count: v,
			gid:   g,
		})
	}
	sortGroupCount(groupCounts)
	max := groupCounts[0].count
	maxGid := groupCounts[0].gid
	min := groupCounts[len(groupCounts)-1].count
	minGid := groupCounts[len(groupCounts)-1].gid
	return max, min, maxGid, minGid
}

func rebalance(shards [NShards]int64, activeGroups []int64) [NShards]int64 {
	if len(activeGroups) == 0 {
		for idx, _ := range shards {
			shards[idx] = 0
			return shards
		}
	} else {
		for idx, value := range shards {
			if !contain(activeGroups, value) {
				shards[idx] = 0
			}
		}
		max, min, maxGid, minGid := getMinMax(shards, activeGroups)

		for idx, value := range shards {
			if value == 0 {
				shards[idx] = minGid
				return rebalance(shards, activeGroups)
			}
		}
		if max-min > 1 {
			for idx, value := range shards {
				if value == maxGid {
					shards[idx] = minGid
					return rebalance(shards, activeGroups)
				}
			}
		} else {
			return shards
		}
	}
	return shards
}

func (sm *ShardMaster) DPrintln(format string, a ...interface{}) {
	pc, _, _, _ := runtime.Caller(1)
	if Debug > 0 {
		value := runtime.FuncForPC(pc).Name() + " : " + "m" + strconv.Itoa(sm.me) + " : " + format + "\n"
		_, _ = fmt.Printf(value, a...)
	}
	return
}

//func (sm *ShardMaster) Insert(newGid int64) bool {
//	var idx int
//	var gid int64
//	for _, gid = range sm.gidActive {
//		if gid == newGid {
//			sm.DPrintln("gid %v is already inside the active lineup")
//			return false
//		}
//	}
//	for idx, gid = range sm.gidActive {
//		if gid == 0 {
//			sm.DPrintln("appending gid %v to idx %v", gid, idx)
//			sm.gidActive[idx] = newGid
//			sm.DPrintln("gidActive : %v", sm.gidActive)
//			return true
//		}
//	}
//	sm.DPrintln("Creating new index for gid %v", gid)
//	sm.gidActive = append(sm.gidActive, newGid)
//	sm.DPrintln("gidActive : %v", sm.gidActive)
//	return true
//}
//
//func (sm *ShardMaster) Remove(curGid int64) bool {
//	var idx int
//	var gid int64
//	var found bool
//	found = false
//	for _, gid = range sm.gidActive {
//		if gid == curGid {
//			found = true
//		}
//	}
//	if !found {
//		sm.DPrintln("gid %v is not inside the active lineup")
//		return false
//	}
//	for idx, gid = range sm.gidActive {
//		if gid == curGid {
//			sm.DPrintln("removing gid %v in idx %v", gid, idx)
//			sm.gidActive[idx] = 0
//		}
//	}
//	sm.DPrintln("gidActive : %v", sm.gidActive)
//	return true
//}

func (sm *ShardMaster) Submit(op Op) (int, Err) {
	gob.Register(Op{})
	id := op.Id
	if sm.seqDone+1 > sm.px.Max() {
		sm.seqPropose = sm.seqDone + 1
	} else {
		sm.seqPropose = sm.px.Max()
	}
	sm.DPrintln("starting Submit for seq %v", sm.seqPropose)
	for {
		sm.px.Start(sm.seqPropose, op)
		time.Sleep(SERVERTIME * time.Millisecond)
		done, value := sm.px.Status(sm.seqPropose)
		if done {
			if value.(Op).Id == op.Id {
				sm.DPrintln("Submit for seq %v is successful. id: %v", sm.seqPropose, id)
				seqPropose := sm.seqPropose
				return seqPropose, OK
			} else {
				sm.DPrintln("Submit for seq %v has been taken by other operation. value %v, id %v", sm.seqPropose, value.(Op).Id, id)
				sm.seqPropose = sm.seqPropose + 1
			}
		} else {
			sm.DPrintln("Submit for seq %v has timed out", sm.seqPropose)
			sm.seqPropose = sm.seqPropose - 1
			seqPropose := sm.seqPropose
			return seqPropose, ErrSubmitError
		}
	}
}

func (sm *ShardMaster) Do(maxSeq int) (int, Err) {
	for {
		if sm.seqDone >= maxSeq {
			sm.DPrintln("Seq : %v done up to maxSeq: %v, exiting", sm.seqDone, maxSeq)
			return sm.seqDone, OK
		}
		done, op := sm.px.Status(sm.seqDone + 1)
		if done {
			id := op.(Op).Id
			method := op.(Op).Method
			sm.DPrintln("id : %v, method : %v", id, method)

			switch method {
			case Join:
				gid := op.(Op).GID
				servers := op.(Op).Servers
				sm.DPrintln("Join method. GID: %v, Servers: %v", gid, servers)

				_, ok := sm.groups[gid]
				if !ok {
					sm.DPrintln("prevConfigNum : %v", sm.configNum)
					sm.DPrintln("prevShards : %v", sm.shards)
					sm.DPrintln("prevGroups : %v", sm.groups)

					newShards := [NShards]int64{}
					newGroups := make(map[int64][]string)
					var activeGroups []int64

					for i, v := range sm.shards {
						newShards[i] = v
					}

					for g, s := range sm.groups {
						newGroups[g] = s
					}

					newGroups[gid] = servers

					for g, _ := range newGroups {
						activeGroups = append(activeGroups, g)
					}

					newShards = rebalance(newShards, activeGroups)

					sm.configNum = sm.configNum + 1
					sm.shards = newShards
					sm.groups = newGroups

					sm.configs = append(sm.configs, Config{
						sm.configNum,
						sm.shards,
						sm.groups,
					})
					sm.DPrintln("newConfigNum : %v", sm.configNum)
					sm.DPrintln("newShards : %v", sm.shards)
					sm.DPrintln("newGroups : %v", sm.groups)

				} else {
					sm.DPrintln("Join has already been done")
				}

			case Leave:
				gid := op.(Op).GID
				sm.DPrintln("Leave method. GID: %v", gid)

				_, ok := sm.groups[gid]
				if ok {
					sm.DPrintln("prevConfigNum : %v", sm.configNum)
					sm.DPrintln("prevShards : %v", sm.shards)
					sm.DPrintln("prevGroups : %v", sm.groups)

					newShards := [NShards]int64{}
					newGroups := make(map[int64][]string)
					var activeGroups []int64

					for i, v := range sm.shards {
						newShards[i] = v
					}

					for g, s := range sm.groups {
						newGroups[g] = s
					}

					if _, ok = newGroups[gid]; ok {
						delete(newGroups, gid)
					}

					for g, _ := range newGroups {
						activeGroups = append(activeGroups, g)
					}

					newShards = rebalance(newShards, activeGroups)

					sm.configNum = sm.configNum + 1
					sm.shards = newShards
					sm.groups = newGroups

					sm.configs = append(sm.configs, Config{
						sm.configNum,
						sm.shards,
						sm.groups,
					})

					sm.DPrintln("newConfigNum : %v", sm.configNum)
					sm.DPrintln("newShards : %v", sm.shards)
					sm.DPrintln("newGroups : %v", sm.groups)

				} else {
					sm.DPrintln("Leave has already been done")
				}
			case Move:
				shard := op.(Op).Shard
				gid := op.(Op).GID
				sm.DPrintln("Move method. GID: %v, Shard: %v", gid, shard)

				newShards := [NShards]int64{}
				for i, v := range sm.shards {
					newShards[i] = v
				}
				if newShards[shard] != gid {
					sm.DPrintln("prevConfigNum : %v", sm.configNum)
					sm.DPrintln("prevShards : %v", sm.shards)

					newShards[shard] = gid

					sm.configNum = sm.configNum + 1
					sm.shards = newShards
					sm.configs = append(sm.configs, Config{
						sm.configNum,
						sm.shards,
						sm.groups,
					})
					sm.DPrintln("newConfigNum : %v", sm.configNum)
					sm.DPrintln("newShards : %v", sm.shards)
				} else {
					sm.DPrintln("Move has already been done")
				}

			case Query:
				sm.DPrintln("Query method. Nothing needs to be done")
			default:
				sm.DPrintln("Method Not Recognized %v", method)
				return sm.seqDone, ErrInvalidMethod
			}

			sm.px.Done(sm.seqDone)
			sm.DPrintln("Calling Seq Done on %v, current globalDone %v", sm.seqDone, sm.px.GlobalDone)
			sm.seqDone = sm.seqDone + 1
		} else {
			sm.DPrintln("Seq : %v is not done, exiting", sm.seqDone+1)
			return sm.seqDone, OK
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	var method MethodName

	sm.mu.Lock()
	defer sm.mu.Unlock()

	method = Join
	id := strconv.Itoa(int(nrand()))
	gid := args.GID
	servers := args.Servers
	shard := 0
	op := Op{
		Id:      id,
		Method:  method,
		GID:     gid,
		Servers: servers,
		Shard:   shard,
	}

	submitSeq, err := sm.Submit(op)
	if err != OK {
		return errors.New(string(err))
	}
	doSeq, err := sm.Do(submitSeq)
	if err != OK {
		return errors.New(string(err))
	}
	if submitSeq != doSeq {
		err = ErrDoSeqBehind
		return errors.New(string(err))
	}
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	var method MethodName

	sm.mu.Lock()
	defer sm.mu.Unlock()

	method = Leave
	id := strconv.Itoa(int(nrand()))
	gid := args.GID
	var servers []string
	shard := 0
	op := Op{
		Id:      id,
		Method:  method,
		GID:     gid,
		Servers: servers,
		Shard:   shard,
	}

	submitSeq, err := sm.Submit(op)
	if err != OK {
		return errors.New(string(err))
	}
	doSeq, err := sm.Do(submitSeq)
	if err != OK {
		return errors.New(string(err))
	}
	if submitSeq != doSeq {
		err = ErrDoSeqBehind
		return errors.New(string(err))
	}
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	var method MethodName

	sm.mu.Lock()
	defer sm.mu.Unlock()

	method = Move
	id := strconv.Itoa(int(nrand()))
	gid := args.GID
	var servers []string
	shard := args.Shard
	op := Op{
		Id:      id,
		Method:  method,
		GID:     gid,
		Servers: servers,
		Shard:   shard,
	}

	submitSeq, err := sm.Submit(op)
	if err != OK {
		return errors.New(string(err))
	}
	doSeq, err := sm.Do(submitSeq)
	if err != OK {
		return errors.New(string(err))
	}
	if submitSeq != doSeq {
		err = ErrDoSeqBehind
		return errors.New(string(err))
	}
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	var method MethodName

	sm.mu.Lock()
	defer sm.mu.Unlock()
	method = Query
	id := strconv.Itoa(int(nrand()))
	var gid int64
	var servers []string
	shard := 0

	op := Op{
		Id:      id,
		Method:  method,
		GID:     gid,
		Servers: servers,
		Shard:   shard,
	}

	submitSeq, err := sm.Submit(op)
	if err != OK {
		return errors.New(string(err))
	}
	doSeq, err := sm.Do(submitSeq)
	if err != OK {
		return errors.New(string(err))
	}
	if submitSeq != doSeq {
		err = ErrDoSeqBehind
		return errors.New(string(err))
	}

	num := args.Num
	sm.DPrintln("num: %v", num)
	if num < 0 {
		reply.Config = sm.configs[len(sm.configs)-1]
		return nil
	}

	if len(sm.configs) < num {
		err = ErrConfigNotFound
		return errors.New(string(err))
	}

	config := sm.configs[num]
	if config.Num != num {
		err = ErrConfigNotSame
		return errors.New(string(err))
	}

	reply.Config = config
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
