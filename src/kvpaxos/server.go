package kvpaxos

import (
	"errors"
	"net"
	"runtime"
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

const Debug = 1
const SERVERTIME = 50

type MethodName string

const (
	Get     MethodName = "Get"
	Put     MethodName = "Put"
	PutHash MethodName = "PutHash"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id     string
	Method MethodName
	Key    string
	Value  string
}

// TODO: If you have submitted, don't submit again

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	seqDone      int               // To let know which sequence has been done
	seqPropose   int               // To let know which one to Propose
	store        map[string]string // Current KV store
	prevStore    map[string]string // Previous KV store
	completedOps map[string]int    // List of all OpID completed
}

func (kv *KVPaxos) DPrintln(format string, a ...interface{}) {
	pc, _, _, _ := runtime.Caller(1)
	if Debug > 0 {
		value := runtime.FuncForPC(pc).Name() + " : " + "m" + strconv.Itoa(kv.me) + " : " + format + "\n"
		_, _ = fmt.Printf(value, a...)
	}
	return
}

func (kv *KVPaxos) getID(method MethodName, key string, value string, callID string) string {
	opID := string(method) + strconv.Itoa(int(hash(key+value))) + callID
	kv.DPrintln("opID: %v", opID)
	return opID
}

func (kv *KVPaxos) Submit(op Op) (int, Err) {
	gob.Register(Op{})
	id := op.Id
	if kv.seqDone+1 > kv.px.Max() {
		kv.seqPropose = kv.seqDone + 1
	} else {
		kv.seqPropose = kv.px.Max()
	}
	kv.DPrintln("starting Submit for seq %v", kv.seqPropose)
	for {
		kv.px.Start(kv.seqPropose, op)
		time.Sleep(SERVERTIME * time.Millisecond)
		done, value := kv.px.Status(kv.seqPropose)
		if done {
			if value == op {
				kv.DPrintln("Submit for seq %v is successful. id: %v", kv.seqPropose, id)
				seqPropose := kv.seqPropose
				return seqPropose, OK
			} else {
				kv.DPrintln("Submit for seq %v has been taken by other operation. value %v, id %v", kv.seqPropose, value.(Op).Id, id)
				kv.seqPropose = kv.seqPropose + 1
			}
		} else {
			kv.DPrintln("Submit for seq %v has timed out", kv.seqPropose)
			kv.seqPropose = kv.seqPropose - 1
			seqPropose := kv.seqPropose
			return seqPropose, ErrSubmitError
		}
	}
}

func (kv *KVPaxos) Do(maxSeq int) (int, Err) {
	var prevValue string
	var newValue string
	for {
		if kv.seqDone >= maxSeq {
			kv.DPrintln("Seq : %v done up to maxSeq: %v, exiting", kv.seqDone, maxSeq)
			return kv.seqDone, OK
		}
		done, op := kv.px.Status(kv.seqDone + 1)
		if done {
			id := op.(Op).Id
			method := op.(Op).Method
			key := op.(Op).Key
			value := op.(Op).Value
			kv.DPrintln("id : %v, method : %v, key : %v", id, method, key)

			_, ok := kv.completedOps[id]
			if ok {
				kv.DPrintln("Operation id %v has been completed", id)
			} else {
				if method == Get {
					kv.DPrintln("Get method. Nothing needs to be done")
				} else if method == Put {
					prevValue, ok = kv.store[key]
					if !ok {
						prevValue = ""
					}
					newValue = value
					kv.store[key] = newValue
					kv.prevStore[key] = prevValue
				} else if method == PutHash {
					prevValue, ok = kv.store[key]
					if !ok {
						prevValue = ""
					}
					newValue = strconv.Itoa(int(hash(prevValue + value)))
					kv.store[key] = newValue
					kv.prevStore[key] = prevValue
				} else {
					kv.DPrintln("Method Not Recognized %v", method)
					return kv.seqDone, ErrInvalidMethod
				}
				kv.DPrintln("Storing id %v", id)
				kv.completedOps[id] = kv.seqDone
			}
			kv.px.Done(kv.seqDone)
			kv.DPrintln("Calling Seq Done on %v, current globalDone %v", kv.seqDone, kv.px.GlobalDone)
			//kv.CleanUpDo(kv.px.GlobalDone)
			kv.seqDone = kv.seqDone + 1
		} else {
			kv.DPrintln("Seq : %v is not done, exiting", kv.seqDone+1)
			return kv.seqDone, OK
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	var id string
	var method MethodName

	kv.mu.Lock()
	defer kv.mu.Unlock()

	method = Get
	key := args.Key
	value := ""
	callID := args.CallID

	id = kv.getID(method, key, value, callID)

	op := Op{
		Id:     id,
		Method: method,
		Key:    key,
		Value:  value,
	}

	submitSeq, err := kv.Submit(op)
	if err != OK {
		reply.Err = err
		return errors.New(string(reply.Err))
	}
	doSeq, err := kv.Do(submitSeq)
	if err != OK {
		reply.Err = err
		return errors.New(string(reply.Err))
	}
	if submitSeq != doSeq {
		reply.Err = ErrDoSeqBehind
		return errors.New(string(reply.Err))
	}
	result, ok := kv.store[key]
	if !ok {
		reply.Value = ""
		reply.Err = ErrNoKey
		return nil
	} else {
		reply.Value = result
		reply.Err = OK
	}
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	var id string
	var method MethodName

	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	value := args.Value
	doHash := args.DoHash
	callID := args.CallID

	if doHash {
		id = kv.getID(PutHash, key, value, callID)
		method = PutHash
	} else {
		id = kv.getID(Put, key, value, callID)
		method = Put
	}

	op := Op{
		Id:     id,
		Method: method,
		Key:    key,
		Value:  value,
	}

	submitSeq, err := kv.Submit(op)
	if err != OK {
		reply.Err = err
		return errors.New(string(reply.Err))
	}
	doSeq, err := kv.Do(submitSeq)
	if err != OK {
		reply.Err = err
		return errors.New(string(reply.Err))
	}
	if submitSeq != doSeq {
		reply.Err = ErrDoSeqBehind
		return errors.New(string(reply.Err))
	}
	result, ok := kv.prevStore[key]
	if !ok {
		reply.Err = ErrNoKey
		return errors.New(string(reply.Err))
	} else {
		reply.PreviousValue = result
		reply.Err = OK
		return nil
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.seqDone = 0
	kv.seqPropose = 0
	kv.store = make(map[string]string)
	kv.prevStore = make(map[string]string)
	kv.completedOps = make(map[string]int)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
