package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"math"
	"net"
	"runtime"
	"strconv"
	"time"
)
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

type ProposeStatus string
type AcceptStatus string
type DecideStatus string

const Debug = 0

const (
	ProposeOk      ProposeStatus = "ProposeOk"
	ProposeNo      ProposeStatus = "ProposeNo"
	ProposeTimeout ProposeStatus = "ProposeTimeout"

	AcceptOk      AcceptStatus = "AcceptOk"
	AcceptNo      AcceptStatus = "AcceptNo"
	AcceptTimeout AcceptStatus = "AcceptTimeout"

	DecideOk      DecideStatus = "DecideOk"
	DecideNo      DecideStatus = "DecideNo"
	DecideTimeout DecideStatus = "DecideTimeout"
)

type InstanceMetadata struct {
	np       float64
	na       float64
	va       interface{}
	done     bool
	proposes []ProposeStatus
	accepts  []AcceptStatus
	decides  []DecideStatus
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instancesMetadata map[int]InstanceMetadata
	myAddress         string
	localDone         int
	GlobalDone        int
}

type PRArgs struct {
	Seq int
	N   float64
}

type PRReply struct {
	Status ProposeStatus
	Np     float64
	Na     float64
	Va     interface{}
	Done   bool
}

type ARArgs struct {
	Seq int
	N   float64
	V   interface{}
}

type ARReply struct {
	Status AcceptStatus
	Np     float64
	Na     float64
	Va     interface{}
	Done   bool
}

type DRArgs struct {
	Seq int
	N   float64
	V   interface{}
}

type DRReply struct {
	Status DecideStatus
	Np     float64
	Na     float64
	Va     interface{}
	Done   bool
}

type MQArgs struct {
	Placeholder int
}

type MQReply struct {
	LocalDone int
}

type MUArgs struct {
	GlobalDone int
}

type MUReply struct {
	Placeholder int
}

type ERArgs struct {
	GlobalDone int
}

type ERReply struct {
	N int
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) DPrintln(seq int, format string, a ...interface{}) {
	pc, _, _, _ := runtime.Caller(1)
	if Debug > 0 {
		value := runtime.FuncForPC(pc).Name() + " : " + "m" + strconv.Itoa(px.me) + " : s" + strconv.Itoa(seq) + " - " + format + "\n"
		_, _ = fmt.Printf(value, a...)
	}
	return
}

func (px *Paxos) CreateIfNotExists(seq int) {
	px.mu.Lock()
	_, ok := px.instancesMetadata[seq]
	if !ok {
		px.DPrintln(seq, "Create New InstanceMetadata for seq %v", seq)
		px.instancesMetadata[seq] = InstanceMetadata{
			np:       0,
			na:       0,
			va:       nil,
			done:     false,
			proposes: make([]ProposeStatus, len(px.peers)),
			accepts:  make([]AcceptStatus, len(px.peers)),
			decides:  make([]DecideStatus, len(px.peers)),
		}
	}
	px.mu.Unlock()
}

func (px *Paxos) ProposeChoose(seq int, replies []PRReply, n float64, v interface{}) (float64, interface{}) {
	var prevVa interface{}
	var acceptV interface{}
	prevNa := 0.0
	prevVa = nil
	for _, reply := range replies {
		if reply.Na > prevNa {
			prevNa = reply.Na
			prevVa = reply.Va
		}
	}
	px.DPrintln(seq, "prevNa : %v, prevVa: %v", prevNa, prevVa)
	acceptN := n
	if prevVa == nil {
		acceptV = v
	} else {
		acceptV = prevVa
	}

	px.DPrintln(seq, "N : %v, V : %v", acceptN, acceptV)

	return acceptN, acceptV
}

func (px *Paxos) ProposeUpdate(seq int, replies []PRReply) {
	px.mu.Lock()
	defer px.mu.Unlock()

	metadata := px.instancesMetadata[seq]

	for _, reply := range replies {
		if reply.Np > metadata.np {
			metadata.np = reply.Np
		}
		if reply.Na > metadata.na {
			metadata.na = reply.Na
			metadata.va = reply.Va
			if reply.Done {
				metadata.done = reply.Done
			}
		}
	}
	px.DPrintln(seq, "Np : %v, Na : %v, Va : %v, done %v", metadata.np, metadata.na, metadata.va, metadata.done)
	px.instancesMetadata[seq] = metadata
}

func (px *Paxos) ProposeSuccess(seq int) bool {
	px.mu.Lock()
	defer px.mu.Unlock()

	metadata := px.instancesMetadata[seq]
	if metadata.done {
		px.DPrintln(seq, "seq %v has been completed", seq)
		return false
	} else {
		nSuccess := 0.0
		for _, propose := range metadata.proposes {
			if propose == ProposeOk {
				nSuccess = nSuccess + 1.0
			}
		}
		px.DPrintln(seq, "success %v out of %v", nSuccess, len(px.peers))
		if nSuccess/float64(len(px.peers)) > 0.5 {
			return true
		} else {
			return false
		}
	}
}

func (px *Paxos) ProposeSendSingle(seq int, n float64, peer string, peerIndex int) PRReply {
	args := PRArgs{
		Seq: seq,
		N:   n,
	}
	var reply PRReply

	px.CreateIfNotExists(seq)

	if peer == px.myAddress {
		err := px.ProposeReceive(&args, &reply)
		if err == nil {
			px.mu.Lock()
			px.DPrintln(seq, "reply %v : %v", peer, reply.Status)
			px.instancesMetadata[seq].proposes[peerIndex] = reply.Status
			px.mu.Unlock()
		} else {
			px.mu.Lock()
			px.DPrintln(seq, "Timeout : %v", peer)
			reply.Status = ProposeTimeout
			px.instancesMetadata[seq].proposes[peerIndex] = reply.Status
			px.mu.Unlock()
		}
	} else {
		ok := call(peer, "Paxos.ProposeReceive", &args, &reply)
		if ok {
			px.mu.Lock()
			px.DPrintln(seq, "reply %v : %v", peer, reply.Status)
			px.instancesMetadata[seq].proposes[peerIndex] = reply.Status
			px.mu.Unlock()
		} else {
			px.mu.Lock()
			px.DPrintln(seq, "Timeout : %v", peer)
			reply.Status = ProposeTimeout
			px.instancesMetadata[seq].proposes[peerIndex] = reply.Status
			px.mu.Unlock()
		}
	}
	return reply
}

func (px *Paxos) ProposeAgain(seq int, v interface{}, replies []PRReply) {
	r := rand.Intn(10) * 10
	time.Sleep(time.Duration(r)*time.Millisecond + (50 * time.Millisecond))

	px.ProposeUpdate(seq, replies)

	px.mu.Lock()
	metadata := px.instancesMetadata[seq]
	px.mu.Unlock()

	n := math.Floor(metadata.np) + 1.0 + (0.1 * float64(px.me))
	px.DPrintln(seq, "seq: %v, n: %v, n: %v.", seq, n, v)

	if metadata.done {
		px.DPrintln(seq, "Seq %v is done. Preaching Decide", seq)
		px.DecideSend(seq, metadata.np, metadata.va)
	} else {
		px.DPrintln(seq, "Resending Propose")
		px.ProposeSend(seq, n, v)
	}
}

func (px *Paxos) ProposeSend(seq int, n float64, v interface{}) {
	if seq < px.GlobalDone {
		return
	}

	var wg sync.WaitGroup

	px.CreateIfNotExists(seq)

	replies := make([]PRReply, len(px.peers))

	reply := px.ProposeSendSingle(seq, n, px.myAddress, px.me)
	replies[px.me] = reply

	for i, peer := range px.peers {
		if peer != px.myAddress {
			wg.Add(1)
			i := i
			peer := peer
			go func() {
				defer wg.Done()
				reply := px.ProposeSendSingle(seq, n, peer, i)
				replies[i] = reply
			}()
		}
	}

	wg.Wait()
	px.DPrintln(seq, "replies: %v", replies)
	if px.ProposeSuccess(seq) {
		acceptN, acceptV := px.ProposeChoose(seq, replies, n, v)
		px.AcceptSend(seq, acceptN, acceptV)
	} else {
		px.ProposeAgain(seq, v, replies)
	}
}

func (px *Paxos) ProposeReceive(args *PRArgs, reply *PRReply) error {
	seq := args.Seq
	n := args.N

	px.mu.Lock()
	defer px.mu.Unlock()
	metadata, ok := px.instancesMetadata[seq]
	if !ok {
		px.DPrintln(seq, "Create New InstanceMetadata for seq %v", seq)
		px.instancesMetadata[seq] = InstanceMetadata{
			np:       n,
			na:       0.0,
			va:       nil,
			done:     false,
			proposes: make([]ProposeStatus, len(px.peers)),
			accepts:  make([]AcceptStatus, len(px.peers)),
			decides:  make([]DecideStatus, len(px.peers)),
		}
		reply.Status = ProposeOk
		reply.Np = n
		reply.Na = 0.0
		reply.Va = nil
		reply.Done = false

		px.DPrintln(seq, "Reply status %v, Np %v, Na %v, Va %v, Done %v", reply.Status, reply.Np, reply.Na, reply.Va, reply.Done)
		return nil
	} else {
		if metadata.done {
			px.DPrintln(seq, "seq %v is completed", seq)
			reply.Status = ProposeNo
			reply.Np = metadata.np
			reply.Na = metadata.na
			reply.Va = metadata.va
			reply.Done = metadata.done
			px.DPrintln(seq, "Reply status %v, Np %v, Na %v, Va %v, Done %v", reply.Status, reply.Np, reply.Na, reply.Va, reply.Done)
			return nil
		} else if n > metadata.np {
			px.DPrintln(seq, "Propose Accepted, N: %v", n)
			metadata.np = n
			px.instancesMetadata[seq] = metadata

			reply.Status = ProposeOk
			reply.Np = metadata.np
			reply.Na = metadata.na
			reply.Va = metadata.va
			reply.Done = metadata.done
			px.DPrintln(seq, "Reply status %v, Np %v, Na %v, Va %v, Done %v", reply.Status, reply.Np, reply.Na, reply.Va, reply.Done)
			return nil
		} else {
			px.DPrintln(seq, "Outdated proposal. n: %v, np: %v", n, metadata.np)
			reply.Status = ProposeNo
			reply.Np = metadata.np
			reply.Na = metadata.na
			reply.Va = metadata.va
			reply.Done = metadata.done
			px.DPrintln(seq, "Reply status %v, Np %v, Na %v, Va %v, Done %v", reply.Status, reply.Np, reply.Na, reply.Va, reply.Done)
			return nil
		}
	}
}

func (px *Paxos) AcceptUpdate(seq int, replies []ARReply) {
	px.mu.Lock()
	defer px.mu.Unlock()

	metadata := px.instancesMetadata[seq]

	for _, reply := range replies {
		if reply.Np > metadata.np {
			metadata.np = reply.Np
		}
		if reply.Na > metadata.na {
			metadata.na = reply.Na
			metadata.va = reply.Va
			if reply.Done {
				metadata.done = reply.Done
			}
		}
	}
	px.DPrintln(seq, "Np : %v, Na : %v, Va : %v, done %v", metadata.np, metadata.na, metadata.va, metadata.done)
	px.instancesMetadata[seq] = metadata
}

func (px *Paxos) AcceptSuccess(seq int) bool {
	px.mu.Lock()
	defer px.mu.Unlock()

	metadata := px.instancesMetadata[seq]
	if metadata.done {
		px.DPrintln(seq, "seq %v has been completed", seq)
		return false
	} else {
		nSuccess := 0.0
		for _, accept := range metadata.accepts {
			if accept == AcceptOk {
				nSuccess = nSuccess + 1.0
			}
		}
		px.DPrintln(seq, "success %v out of %v", nSuccess, len(px.peers))
		if nSuccess/float64(len(px.peers)) > 0.5 {
			return true
		} else {
			return false
		}
	}
}

func (px *Paxos) AcceptSendSingle(seq int, n float64, v interface{}, peer string, peerIndex int) ARReply {
	args := ARArgs{
		Seq: seq,
		N:   n,
		V:   v,
	}
	var reply ARReply

	px.CreateIfNotExists(seq)

	if peer == px.myAddress {
		err := px.AcceptReceive(&args, &reply)
		if err == nil {
			px.mu.Lock()
			px.DPrintln(seq, "reply %v : %v", peer, reply.Status)
			px.instancesMetadata[seq].accepts[peerIndex] = reply.Status
			px.mu.Unlock()
		} else {
			px.mu.Lock()
			reply.Status = AcceptTimeout
			px.DPrintln(seq, "Timeout : %v", peer)
			px.instancesMetadata[seq].accepts[peerIndex] = reply.Status
			px.mu.Unlock()
		}
	} else {
		ok := call(peer, "Paxos.AcceptReceive", &args, &reply)
		if ok {
			px.mu.Lock()
			px.DPrintln(seq, "reply %v : %v", peer, reply.Status)
			px.instancesMetadata[seq].accepts[peerIndex] = reply.Status
			px.mu.Unlock()
		} else {
			px.mu.Lock()
			px.DPrintln(seq, "Timeout : %v", peer)
			reply.Status = AcceptTimeout
			px.instancesMetadata[seq].accepts[peerIndex] = reply.Status
			px.mu.Unlock()
		}
	}
	return reply
}

func (px *Paxos) AcceptAgain(seq int, v interface{}, replies []ARReply) {
	r := rand.Intn(10) * 10
	time.Sleep(time.Duration(r)*time.Millisecond + (100 * time.Millisecond))

	px.AcceptUpdate(seq, replies)

	px.mu.Lock()
	metadata := px.instancesMetadata[seq]
	px.mu.Unlock()

	n := math.Floor(metadata.np) + 1.0 + (0.1 * float64(px.me))
	px.DPrintln(seq, "seq: %v, n: %v, n: %v.", seq, n, v)

	if metadata.done {
		px.DPrintln(seq, "Seq %v is done, Preaching Decide", seq)
		px.DecideSend(seq, metadata.np, metadata.va)
	} else {
		px.DPrintln(seq, "Resending Propose")
		px.ProposeSend(seq, n, v)
	}
}

func (px *Paxos) AcceptSend(seq int, n float64, v interface{}) {
	if seq < px.GlobalDone {
		return
	}

	var wg sync.WaitGroup

	px.CreateIfNotExists(seq)

	replies := make([]ARReply, len(px.peers))

	reply := px.AcceptSendSingle(seq, n, v, px.myAddress, px.me)
	replies[px.me] = reply

	for i, peer := range px.peers {
		if peer != px.myAddress {
			wg.Add(1)
			i := i
			peer := peer
			go func() {
				defer wg.Done()
				reply := px.AcceptSendSingle(seq, n, v, peer, i)
				replies[i] = reply
			}()
		}
	}

	wg.Wait()
	px.DPrintln(seq, "replies: %v", replies)
	if px.AcceptSuccess(seq) {
		px.DecideSend(seq, n, v)
	} else {
		px.AcceptAgain(seq, v, replies)
	}
}

func (px *Paxos) AcceptReceive(args *ARArgs, reply *ARReply) error {
	seq := args.Seq
	n := args.N
	v := args.V

	px.mu.Lock()
	defer px.mu.Unlock()
	metadata, ok := px.instancesMetadata[seq]
	if !ok {
		px.DPrintln(seq, "Create New InstanceMetadata for seq %v", seq)
		px.instancesMetadata[seq] = InstanceMetadata{
			np:       n,
			na:       n,
			va:       v,
			done:     false,
			proposes: make([]ProposeStatus, len(px.peers)),
			accepts:  make([]AcceptStatus, len(px.peers)),
			decides:  make([]DecideStatus, len(px.peers)),
		}
		reply.Status = AcceptOk
		reply.Np = n
		reply.Na = n
		reply.Va = v
		reply.Done = false

		px.DPrintln(seq, "Reply status %v, Np %v, Na %v, Va %v, Done %v", reply.Status, reply.Np, reply.Na, reply.Va, reply.Done)
		return nil
	} else {
		if metadata.done {
			px.DPrintln(seq, "seq %v is completed", seq)
			reply.Status = AcceptNo
			reply.Np = metadata.np
			reply.Na = metadata.na
			reply.Va = metadata.va
			reply.Done = metadata.done
			px.DPrintln(seq, "Reply status %v, Np %v, Na %v, Va %v, Done %v", reply.Status, reply.Np, reply.Na, reply.Va, reply.Done)
			return nil
		} else if n >= metadata.np {
			px.DPrintln(seq, "Accepts Accepted, N: %v", n)
			metadata.np = n
			metadata.na = n
			metadata.va = v
			px.instancesMetadata[seq] = metadata

			reply.Status = AcceptOk
			reply.Np = metadata.np
			reply.Na = metadata.na
			reply.Va = metadata.va
			reply.Done = metadata.done
			px.DPrintln(seq, "Reply status %v, Np %v, Na %v, Va %v, Done %v", reply.Status, reply.Np, reply.Na, reply.Va, reply.Done)
			return nil
		} else {
			px.DPrintln(seq, "Outdated proposal. n: %v, np: %v", n, metadata.np)
			reply.Status = AcceptNo
			reply.Np = metadata.np
			reply.Na = metadata.na
			reply.Va = metadata.va
			reply.Done = metadata.done
			px.DPrintln(seq, "Reply status %v, Np %v, Na %v, Va %v, Done %v", reply.Status, reply.Np, reply.Na, reply.Va, reply.Done)
			return nil
		}
	}
}

func (px *Paxos) DecideUpdate(seq int, replies []DRReply) {
	px.mu.Lock()
	defer px.mu.Unlock()

	metadata := px.instancesMetadata[seq]

	for _, reply := range replies {
		if reply.Np > metadata.np {
			metadata.np = reply.Np
		}
	}

	px.DPrintln(seq, "Np : %v, Na : %v, Va : %v, done %v", metadata.np, metadata.na, metadata.va, metadata.done)
	px.instancesMetadata[seq] = metadata
}

func (px *Paxos) DecideSuccess(seq int) bool {
	px.mu.Lock()
	defer px.mu.Unlock()

	metadata := px.instancesMetadata[seq]
	nSuccess := 0

	for _, decide := range metadata.decides {
		if decide == DecideOk {
			nSuccess = nSuccess + 1
		}
	}

	px.DPrintln(seq, "success %v out of %v", nSuccess, len(px.peers))

	return nSuccess == len(px.peers)
}

func (px *Paxos) DecideSendSingle(seq int, n float64, v interface{}, peer string, peerIndex int) DRReply {
	args := DRArgs{
		Seq: seq,
		N:   n,
		V:   v,
	}
	var reply DRReply

	px.CreateIfNotExists(seq)

	if peer == px.myAddress {
		err := px.DecideReceive(&args, &reply)
		if err == nil {
			px.mu.Lock()
			px.DPrintln(seq, "reply %v : %v", peer, reply.Status)
			px.instancesMetadata[seq].decides[peerIndex] = reply.Status
			px.mu.Unlock()
		} else {
			px.mu.Lock()
			px.DPrintln(seq, "Timeout : %v", peer)
			reply.Status = DecideTimeout
			px.instancesMetadata[seq].decides[peerIndex] = reply.Status
			px.mu.Unlock()
		}
	} else {
		ok := call(peer, "Paxos.DecideReceive", &args, &reply)
		if ok {
			px.mu.Lock()
			px.DPrintln(seq, "reply %v : %v", peer, reply.Status)
			px.instancesMetadata[seq].decides[peerIndex] = reply.Status
			px.mu.Unlock()
		} else {
			px.mu.Lock()
			px.DPrintln(seq, "Timeout : %v", peer)
			reply.Status = DecideTimeout
			px.instancesMetadata[seq].decides[peerIndex] = reply.Status
			px.mu.Unlock()
		}
	}
	return reply
}

func (px *Paxos) DecideAgain(seq int, n float64, v interface{}) {
	r := rand.Intn(50)
	time.Sleep(time.Duration(r)*time.Microsecond + (50 * time.Microsecond))
	px.DPrintln(seq, "seq: %v, n: %v, n: %v.", seq, n, v)
	px.DPrintln(seq, "Resending Decide")
	px.DecideSend(seq, n, v)
}

func (px *Paxos) DecideSend(seq int, n float64, v interface{}) {
	if seq < px.GlobalDone {
		return
	}

	var wg sync.WaitGroup

	px.CreateIfNotExists(seq)

	replies := make([]DRReply, len(px.peers))

	reply := px.DecideSendSingle(seq, n, v, px.myAddress, px.me)
	replies[px.me] = reply

	for i, peer := range px.peers {
		if peer != px.myAddress {
			wg.Add(1)
			i := i
			peer := peer
			go func() {
				defer wg.Done()
				reply = px.DecideSendSingle(seq, n, v, peer, i)
				replies[i] = reply
			}()
		}
	}

	wg.Wait()

	px.DPrintln(seq, "replies: %v", replies)
	if px.DecideSuccess(seq) {
		px.DPrintln(seq, "Decide Completed for seq %v", seq)
	} else {
		px.DecideUpdate(seq, replies)
		px.DecideAgain(seq, n, v)
	}
}

func (px *Paxos) DecideReceive(args *DRArgs, reply *DRReply) error {
	seq := args.Seq
	n := args.N
	v := args.V

	px.mu.Lock()
	defer px.mu.Unlock()
	metadata, ok := px.instancesMetadata[seq]

	if !ok {
		px.DPrintln(seq, "Create New InstanceMetadata for seq %v", seq)
		px.instancesMetadata[seq] = InstanceMetadata{
			np:       n,
			na:       n,
			va:       v,
			done:     true,
			proposes: make([]ProposeStatus, len(px.peers)),
			accepts:  make([]AcceptStatus, len(px.peers)),
			decides:  make([]DecideStatus, len(px.peers)),
		}

		reply.Status = DecideOk
		reply.Np = n
		reply.Na = n
		reply.Va = v
		reply.Done = true

		px.DPrintln(seq, "Reply status %v, Np %v, Na %v, Va %v, Done %v", reply.Status, reply.Np, reply.Na, reply.Va, reply.Done)
		return nil
	} else {
		if metadata.done {
			px.DPrintln(seq, "seq %v is completed", seq)
			reply.Status = DecideOk
			reply.Np = metadata.np
			reply.Na = metadata.na
			reply.Va = metadata.va
			reply.Done = metadata.done
			px.DPrintln(seq, "Reply status %v, Np %v, Na %v, Va %v, Done %v", reply.Status, reply.Np, reply.Na, reply.Va, reply.Done)
			return nil
		} else {
			metadata.np = n
			metadata.na = n
			metadata.va = v
			metadata.done = true
			px.instancesMetadata[seq] = metadata

			reply.Status = DecideOk
			reply.Np = metadata.np
			reply.Na = metadata.na
			reply.Va = metadata.va
			reply.Done = metadata.done
			px.DPrintln(seq, "Reply status %v, Np %v, Na %v, Va %v, Done %v", reply.Status, reply.Np, reply.Na, reply.Va, reply.Done)
			return nil
		}
	}
}

func (px *Paxos) MinQuery(args *MQArgs, reply *MQReply) error {
	px.mu.Lock()
	reply.LocalDone = px.localDone
	px.mu.Unlock()
	return nil
}

func (px *Paxos) MinUpdate(args *MUArgs, reply *MUReply) error {
	px.mu.Lock()
	if args.GlobalDone > px.GlobalDone {
		px.GlobalDone = args.GlobalDone
	}
	px.mu.Unlock()
	return nil
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	px.mu.Lock()
	metadata, ok := px.instancesMetadata[seq]
	minSeqDone := px.GlobalDone
	px.mu.Unlock()

	if seq < minSeqDone {
		px.DPrintln(seq, "seq %v is below GlobalDone %v", seq, minSeqDone)
		return
	}

	if ok {
		if metadata.done {
			px.DPrintln(seq, "Start: seq %v has been completed. Does not change the value.")
		} else {
			n := math.Floor(metadata.np) + 1.0 + (0.1 * float64(px.me))
			px.DPrintln(seq, "seq %v, n %v, v %v", seq, n, v)
			go px.ProposeSend(seq, n, v)
		}
	} else {
		n := 1.0 + (0.1 * float64(px.me))
		px.DPrintln(seq, "seq %v, n %v, v %v", seq, n, v)
		go px.ProposeSend(seq, n, v)
	}
	return
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	px.localDone = seq
	px.mu.Unlock()

	px.DPrintln(seq, "LocalDone : %v", px.localDone)

	globalDone := px.Min()

	erArgs := ERArgs{
		globalDone,
	}

	for _, peer := range px.peers {
		erReply := ERReply{}
		if peer == px.myAddress {
			px.Erase(&erArgs, &erReply)
		} else {
			call(peer, "Paxos.Erase", &erArgs, &erReply)
		}
	}
}

func (px *Paxos) Erase(args *ERArgs, reply *ERReply) error {
	globalDone := args.GlobalDone
	n := 0
	px.mu.Lock()
	for seq, _ := range px.instancesMetadata {
		if seq < globalDone {
			px.DPrintln(seq, "delete seq %v", seq)
			delete(px.instancesMetadata, seq)
			n = n + 1
		}
	}
	px.mu.Unlock()
	reply.N = n
	return nil
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	value := -1
	for seq, _ := range px.instancesMetadata {
		if seq > value {
			value = seq
		}
	}
	return value
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	px.mu.Lock()
	globalDone := px.GlobalDone
	px.mu.Unlock()

	proposeDone := int(^uint(0) >> 1)
	mqArgs := MQArgs{0}

	for _, peer := range px.peers {
		mqReply := MQReply{}
		if peer == px.myAddress {
			err := px.MinQuery(&mqArgs, &mqReply)
			if err == nil {
				if mqReply.LocalDone < proposeDone {
					proposeDone = mqReply.LocalDone
				}
			} else {
				proposeDone = -1
			}
		} else {
			ok := call(peer, "Paxos.MinQuery", &mqArgs, &mqReply)
			if ok {
				if mqReply.LocalDone < proposeDone {
					proposeDone = mqReply.LocalDone
				}
			} else {
				proposeDone = -1
			}
		}
	}
	if proposeDone+1 > globalDone {
		px.DPrintln(-1, "New GlobalDone : %v", proposeDone+1)
		globalDone = proposeDone + 1
	} else {
		px.DPrintln(-1, "proposeDone + 1 : %v. retain GlobalDone: %v", proposeDone+1, globalDone)
	}

	muArgs := MUArgs{
		globalDone,
	}

	for _, peer := range px.peers {
		muReply := MUReply{}
		if peer == px.myAddress {
			px.MinUpdate(&muArgs, &muReply)
		} else {
			call(peer, "Paxos.MinUpdate", &muArgs, &muReply)
		}
	}
	return globalDone
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (bool, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq < px.GlobalDone {
		return false, nil
	}

	metadata, ok := px.instancesMetadata[seq]
	if !ok {
		return false, nil
	} else {
		if metadata.done {
			return true, metadata.va
		} else {
			return false, nil
		}
	}
}

// tell the peer to shut itself down.
// for testing.
// please do not change this function.
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instancesMetadata = make(map[int]InstanceMetadata)
	px.myAddress = px.peers[px.me]
	px.localDone = -1
	px.GlobalDone = -1

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
