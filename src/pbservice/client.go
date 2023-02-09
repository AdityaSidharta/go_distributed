package pbservice

import (
	"crypto/rand"
	"math/big"
	"strconv"
	"time"
	"viewservice"
)
import "net/rpc"
import "fmt"

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	me     string
	callID string
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.me = me
	ck.callID = strconv.Itoa(int(nrand()))
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
func (ck *Clerk) Get(key string) string {
	done := false
	result := ""
	for !done {
		view, ok := ck.vs.Get()
		if !ok {
			DPrintln("ViewService Get has failed")
			time.Sleep(100 * time.Millisecond)
		}
		primary := view.Primary
		args := &GetArgs{
			key,
			ck.callID,
		}
		var reply GetReply
		ok = call(primary, "PBServer.Get", args, &reply)
		if !ok {
			DPrintln("Get: Reply %s", string(reply.Err))
			time.Sleep(100 * time.Millisecond)
		} else {
			DPrintln("Get: Reply %s", string(reply.Err))
			done = true
			result = reply.Value
		}
	}
	return result
}

// tell the primary to update key's value.
// must keep trying until it succeeds.
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	done := false
	result := ""
	for !done {
		view, ok := ck.vs.Get()
		if !ok {
			DPrintln("ViewService Get has failed")
			time.Sleep(100 * time.Millisecond)
		}
		primary := view.Primary
		args := &PutArgs{
			key,
			value,
			dohash,
			ck.callID,
		}
		var reply PutReply
		ok = call(primary, "PBServer.Put", args, &reply)
		if !ok {
			DPrintln("PutExt: Reply %s", string(reply.Err))
			time.Sleep(100 * time.Millisecond)
		} else {
			DPrintln("PutExt: Reply %s", string(reply.Err))
			done = true
			result = reply.PreviousValue
		}
	}
	return result
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}

func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
