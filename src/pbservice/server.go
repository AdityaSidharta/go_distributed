package pbservice

import (
	"errors"
	"net"
	"strconv"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

func DPrintln(format string, a ...interface{}) {
	if Debug > 0 {
		_, _ = fmt.Printf(format+"\n", a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	mu           sync.Mutex
	viewNum      uint
	primary      string
	backup       string
	store        map[string]string
	completedOps map[string]string
}

func (pb *PBServer) getID(op string, key string, value string, callID string) string {
	opID := op + key + value + callID
	DPrintln("getID: Operation ID : %s", opID)
	return opID
}

func (pb *PBServer) copyMap(ori map[string]string) map[string]string {
	copymap := make(map[string]string)
	for k, v := range ori {
		copymap[k] = v
	}
	return copymap
}

func (pb *PBServer) Clone(args *CloneArgs, reply *CloneReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	DPrintln("%s starts Clone...", pb.me)

	if pb.me != pb.backup {
		DPrintln("Clone: pb.me %s is not Backup : %s", pb.me, pb.backup)
		reply.Err = ErrNotBackup
		return errors.New(ErrNotBackup)
	}
	if args.Sender != pb.primary {
		DPrintln("Clone: Sender %s is not Primary : %s", args.Sender, pb.primary)
		reply.Err = ErrSenderNotMaster
		return errors.New(ErrSenderNotMaster)
	}

	pb.store = args.Store
	pb.completedOps = args.CompletedOps
	reply.Err = OK
	DPrintln("%s completed Clone!", pb.me)
	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	DPrintln("%s Started Put...", pb.me)
	DPrintln("Put: Argument - Key %s, Value %s, DoHash %v, CallID %s", args.Key, args.Value, args.DoHash, args.CallID)

	if pb.me != pb.primary {
		DPrintln("Put: pb.me %s is not Primary : %s", pb.me, pb.primary)
		reply.Err = ErrNotMaster
		DPrintln("%s failed Put!", pb.me)
		return errors.New(ErrNotMaster)
	}

	proposedStore := pb.copyMap(pb.store)
	proposedCompleteOps := pb.copyMap(pb.completedOps)

	opID := pb.getID("Put", args.Key, args.Value, args.CallID)
	result, ok := proposedCompleteOps[opID]

	if ok {
		DPrintln("Put: Operation %s has been done before. Previous Value: %s", opID, result)
	} else {
		result, ok = proposedStore[args.Key]
		if !ok {
			result = ""
		}
		proposedCompleteOps[opID] = result
		DPrintln("Put: New Operation %s. Previous Value: %s", opID, result)

		if args.DoHash {
			value := strconv.Itoa(int(hash(result + args.Value)))
			DPrintln("Put: DoHash. Key : %s, Value %s", args.Key, value)
			proposedStore[args.Key] = value
		} else {
			value := args.Value
			DPrintln("Put: Normal. Key : %s, Value %s", args.Key, value)
			proposedStore[args.Key] = value
		}
	}

	reply.PreviousValue = result

	if pb.backup == "" {
		DPrintln("Put: Backup Not Found")
		reply.Err = OK
		pb.store = proposedStore
		pb.completedOps = proposedCompleteOps
		DPrintln("%s Completed Put!", pb.me)
		return nil
	} else {
		DPrintln("Put: Backup found : %s", pb.backup)
		cloneArgs := &CloneArgs{
			Sender:       pb.me,
			Store:        proposedStore,
			CompletedOps: proposedCompleteOps,
		}
		var cloneReply CloneReply

		ok = call(pb.backup, "PBServer.Clone", cloneArgs, &cloneReply)

		if !ok {
			DPrintln("Updating Backup has returned Timeout")
			reply.Err = ErrBackupTimeout
			DPrintln("%s failed Put!", pb.me)
			return errors.New(ErrBackupTimeout)
		} else {
			if cloneReply.Err == OK {
				reply.Err = OK
				pb.store = proposedStore
				pb.completedOps = proposedCompleteOps
				DPrintln("%s Completed Put!", pb.me)
				return nil
			} else {
				reply.Err = cloneReply.Err
				DPrintln("Updating Backup has returned error : %s", string(reply.Err))
				DPrintln("%s failed Put!", pb.me)
				return errors.New(string(reply.Err))
			}
		}
	}
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	DPrintln("%s Started Get...", pb.me)
	DPrintln("Put: Argument - Key %s, CallID %s", args.Key, args.CallID)

	if pb.me != pb.primary {
		DPrintln("Get: pb.me %s is not Primary : %s", pb.me, pb.primary)
		reply.Err = ErrNotMaster
		DPrintln("%s failed Get!", pb.me)
		return errors.New(ErrNotMaster)
	}

	result, ok := pb.store[args.Key]
	if ok {
		DPrintln("Get: Key %s found in store. Result: %s", args.Key, result)
		reply.Value = result
		reply.Err = OK
	} else {
		DPrintln("Get: Key %s is not found in store", args.Key)
		reply.Value = ""
		reply.Err = ErrNoKey
	}

	if pb.backup == "" {
		DPrintln("Get: Backup Not Found")
		DPrintln("%s completed Get!", pb.me)
		return nil
	} else {
		DPrintln("Put: Backup found : %s", pb.backup)
		cloneArgs := &CloneArgs{
			Sender:       pb.me,
			Store:        pb.store,
			CompletedOps: pb.completedOps,
		}
		var cloneReply CloneReply

		ok = call(pb.backup, "PBServer.Clone", cloneArgs, &cloneReply)

		if !ok {
			DPrintln("Updating Backup has returned Timeout")
			reply.Err = ErrBackupTimeout
			DPrintln("%s failed Get!", pb.me)
			return errors.New(ErrBackupTimeout)
		} else {
			if cloneReply.Err == OK {
				DPrintln("%s Completed Get!", pb.me)
				return nil
			} else {
				reply.Err = cloneReply.Err
				DPrintln("Updating Backup has returned error : %s", string(reply.Err))
				DPrintln("%s failed Get!", pb.me)
				return errors.New(string(reply.Err))
			}
		}
	}
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	DPrintln("%s Started Tick...", pb.me)
	view, err := pb.vs.Ping(pb.viewNum)
	if err != nil {
		DPrintln("%s Failed Tick!", pb.me)
		return
	}
	if view.Viewnum != pb.viewNum {
		pb.mu.Lock()
		pb.viewNum = view.Viewnum
		pb.primary = view.Primary
		pb.backup = view.Backup
		DPrintln("tick: New View - ViewNum %d, Primary %s, Backup %s", pb.viewNum, pb.primary, pb.backup)
		pb.mu.Unlock()
	} else {
		DPrintln("tick: View Remains - ViewNum %d, Primary %s, Backup %s", pb.viewNum, pb.primary, pb.backup)
	}
	DPrintln("%s Completed Tick!", pb.me)
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.viewNum = 0
	pb.primary = ""
	pb.backup = ""
	pb.store = make(map[string]string)
	pb.completedOps = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
