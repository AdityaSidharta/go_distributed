package viewservice

import (
	"net"
)
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type Health string
type Role string
type ViewType string

const (
	Dead   Health = "Dead"
	Alive  Health = "Alive"
	Reboot Health = "Reboot"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	currentNum     uint
	currentPrimary string
	currentBackup  string
	currentAck     bool
	views          []View
	serverPing     map[string]time.Time
	serverHealth   map[string]Health
}

func (vs *ViewServer) enableServer(server string) {
	if vs.serverHealth[server] == Reboot {
		DPrintln("enableServer: Enabling server %s from Reboot to Alive", server)
		vs.serverHealth[server] = Alive
		return
	}
}

func (vs *ViewServer) killServer(server string) {
	health, ok := vs.serverHealth[server]
	if !ok {
		DPrintln("killServer: server %s is not defined within serverHealth", server)
		return
	}
	switch health {
	case Dead:
		DPrintln("killServer: server %s remains Dead", server)
		return
	case Alive:
		DPrintln("killServer: Killing server %s from Alive to Dead", server)
		vs.serverHealth[server] = Dead
		return
	case Reboot:
		DPrintln("killServer: Killing server %s from Reboot to Dead", server)
		vs.serverHealth[server] = Dead
		return
	default:
		log.Fatalf("Undefined Health for server %s : %s", server, health)
	}
}

func (vs *ViewServer) reviveServer(server string) {
	health, ok := vs.serverHealth[server]
	if !ok {
		DPrintln("reviveServer: adding server %s into serverHealth. ", server)
		vs.serverHealth[server] = Alive
		return
	}
	switch health {
	case Alive:
		DPrintln("reviveServer: server %s remains on Alive", server)
	case Dead:
		if server == vs.currentPrimary {
			DPrintln("reviveServer: %s is Primary. reviving it from Dead to Reboot", server)
			vs.serverHealth[server] = Reboot
			return
		} else if server == vs.currentBackup {
			DPrintln("reviveServer: %s is Backup. reviving it from Dead to Reboot", server)
			vs.serverHealth[server] = Reboot
		} else {
			DPrintln("reviveServer: %s is Nothing. reviving it from Dead to Alive", server)
			vs.serverHealth[server] = Alive
		}
	case Reboot:
		DPrintln("reviveServer: server %s remains on Reboot", server)
	default:
		log.Fatalf("Undefined Health for server %s : %s", server, health)
	}
}

func (vs *ViewServer) getPrimaryBackup() (primaryServer string, backupServer string) {
	primaryServer = ""
	backupServer = ""
	if (vs.currentPrimary == "") && (vs.currentBackup == "") {
		DPrintln("getPrimaryBackup: empty primary and backup")
		for server, health := range vs.serverHealth {
			if health == Alive {
				DPrintln("Setting server %s as Primary", server)
				primaryServer = server
				break
			}
		}
		for server, health := range vs.serverHealth {
			if (health == Alive) && (server != primaryServer) {
				DPrintln("Setting server %s as Backup", server)
				backupServer = server
				break
			}
		}
	} else if (vs.currentPrimary != "") && (vs.currentBackup == "") {
		DPrintln("getPrimaryBackup: empty backup")
		if vs.serverHealth[vs.currentPrimary] == Alive {
			primaryServer = vs.currentPrimary
			for server, health := range vs.serverHealth {
				if (health == Alive) && (server != vs.currentPrimary) {
					DPrintln("Setting server %s as Backup", server)
					backupServer = server
					break
				}
			}
		} else {
			log.Fatalf("getPrimaryBackup: Primary Died, but no Backup Detected")
		}
	} else if (vs.currentPrimary == "") && (vs.currentBackup != "") {
		log.Fatalf("getPrimaryBackup: vs.currentPrimary is nil but currentBackup is not nil")
	} else {
		DPrintln("getPrimaryBackup: primary and backup filled")
		if (vs.serverHealth[vs.currentPrimary] == Alive) && (vs.serverHealth[vs.currentBackup] == Alive) {
			DPrintln("getPrimaryBackup: primary and backup is healthy")
			primaryServer = vs.currentPrimary
			backupServer = vs.currentBackup
		} else if vs.serverHealth[vs.currentPrimary] == Alive {
			DPrintln("getPrimaryBackup: primary healthy, backup dead")
			primaryServer = vs.currentPrimary
			for server, health := range vs.serverHealth {
				if (health == Alive) && (server != vs.currentPrimary) && (server != vs.currentBackup) {
					DPrintln("Setting server %s as Backup", server)
					backupServer = server
					break
				}
			}
		} else if vs.serverHealth[vs.currentBackup] == Alive {
			DPrintln("getPrimaryBackup: primary dead, backup healthy")
			primaryServer = vs.currentBackup
			for server, health := range vs.serverHealth {
				if (health == Alive) && (server != vs.currentPrimary) && (server != vs.currentBackup) {
					DPrintln("Setting server %s as Backup", server)
					backupServer = server
					break
				}
			}
		} else {
			DPrintln("getPrimaryBackup: [WARNING] vs.currentPrimary is dead and currentBackup is also dead")
			primaryServer = vs.currentPrimary
			backupServer = vs.currentBackup
		}
	}
	return primaryServer, backupServer
}

func (vs *ViewServer) updateRoles() {
	primaryServer, backupServer := vs.getPrimaryBackup()
	DPrintln("updateRoles : primaryServer : %s, backupServer : %s, vs.currentPrimary : %s, vs.currentBackup : %s", primaryServer, backupServer, vs.currentPrimary, vs.currentBackup)
	if (primaryServer == vs.currentPrimary) && (backupServer == vs.currentBackup) {
		DPrintln("updateRoles : No Changes in the Primary & Backup")
		return
	} else {
		DPrintln("updateRoles : New Primary or Backup Detected")
		previousView := View{
			vs.currentNum,
			vs.currentPrimary,
			vs.currentBackup,
		}
		vs.currentNum = vs.currentNum + 1
		vs.currentPrimary = primaryServer
		vs.currentBackup = backupServer
		DPrintln("updateRoles : Previous View : %s", previousView)
		DPrintln("updateRoles : vs.currentNum : %s", vs.currentNum)
		DPrintln("updateRoles : vs.currentPrimary : %s", vs.currentPrimary)
		DPrintln("updateRoles : vs.currentBackup : %s", vs.currentBackup)

		vs.currentAck = false
		vs.views = append(vs.views, previousView)
		for server := range vs.serverHealth {
			vs.enableServer(server)
		}
		return
	}
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	currentTime := time.Now()
	vs.mu.Lock()
	defer vs.mu.Unlock()

	server := args.Me
	viewNum := args.Viewnum
	DPrintln("Ping: server %s, viewNum %d", server, viewNum)

	if viewNum == 0 {
		DPrintln("Ping: ViewNum 0, kill and reboot %s", server)
		vs.killServer(server)
		vs.reviveServer(server)
	}

	if (server == vs.currentPrimary) && (viewNum == vs.currentNum) {
		DPrintln("Ping: vs.CurrentNum %d acknowledged by %s", vs.currentNum, server)
		vs.currentAck = true
	}

	vs.serverPing[server] = currentTime

	if vs.currentAck {
		vs.updateRoles()
	}

	reply.View = View{
		vs.currentNum,
		vs.currentPrimary,
		vs.currentBackup,
	}
	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	reply.View = View{
		vs.currentNum,
		vs.currentPrimary,
		vs.currentBackup,
	}
	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	currentTime := time.Now()
	vs.mu.Lock()
	defer vs.mu.Unlock()
	for server, time := range vs.serverPing {
		if currentTime.Sub(time) > (DeadPings * PingInterval) {
			vs.killServer(server)
		} else {
			vs.reviveServer(server)
		}
	}
}

// tell the server to shut itself down.
// for testing.
// please don't change this function.
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)
	vs.currentNum = 0
	vs.currentPrimary = ""
	vs.currentBackup = ""
	vs.currentAck = true
	vs.views = make([]View, 10)
	vs.serverPing = make(map[string]time.Time)
	vs.serverHealth = make(map[string]Health)
	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
