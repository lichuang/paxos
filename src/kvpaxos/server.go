package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import (
	"time"
)


const Debug = 0

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
	Op string
	ClientSeq int64
	Key string
	Value string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	kv   		map[string]string
	seq  		map[int64]bool
	logs 		[] Op
	lastSeq 	int
	servers		[] string
}

// helper functions
func (kv *KVPaxos) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, v := kv.px.Status(seq)
		DPrintf("%d seq %d status: %d:%d\n", kv.me, seq, status, paxos.Decided)
		if status == paxos.Decided {
			kvlog, _ := v.(Op)
			return kvlog
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos)Apply(v Op)  {
	// append logs
	if (v.Op != GET) {
		kv.logs = append(kv.logs, v)

		// modify k-v
		if (v.Op == PUT) {
			kv.kv[v.Key] = v.Value
		} else if v.Op == APPEND {
			old, err := kv.kv[v.Key]
			if (err) {
				kv.kv[v.Key] = old + v.Value
			} else {
				kv.kv[v.Key] = v.Value
			}
		}
	}
	// add client seq map
	kv.seq[v.ClientSeq] = true

	kv.px.Done(kv.lastSeq)
	kv.lastSeq += 1
}

func (kv *KVPaxos) ProcessOperation(v Op) {
	ok := false
	var log Op
	for !ok {

		seq := kv.lastSeq

		status, val := kv.px.Status(kv.lastSeq)

		if status == paxos.Decided {
			log = val.(Op)
		} else {
			kv.px.Start(seq, v)
			log = kv.wait(seq)
		}

		ok = v.ClientSeq == log.ClientSeq
		kv.Apply(log)
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var v Op = Op{Op:GET, Key:args.Key, ClientSeq:args.Seq}
	kv.ProcessOperation(v)

	val, ok := kv.kv[v.Key]
	if !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = val
	}

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, exist := kv.seq[args.Seq]
	if (exist) {
		reply.Err = OK
		return nil
	}

	appendOp := Op{Op:args.Op, ClientSeq:args.Seq, Key:args.Key, Value:args.Value}
	kv.ProcessOperation(appendOp)

	DPrintf("%d %s %s:%s done\n", kv.me, args.Op, args.Key, args.Value)
	//kv.px.Done(kv.lastSeq)

	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.kv = make(map[string] string)
	kv.seq = make(map[int64]bool)
	kv.logs = [] Op{}
	kv.lastSeq = 1
	kv.servers = servers

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
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
