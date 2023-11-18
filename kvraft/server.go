package kvraft

import (
	"cs651/labgob"
	"cs651/labrpc"
	"cs651/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	// "time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	RequestId int64
	ClientId  int64
}

type KVServer struct {
	mu                sync.Mutex
	mapLocker 			 sync.Mutex
	me                int
	rf                *raft.Raft
	applyCh           chan raft.ApplyMsg
	dead              int32 // set by Kill()
	CompletedRequests map[int64]map[int64]string
	totalRequest map[int64]map[int64]string

	Store        map[string]string
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) channelListener() {
	for !kv.killed() {
		msg := <-kv.applyCh
	

	if msg.Command == nil {
			continue
		}
	
		Operation := msg.Command.(Op)	


		if _, ok := kv.CompletedRequests[Operation.ClientId]; !ok {
			kv.CompletedRequests[Operation.ClientId] = make(map[int64]string)
		}
		if _, ok := kv.CompletedRequests[Operation.ClientId][Operation.RequestId]; !ok {
			switch Operation.Operation {
			case "Put":
				kv.Store[Operation.Key] = Operation.Value
			case "Append":
				kv.Store[Operation.Key] = kv.Store[Operation.Key] + Operation.Value
			}
			print("Server: ", kv.me, " Operation: ", Operation.Operation, " Key: ", Operation.Key, " Value: ", Operation.Value, " RequestId: ", Operation.RequestId, " ClientId: ", Operation.ClientId,"\n") //, " Value:", kv.Store[Operation.Key], "\n")
			kv.CompletedRequests[Operation.ClientId][Operation.RequestId] = kv.Store[Operation.Key]
		}
		
		// print("Server2: ", kv.me, " Operation: ", Operation.Operation, " Key: ", Operation.Key, " Value: ", Operation.Value, " RequestId: ", Operation.RequestId, " ClientId: ", Operation.ClientId,"\n") //, " Value:", kv.Store[Operation.Key], "\n")
			
	}
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Operation := Op{Operation: "Get", Key: args.Key, Value: "", RequestId: args.RequestId, ClientId: args.ClientId}


	// print( " Operation: ", Operation.Operation, " Key: ", Operation.Key, " Value: ", Operation.Value, " RequestId: ", Operation.RequestId, " ClientId: ", Operation.ClientId, "\n")
	if _, ok := kv.CompletedRequests[args.ClientId]; !ok {
		kv.CompletedRequests[args.ClientId] = make(map[int64]string)
	}
	
	if _, ok := kv.CompletedRequests[args.ClientId][args.RequestId]; ok {
		reply.Value = kv.CompletedRequests[args.ClientId][args.RequestId]
		reply.Err = OK
		return
	}
	_, _, isLeader := kv.rf.Start(Operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}



	reply.Err = ErrNoKey


	time.Sleep(20 * time.Millisecond)			
	if _, ok := kv.CompletedRequests[args.ClientId][args.RequestId]; ok {
		reply.Value = kv.CompletedRequests[args.ClientId][args.RequestId]
		reply.Err = OK
		return
	}

	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Operation := Op{Operation: args.Op, Key: args.Key, Value: args.Value, RequestId: args.RequestId, ClientId: args.ClientId}
	// print( " Operation: ", Operation.Operation, " Key: ", Operation.Key, " Value: ", Operation.Value, " RequestId: ", Operation.RequestId, " ClientId: ", Operation.ClientId, "\n")
	if _, ok := kv.CompletedRequests[args.ClientId]; !ok {
		kv.CompletedRequests[args.ClientId] = make(map[int64]string)
	}
	if _, ok := kv.CompletedRequests[args.ClientId][args.RequestId]; ok {

		reply.Err = OK

		return
	}
	_, _, isLeader := kv.rf.Start(Operation)
	if !isLeader {
		reply.Err = ErrWrongLeader

		return
	}


	// time.Sleep(20 * time.Millisecond)
	// if _, ok := kv.CompletedRequests[args.ClientId][args.RequestId]; ok {
	// 	reply.Err = OK
	// 	return
	// }
	// reply.Err = ErrNoKey

	// retry := 0
	reply.Err = ErrNoKey

	time.Sleep(20 * time.Millisecond)
	if _, ok := kv.CompletedRequests[args.ClientId][args.RequestId]; ok {
		reply.Err = OK
		return
	}



}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.CompletedRequests = make(map[int64]map[int64]string)
	kv.Store = make(map[string]string)

	go kv.channelListener()

	// You may need initialization code here.

	return kv
}
