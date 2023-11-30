package kvraft

import (
	"bytes"
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
	mapLocker         sync.Mutex
	Persist           *raft.Persister
	me                int
	rf                *raft.Raft
	applyCh           chan raft.ApplyMsg
	dead              int32 // set by Kill()
	CompletedRequests map[int64]map[int64]string

	Store        map[string]string
	maxraftstate int // snapshot if log grows this big

	LastApplied int
	// Your definitions here.
}

func (kv *KVServer) getSnapshot() []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.CompletedRequests)
	e.Encode(kv.Store)
	e.Encode(kv.LastApplied)
	data := w.Bytes()
	return data
}

func (kv *KVServer) readSnapshot(data []byte) {

	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var CompletedRequests map[int64]map[int64]string
	var Store map[string]string
	var Index  int
	if d.Decode(&CompletedRequests) != nil ||
		d.Decode(&Store) != nil || d.Decode(&Index) != nil {
		log.Fatal("Error reading persist data")
	} else {
		kv.CompletedRequests = CompletedRequests
		kv.Store = Store
		kv.LastApplied = Index
	}
}

func (kv *KVServer) checkSnapshot() {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.Persist.RaftStateSize() > kv.maxraftstate {
		index := kv.LastApplied
		print("Server: ", kv.me, " -------Snapshot: ", kv.Persist.RaftStateSize(), " Max: ", kv.maxraftstate, " index:", index, "\n")
		data := kv.getSnapshot()
		kv.rf.Snapshot(index, data)
	}
}
func (kv *KVServer) checkSnapshotListener() {

	for !kv.killed() {
		kv.mu.Lock()
		kv.mapLocker.Lock()

		kv.checkSnapshot()
		// time.Sleep(20 * time.Millisecond)
		kv.mu.Unlock()
		kv.mapLocker.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *KVServer) channelListener() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if !msg.CommandValid {
			kv.mu.Lock()
			kv.mapLocker.Lock()
			kv.readSnapshot(msg.Snapshot)
			kv.mapLocker.Unlock()
			kv.mu.Unlock()
			continue
		}

		if msg.Command == nil {
			continue
		}
		if msg.CommandIndex <= kv.LastApplied {
			continue
		}
		kv.LastApplied = msg.CommandIndex
		Operation := msg.Command.(Op)
		kv.mapLocker.Lock()

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
			print("Server: ", kv.me, " Operation: ", Operation.Operation, " Key: ", Operation.Key, " RequestId: ", Operation.RequestId, " ClientId: ", Operation.ClientId, "\n") //, " Value:", kv.Store[Operation.Key], "\n")
			kv.CompletedRequests[Operation.ClientId] = make(map[int64]string)
			kv.CompletedRequests[Operation.ClientId][Operation.RequestId] = kv.Store[Operation.Key]

		}

		kv.mapLocker.Unlock()
		// print("Server2: ", kv.me, " Operation: ", Operation.Operation, " Key: ", Operation.Key, " Value: ", Operation.Value, " RequestId: ", Operation.RequestId, " ClientId: ", Operation.ClientId,"\n") //, " Value:", kv.Store[Operation.Key], "\n")

	}
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Operation := Op{Operation: "Get", Key: args.Key, Value: "", RequestId: args.RequestId, ClientId: args.ClientId}

	// print( " Operation: ", Operation.Operation, " Key: ", Operation.Key, " Value: ", Operation.Value, " RequestId: ", Operation.RequestId, " ClientId: ", Operation.ClientId, "\n")

	kv.mapLocker.Lock()
	if _, ok := kv.CompletedRequests[args.ClientId]; !ok {
		kv.CompletedRequests[args.ClientId] = make(map[int64]string)
	}

	if _, ok := kv.CompletedRequests[args.ClientId][args.RequestId]; ok {
		reply.Value = kv.CompletedRequests[args.ClientId][args.RequestId]
		reply.Err = OK
		kv.mapLocker.Unlock()
		return
	} else {
		kv.mapLocker.Unlock()
	}

	_, _, isLeader := kv.rf.Start(Operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = ErrNoKey

	time.Sleep(20 * time.Millisecond)

	kv.mapLocker.Lock()
	if _, ok := kv.CompletedRequests[args.ClientId][args.RequestId]; ok {
		reply.Value = kv.CompletedRequests[args.ClientId][args.RequestId]
		reply.Err = OK
		kv.mapLocker.Unlock()
		return
	} else {
		kv.mapLocker.Unlock()
	}

	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Operation := Op{Operation: args.Op, Key: args.Key, Value: args.Value, RequestId: args.RequestId, ClientId: args.ClientId}
	// print( " Operation: ", Operation.Operation, " Key: ", Operation.Key, " Value: ", Operation.Value, " RequestId: ", Operation.RequestId, " ClientId: ", Operation.ClientId, "\n")

	kv.mapLocker.Lock()
	if _, ok := kv.CompletedRequests[args.ClientId]; !ok {
		kv.CompletedRequests[args.ClientId] = make(map[int64]string)
	}
	if _, ok := kv.CompletedRequests[args.ClientId][args.RequestId]; ok {

		reply.Err = OK
		kv.mapLocker.Unlock()
		return
	} else {
		kv.mapLocker.Unlock()
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

	kv.mapLocker.Lock()
	if _, ok := kv.CompletedRequests[args.ClientId][args.RequestId]; ok {
		reply.Err = OK
		kv.mapLocker.Unlock()
		return
	} else {
		kv.mapLocker.Unlock()
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
	kv.CompletedRequests = make(map[int64]map[int64]string)
	kv.Store = make(map[string]string)

	kv.Persist = persister
	kv.applyCh = make(chan raft.ApplyMsg)

	go kv.channelListener()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	print("Server: ", kv.me, " -------Reading Snapshot: ", kv.Persist.RaftStateSize(), " Max: ", kv.maxraftstate, "\n")
	kv.readSnapshot(kv.Persist.ReadSnapshot())
	kv.LastApplied = -1
	go kv.checkSnapshotListener()

	// You may need initialization code here.

	return kv
}
