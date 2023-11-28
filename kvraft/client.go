package kvraft

import (
	"crypto/rand"
	"cs651/labrpc"
	"sync"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ID          int64
	LeaderID    int
	numRequests int64
	mu 		sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ID = nrand()

	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) RetryGet(key string) (bool, string) {
	args := GetArgs{Key: key, RequestId: ck.numRequests + 1, ClientId: ck.ID}
	ck.numRequests += 1
	numRetries := 1
	for {
		if numRetries%10 == 0 || ck.LeaderID == -1 {
			for index, server := range ck.servers {
				var reply GetReply
				ok := server.Call("KVServer.Get", &args, &reply)
				if reply.Err != ErrWrongLeader {
					ck.LeaderID = index
				}
				if ok && reply.Err == OK {
					// print("Client: ", ck.ID, " Get Key: ", key, " Value: ", reply.Value, "\n")
					return ok, reply.Value
				}
			}
		} else {
			var reply GetReply
			ok := ck.servers[ck.LeaderID].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == OK {
				// print("Client: ", ck.ID, " Get Key: ", key, " Value: ", reply.Value, "\n")
				return ok, reply.Value
			}
		}
		numRetries += 1
		// time.Sleep(2 * time.Millisecond)
		time.Sleep(10 * time.Millisecond)
	}

}

func (ck *Clerk) RetryPut(key string, value string, op string) bool {
	args := PutAppendArgs{Key: key, Value: value, RequestId: ck.numRequests + 1, ClientId: ck.ID, Op: op}
	ck.numRequests += 1
	numRetries := 1
	for {
		if numRetries%10 == 0 || ck.LeaderID == -1 {
			for index, server := range ck.servers {
				var reply GetReply
				ok := server.Call("KVServer.PutAppend", &args, &reply)
				if reply.Err != ErrWrongLeader {
					ck.LeaderID = index
				}
				if ok && reply.Err == OK {
					// print("Client: ", ck.ID, " Put Key: ", key, " Value: ", reply.Value, "\n")
					return ok
				}
			}
		} else {
			var reply PutAppendReply
			ok := ck.servers[ck.LeaderID].Call("KVServer.PutAppend", &args, &reply)
			// print("Client: ", ck.ID, " Put Key: ", key, " ID:", args.RequestId, "client ID", args.ClientId, "\n")
			if ok && reply.Err == OK {
				return ok
			}
		}

		numRetries += 1
		time.Sleep(10 * time.Millisecond)
	}

}
func (ck *Clerk) Get(key string) string {

	_, value := ck.RetryGet(key)
	return value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	ck.RetryPut(key, value, op)

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
