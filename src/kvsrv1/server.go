package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type vType struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	m map[string]vType
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.m = make(map[string]vType)
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	// log.Printf("KVServer: Get request comes %v", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.m[args.Key]; ok {
		reply.Value = v.value
		reply.Version = v.version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	// log.Printf("KVServer: Put request comes %v", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.m[args.Key]; ok {
		if v.version == args.Version {
			kv.m[args.Key] = vType{args.Value, args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			kv.m[args.Key] = vType{args.Value, 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
