package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

// WARN: may be not a good value?
const (
	lockfree = "0"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here

	// clientId is the identifier to the lock Acquirer
	clientId string
	lockKey  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
//
// NOTE: each client will call the `MakeLock`
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:      ck,
		lockKey: l,
		// each client generate a unique identifier
		clientId: kvtest.RandValue(16),
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		// 1. Get lock value from kv server
		// version will be used to implement CAS
		//
		// whatever the key exists or not, the return
		// version is the one we need in put method
		v, ver, err := lk.ck.Get(lk.lockKey)
		if v != lockfree && err != rpc.ErrNoKey {
			time.Sleep(time.Second)
			continue
		}

		// 2. set key with clientId and version
		//
		// maybe multiple goroutine write the same lock key
		// but with the protect of version,
		// there will be only one goroutine that can successfully
		// set the value
		//
		// clientId as value is aimed at
		// 1. The lock can only be released by the Acquirer
		err = lk.ck.Put(lk.lockKey, lk.clientId, ver)
		if err == rpc.OK {
			break
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	holdClient, ver, err := lk.ck.Get(lk.lockKey)
	if err == rpc.ErrNoKey || holdClient == lockfree {
		log.Print("Error: release a non-acquired key")
		return
	}

	// prevent from other client release the key
	if holdClient != lk.clientId {
		log.Print("Error: release the lock by clients who is not the lock owner")
		return
	}

	// whatever the key exists or not, the return
	// version is the one we need in put method
	//
	// we dont need to retry, because only the Acquirer can
	// come here to set the lock to free.
	err = lk.ck.Put(lk.lockKey, lockfree, ver)
	if err == rpc.ErrVersion {
		log.Print("Error: lock cannot be released")
		return
	}
}
