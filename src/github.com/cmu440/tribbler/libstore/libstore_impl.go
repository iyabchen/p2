package libstore

import (
	"fmt"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"sync"

	"log"
	"net/rpc"
	"os"
	"time"
)

var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)

type cacheEntry struct {
	content      interface{}
	validSeconds int
}

type libstore struct {
	hostport          string
	mode              LeaseMode // never, normal, always
	storageServerList []storagerpc.Node
	connMap           map[storagerpc.Node]*rpc.Client
	cacheMutex        sync.RWMutex
	cache             map[string]*cacheEntry // key, content with lease
	queryHistoryMutex sync.RWMutex
	queryHistory      map[string][]int64 // key, query time
	ticker            *time.Ticker
}

// NewLibstore creates a new instance of a TribServer's libstore.
// masterServerHostPort is the master storage server's host:port.
// myHostPort is this Libstore's host:port (i.e. the callback address that the
// storage servers should use to send back notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {

	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	defer cli.Close()
	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply
	retryAttempts := 5

	for i := 0; i < retryAttempts; i++ {
		if err := cli.Call("StorageServer.GetServers", args, &reply); err != nil {
			return nil, err
		}
		if reply.Status == storagerpc.OK {
			break
		} else if reply.Status == storagerpc.NotReady {
			if i == 4 {
				return nil, fmt.Errorf("Tried %d times, and servers are still not ready", retryAttempts)
			}
			time.Sleep(1 * time.Second)
		} else {
			return nil, fmt.Errorf("NewLibstore: Calling StorageServer.GetServers returns abnormal status: %d", reply.Status)
		}
	}

	ls := libstore{}
	ls.hostport = myHostPort
	ls.storageServerList = reply.Servers
	ls.connMap = make(map[storagerpc.Node]*rpc.Client, len(ls.storageServerList))
	ls.mode = mode
	ls.cache = make(map[string]*cacheEntry)
	ls.queryHistory = make(map[string][]int64)
	ls.ticker = time.NewTicker(1 * time.Second)
	if err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(&ls)); err != nil {
		return nil, err
	}

	go ls.cacheMaintenance()

	return &ls, nil
}

func (ls *libstore) cacheMaintenance() {
	for range ls.ticker.C {
		ls.cacheMutex.Lock()
		for k, v := range ls.cache {
			timeout := v.validSeconds
			if timeout <= 1 {
				delete(ls.cache, k)
				continue
			}
			ls.cache[k].validSeconds--
		}
		ls.cacheMutex.Unlock()
	}
}

// for a given key, the node that handles it is selected by generating a hash
// of the key and finding the "successor" of this value
func (ls *libstore) schedule(key string) (*rpc.Client, error) {
	// binary search
	hash := StoreHash(key)
	start := 0
	end := len(ls.storageServerList) - 1
	res := -1
	for start < end {
		mid := start + (end-start)/2
		if ls.storageServerList[mid].NodeID < hash {
			start = mid + 1
		} else if ls.storageServerList[mid].NodeID > hash {
			end = mid
		} else {
			res = mid
			break
		}
	}
	if res == -1 {
		res = end
	}
	if ls.storageServerList[res].NodeID < hash {
		res = 0
	}

	server := ls.storageServerList[res]
	if ls.connMap[server] == nil {
		cli, err := rpc.DialHTTP("tcp", server.HostPort)
		if err != nil {
			return nil, err
		}
		ls.connMap[server] = cli
	}
	return ls.connMap[server], nil
}

func (ls *libstore) searchCache(key string) (interface{}, error) {
	ls.cacheMutex.Lock()
	defer ls.cacheMutex.Unlock()

	ce, ok := ls.cache[key]
	if !ok {
		return nil, fmt.Errorf("Not found in cache/cache expired")
	}
	return ce.content, nil

}

func (ls *libstore) isFrequentQuery(key string) bool {
	ls.queryHistoryMutex.Lock()
	defer ls.queryHistoryMutex.Unlock()
	now := time.Now().Unix()
	ls.queryHistory[key] = append(ls.queryHistory[key], now)
	list := ls.queryHistory[key]
	if len(list) > storagerpc.QueryCacheThresh {
		earlist := list[0]
		ls.queryHistory[key] = ls.queryHistory[key][1:]
		if now-earlist <=
			storagerpc.QueryCacheSeconds {
			return true
		}
	}
	return false
}

func (ls *libstore) Get(key string) (string, error) {

	freqQuery := ls.isFrequentQuery(key)
	if res, err := ls.searchCache(key); err == nil {
		return res.(string), nil
	}

	cli, err := ls.schedule(key)
	if err != nil {
		LOGE.Printf("Failed to get storage server connection: %s", err)
		return "", err
	}

	wantLease := false
	if freqQuery && ls.mode == Normal || ls.mode == Always {
		wantLease = true
	}

	args := storagerpc.GetArgs{Key: key, WantLease: wantLease, HostPort: ls.hostport}
	var reply storagerpc.GetReply

	if err = cli.Call("StorageServer.Get", args, &reply); err != nil {
		LOGE.Printf("rpc call error to StorageServer.Get: %s", err)
		return "", err
	}
	if reply.Status == storagerpc.OK {
		if wantLease && reply.Lease.Granted {
			ls.cacheMutex.Lock()
			defer ls.cacheMutex.Unlock()
			ls.cache[key] = &cacheEntry{content: reply.Value, validSeconds: reply.Lease.ValidSeconds}
		}
		return reply.Value, nil
	} else if reply.Status == storagerpc.KeyNotFound {
		return "", fmt.Errorf("libstore.Get: key %s not found", key)
	}

	LOGE.Printf("StorageServer.Get return unexpected status: %s", reply.Status)
	return "", fmt.Errorf("Libstore.Get: StorageServer.Get return unexpected status %s", reply.Status)
}

func (ls *libstore) Put(key, value string) error {
	cli, err := ls.schedule(key)
	if err != nil {
		LOGE.Printf("Failed to get storage server connection: %s", err)
		return err
	}

	args := storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply

	if err = cli.Call("StorageServer.Put", args, &reply); err != nil {
		LOGE.Printf("rpc call error to StorageServer.Put: %s", err)
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	}
	LOGE.Printf("StorageServer.Put return unexpected status: %s", reply.Status)
	return fmt.Errorf("Libstore.Put: StorageServer.Put return unexpected status: %s", reply.Status)
}

func (ls *libstore) Delete(key string) error {
	cli, err := ls.schedule(key)
	if err != nil {
		LOGE.Printf("Failed to get storage server connection: %s", err)
		return err
	}

	args := storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply

	if err = cli.Call("StorageServer.Delete", args, &reply); err != nil {
		LOGE.Printf("rpc call error to StorageServer.Delete: %s", err)
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	} else if reply.Status == storagerpc.KeyNotFound {
		return fmt.Errorf("Libstore.Delete: key %s not found", key)
	}
	LOGE.Printf("StorageServer.Delete return unexpected status: %s", reply.Status)
	return fmt.Errorf("Libstore.Delete: StorageServer.Delete return unexpected status: %s", reply.Status)
}

func (ls *libstore) GetList(key string) ([]string, error) {

	freqQuery := ls.isFrequentQuery(key)
	if res, err := ls.searchCache(key); err == nil {
		return res.([]string), nil
	}

	cli, err := ls.schedule(key)
	if err != nil {
		LOGE.Printf("Failed to get storage server connection: %s", err)
		return nil, err
	}

	wantLease := false
	if freqQuery && ls.mode == Normal || ls.mode == Always {
		wantLease = true
	}

	args := storagerpc.GetArgs{Key: key, WantLease: wantLease, HostPort: ls.hostport}
	var reply storagerpc.GetListReply
	if err = cli.Call("StorageServer.GetList", args, &reply); err != nil {
		LOGE.Printf("rpc call error to StorageServer.GetList: %s", err)
		return nil, err
	}
	if reply.Status == storagerpc.OK {
		if wantLease && reply.Lease.Granted {
			ls.cacheMutex.Lock()
			defer ls.cacheMutex.Unlock()
			ls.cache[key] = &cacheEntry{content: reply.Value, validSeconds: reply.Lease.ValidSeconds}
		}
		return reply.Value, nil
	} else if reply.Status == storagerpc.KeyNotFound {
		return nil, fmt.Errorf("Libstore.GetList: key %s not found", key)
	}
	LOGE.Printf("StorageServer.GetList return unexpected status: %s", reply.Status)
	return nil, fmt.Errorf("libstore.GetList:StorageServer.GetList return unexpected status: %s", reply.Status)
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	cli, err := ls.schedule(key)
	if err != nil {
		LOGE.Printf("Failed to get storage server connection: %s", err)
		return err
	}

	args := storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply
	if err = cli.Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		LOGE.Printf("rpc error in StorageServer.RemoveFromList: %s", err)
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	} else if reply.Status == storagerpc.ItemNotFound {
		return fmt.Errorf("Libstore.RemoveFromList: item %s not found in %s", removeItem, key)
	}

	LOGE.Printf("StorageServer.RemoveFromList return unexpected status: %s", reply.Status)
	return fmt.Errorf("LibStorage.RemoveFromList: StorageServer.RemoveFromList return unexpected status: %s", reply.Status)
}

func (ls *libstore) AppendToList(key, newItem string) error {
	cli, err := ls.schedule(key)
	if err != nil {
		LOGE.Printf("Failed to get storage server connection: %s", err)
		return err
	}

	args := storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply
	if err = cli.Call("StorageServer.AppendToList", args, &reply); err != nil {
		LOGE.Printf("rpc error in StorageServer.AppendToList: %s", err)
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	} else if reply.Status == storagerpc.ItemExists {
		return fmt.Errorf("Libstore.AppendToList: item %s exists for list %s", newItem, key)
	}
	LOGE.Printf("StorageServer.AppendToList return unexpected status: %s", reply.Status)

	return fmt.Errorf("LibStore.AppendToList: StorageServer.AppendToList return unexpected status: %s", reply.Status)
}

// RevokeLease is a callback RPC method that is invoked by storage
// servers when a lease is revoked. It should reply with status OK
// if the key was successfully revoked, or with status KeyNotFound
// if the key did not exist in the cache.
func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.cacheMutex.Lock()
	defer ls.cacheMutex.Unlock()

	if _, ok := ls.cache[args.Key]; !ok {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}

	delete(ls.cache, args.Key)
	reply.Status = storagerpc.OK
	return nil
}
