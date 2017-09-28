package storageserver

import (
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)

type mutex struct {
	c chan struct{}
}

type storageServer struct {
	httpServer *http.Server
	nodesMutex sync.RWMutex
	nodes      []storagerpc.Node
	nodeInx    int
	numNodes   int
	nodeInfo   storagerpc.Node
	isMaster   bool

	// master-only
	doneBootstrap chan bool

	// storage
	storeMutex sync.RWMutex
	store      map[string]interface{}

	// grant/revoke lease
	grantLeaseLockMutex sync.RWMutex
	grantLeaseLock      map[string]*mutex // for revoke/grant lease on a key

	// lease
	leaseMapMutex sync.RWMutex
	leaseMap      map[string]map[string]int // key, <hostPort, timeout>
	lsConnMutex   sync.RWMutex
	lsConn        map[string]*rpc.Client
	ticker        *time.Ticker
}

// byNodeID is for sorting nodes based on node ID
type byNodeID []storagerpc.Node

func (t byNodeID) Len() int { return len(t) }
func (t byNodeID) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
func (t byNodeID) Less(i, j int) bool {
	return t[i].NodeID < t[j].NodeID
}

// mutex has trylock func
func newMutex() *mutex {
	return &mutex{make(chan struct{}, 1)}
}
func (m *mutex) lock() {
	m.c <- struct{}{}
}
func (m *mutex) unlock() {
	<-m.c
}
func (m *mutex) trylock() bool {
	select {
	case m.c <- struct{}{}:
		return true
	default:
		return false
	}
}

// NewStorageServer creates and starts a new StorageServer.
// masterServerHostPort is the master storage server's host:port addresrv.
// If empty, then this server is the master; otherwise, this server is a slave.
// numNodes is the total number of servers in the ring.
// port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
//
// The slave server should sleep for one second before sending
// another RegisterServer request, and this process should repeat until the master
// finally replies with an OK status indicating that the startup phase is complete.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	srv := new(storageServer)
	srv.nodeInfo = storagerpc.Node{NodeID: nodeID, HostPort: fmt.Sprintf(":%d", port)}
	srv.numNodes = numNodes
	srv.httpServer = &http.Server{Addr: srv.nodeInfo.HostPort}
	srv.store = make(map[string]interface{})
	srv.leaseMap = make(map[string]map[string]int)
	srv.grantLeaseLock = make(map[string]*mutex)
	srv.lsConn = make(map[string]*rpc.Client)
	srv.ticker = time.NewTicker(1 * time.Second)
	if err := rpc.RegisterName("StorageServer", storagerpc.Wrap(srv)); err != nil {
		return nil, err
	}
	rpc.HandleHTTP()

	if masterServerHostPort == "" {
		srv.isMaster = true
		srv.nodes = append(srv.nodes, srv.nodeInfo)
		srv.doneBootstrap = make(chan bool)
	}

	go func() {
		err := srv.httpServer.ListenAndServe()
		if err != nil {
			LOGE.Fatalf("http server start failed with error: %v", err)
		}
	}()

	go srv.leaseMaintenance()

	if srv.isMaster {
		if srv.numNodes > 1 {
			<-srv.doneBootstrap
		}
	} else {
		cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, err
		}
		defer cli.Close()

		args := storagerpc.RegisterArgs{ServerInfo: srv.nodeInfo}
		for {
			var reply storagerpc.RegisterReply
			if err = cli.Call("StorageServer.RegisterServer", args, &reply); err != nil {
				return nil, err
			}
			if reply.Status == storagerpc.OK {
				srv.nodes = reply.Servers
				break
			} else {
				time.Sleep(1 * time.Second)
			}
		}
	}

	// get node index
	srv.nodesMutex.RLock()
	defer srv.nodesMutex.RUnlock()
	for inx, n := range srv.nodes {
		if n.NodeID == srv.nodeInfo.NodeID {
			srv.nodeInx = inx
			break
		}
	}

	return srv, nil
}

// RegisterServer adds a storage server to the ring. It replies with
// status NotReady if not all nodes in the ring have joined. Once
// all nodes have joined, it should reply with status OK and a list
// of all connected nodes in the ring.
func (srv *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	srv.nodesMutex.Lock()
	defer srv.nodesMutex.Unlock()

	// if calling RegisterServer after bootstrapping
	if len(srv.nodes) == srv.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = append(reply.Servers, srv.nodes...)
		return nil
	}

	// check if already exists
	for _, n := range srv.nodes {
		if n.NodeID == args.ServerInfo.NodeID {
			reply.Status = storagerpc.NotReady
			return nil
		}
	}

	fmt.Printf("%s: %s joined\n", time.Now(), args.ServerInfo.HostPort)
	srv.nodes = append(srv.nodes, args.ServerInfo)
	// if all nodes joined
	if len(srv.nodes) == srv.numNodes {
		sort.Sort(byNodeID(srv.nodes))
		reply.Servers = append(reply.Servers, srv.nodes...)
		reply.Status = storagerpc.OK
		srv.doneBootstrap <- true
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

// GetServers retrieves a list of all connected nodes in the ring. It
// replies with status NotReady if not all nodes in the ring have joined.
func (srv *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	srv.nodesMutex.RLock()
	defer srv.nodesMutex.RUnlock()
	if len(srv.nodes) < srv.numNodes {
		reply.Status = storagerpc.NotReady
	} else {
		reply.Servers = append(reply.Servers, srv.nodes...)
		reply.Status = storagerpc.OK
	}
	return nil
}

// Get retrieves the specified key from the data store and replies with
// the key's value and a lease if one was requested. If the key does not
// fall within the storage server's range, it should reply with status
// WrongServer. If the key is not found, it should reply with status
// KeyNotFound.
func (srv *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !srv.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	srv.storeMutex.RLock()
	defer srv.storeMutex.RUnlock()
	if _, ok := srv.store[args.Key]; ok {
		if args.WantLease {
			if srv.grantLease(args.Key, args.HostPort) {
				reply.Lease = storagerpc.Lease{Granted: true, ValidSeconds: storagerpc.LeaseSeconds}
			} else {
				reply.Lease = storagerpc.Lease{Granted: false}
			}
		}
		reply.Value = srv.store[args.Key].(string)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

// GetList retrieves the specified key from the data store and replies with
// the key's list value and a lease if one was requested. If the key does not
// fall within the storage server's range, it should reply with status
// WrongServer. If the key is not found, it should reply with status
// KeyNotFound.
func (srv *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !srv.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	srv.storeMutex.RLock()
	defer srv.storeMutex.RUnlock()
	if _, ok := srv.store[args.Key]; ok {
		if args.WantLease {
			if srv.grantLease(args.Key, args.HostPort) {
				reply.Lease = storagerpc.Lease{Granted: true, ValidSeconds: storagerpc.LeaseSeconds}
			} else {
				reply.Lease = storagerpc.Lease{Granted: false}
			}
		}
		m := srv.store[args.Key].(map[string]bool)
		for k := range m {
			reply.Value = append(reply.Value, k)
		}
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

// Delete remove the specified key from the data store.
// If the key does not fall within the storage server's range,
// it should reply with status WrongServer.
// If the key is not found, it should reply with status KeyNotFound.
func (srv *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !srv.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	if err := srv.revokeLease(args.Key); err != nil {
		return err
	}

	srv.storeMutex.Lock()
	defer srv.storeMutex.Unlock()
	if _, ok := srv.store[args.Key]; ok {
		delete(srv.store, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

// Put inserts the specified key/value pair into the data store. If
// the key does not fall within the storage server's range, it should
// reply with status WrongServer.
func (srv *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !srv.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	srv.setGrantLock(args.Key)
	defer srv.unsetGrantLock(args.Key)

	if err := srv.revokeLease(args.Key); err != nil {
		return err
	}

	srv.storeMutex.Lock()
	defer srv.storeMutex.Unlock()
	srv.store[args.Key] = args.Value
	reply.Status = storagerpc.OK

	return nil
}

// AppendToList retrieves the specified key from the data store and appends
// the specified value to its list. If the key does not fall within the
// receiving server's range, it should reply with status WrongServer. If
// the specified value is already contained in the list, it should reply
// with status ItemExists.
func (srv *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !srv.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	srv.setGrantLock(args.Key)
	defer srv.unsetGrantLock(args.Key)

	if err := srv.revokeLease(args.Key); err != nil {
		return err
	}

	srv.storeMutex.Lock()
	defer srv.storeMutex.Unlock()

	if _, ok := srv.store[args.Key]; !ok {
		newList := make(map[string]bool)
		srv.store[args.Key] = newList
	}
	m := srv.store[args.Key].(map[string]bool)
	if _, ok := m[args.Value]; ok {
		reply.Status = storagerpc.ItemExists
	} else {
		m[args.Value] = true
		srv.store[args.Key] = m
		reply.Status = storagerpc.OK
	}
	return nil
}

// RemoveFromList retrieves the specified key from the data store and removes
// the specified value from its list. If the key does not fall within the
// receiving server's range, it should reply with status WrongServer. If
// the specified value is not already contained in the list, it should reply
// with status ItemNotFound.
func (srv *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !srv.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	srv.setGrantLock(args.Key)
	defer srv.unsetGrantLock(args.Key)

	if err := srv.revokeLease(args.Key); err != nil {
		return err
	}

	srv.storeMutex.Lock()
	defer srv.storeMutex.Unlock()

	m := srv.store[args.Key].(map[string]bool)
	if _, ok := m[args.Value]; ok {
		delete(m, args.Value)
		srv.store[args.Key] = m
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.ItemNotFound
	}

	return nil
}

func (srv *storageServer) Close() {
	srv.httpServer.Close()
	srv.lsConnMutex.Lock()
	defer srv.lsConnMutex.Unlock()
	for _, cli := range srv.lsConn {
		cli.Close()
	}
}

func (srv *storageServer) leaseMaintenance() {
	for range srv.ticker.C {
		srv.leaseMapMutex.Lock()
		for _, v := range srv.leaseMap {
			for hostPort, timeout := range v {
				if timeout <= 1 {
					delete(v, hostPort)
					continue
				}
				v[hostPort]--
			}
		}
		srv.leaseMapMutex.Unlock()
	}
}

// key hash should be smaller than the current nodeID, but larger than the pervious one in the node ring
// if it's larger than the max nodeID, then it should be served by the smallest nodeID
func (srv *storageServer) checkKeyValid(key string) bool {
	hash := libstore.StoreHash(key)
	srv.nodesMutex.RLock()
	defer srv.nodesMutex.RUnlock()
	if srv.nodeInx == 0 {
		// if larger than the max node ID
		if hash > srv.nodes[srv.numNodes-1].NodeID {
			return true
		}
		if hash <= srv.nodeInfo.NodeID {
			return true
		}
		return false
	}
	if hash > srv.nodes[srv.nodeInx-1].NodeID && hash <= srv.nodeInfo.NodeID {
		return true
	}
	return false
}

func (srv *storageServer) setGrantLock(key string) {
	srv.grantLeaseLockMutex.Lock()
	if _, ok := srv.grantLeaseLock[key]; !ok {
		srv.grantLeaseLock[key] = newMutex()
	}
	keyLock := srv.grantLeaseLock[key]
	srv.grantLeaseLockMutex.Unlock()

	keyLock.lock()
}

func (srv *storageServer) unsetGrantLock(key string) {
	srv.grantLeaseLockMutex.Lock()
	keyLock := srv.grantLeaseLock[key]
	srv.grantLeaseLockMutex.Unlock()
	keyLock.unlock()
}

func (srv *storageServer) tryGrantLock(key string) bool {
	srv.grantLeaseLockMutex.Lock()
	if _, ok := srv.grantLeaseLock[key]; !ok {
		srv.grantLeaseLock[key] = newMutex()
	}
	keyLock := srv.grantLeaseLock[key]
	srv.grantLeaseLockMutex.Unlock()
	return keyLock.trylock()
}

func (srv *storageServer) grantLease(key, hostPort string) bool {
	if !srv.tryGrantLock(key) {
		return false
	}
	defer srv.unsetGrantLock(key)
	srv.leaseMapMutex.Lock()
	defer srv.leaseMapMutex.Unlock()
	if _, ok := srv.leaseMap[key]; !ok {
		srv.leaseMap[key] = make(map[string]int)
	}
	srv.leaseMap[key][hostPort] = storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
	return true
}

func (srv *storageServer) revokeLease(key string) error {
	srv.leaseMapMutex.RLock()
	srv.lsConnMutex.Lock()
	var wg sync.WaitGroup
	for hostPort := range srv.leaseMap[key] {
		wg.Add(1)
		if _, ok := srv.lsConn[hostPort]; !ok {
			cli, err := rpc.DialHTTP("tcp", hostPort)
			if err != nil {
				return err
			}
			srv.lsConn[hostPort] = cli
		}

		cli := srv.lsConn[hostPort]
		go func(key, hostPort string, cli *rpc.Client, wg *sync.WaitGroup) {
			defer wg.Done()

			// either revoke lease replies without error or lease timed out
			done := make(chan struct{})
			go func(done chan struct{}) {
				ticker := time.NewTicker(1 * time.Second)
				for range ticker.C {
					srv.leaseMapMutex.RLock()
					_, ok := srv.leaseMap[key][hostPort]
					srv.leaseMapMutex.RUnlock()
					if !ok {
						done <- struct{}{}
					}
				}
			}(done)
			go func(done chan struct{}) {
				args := storagerpc.RevokeLeaseArgs{Key: key}
				var reply storagerpc.RevokeLeaseReply
				if err := cli.Call("LeaseCallbacks.RevokeLease", args, &reply); err == nil {
					// if no err, reply returns either OK or KeyNotFound
					done <- struct{}{}
				}
			}(done)

			<-done
		}(key, hostPort, cli, &wg)
	}
	srv.lsConnMutex.Unlock()
	srv.leaseMapMutex.RUnlock()

	wg.Wait()
	srv.leaseMapMutex.Lock()
	delete(srv.leaseMap, key)
	srv.leaseMapMutex.Unlock()
	return nil

}
