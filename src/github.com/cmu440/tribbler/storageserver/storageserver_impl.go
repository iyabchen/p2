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

type role int
type status int

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
}

type ByNodeID []storagerpc.Node

func (t ByNodeID) Len() int { return len(t) }
func (t ByNodeID) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
func (t ByNodeID) Less(i, j int) bool {
	return t[i].NodeID < t[j].NodeID
}

// NewStorageServer creates and starts a new StorageServer.
// masterServerHostPort is the master storage server's host:port address.
// If empty, then this server is the master; otherwise, this server is a slave.
// numNodes is the total number of servers in the ring.
// port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.

//The slave server should sleep for one second before sending
// another RegisterServer request, and this process should repeat until the master
// finally replies with an OK status indicating that the startup phase is complete.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	srv := new(storageServer)
	srv.nodeInfo = storagerpc.Node{NodeID: nodeID, HostPort: fmt.Sprintf(":%d", port)}
	srv.numNodes = numNodes
	srv.httpServer = &http.Server{Addr: srv.nodeInfo.HostPort}
	srv.store = make(map[string]interface{})

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

	if srv.isMaster {
		<-srv.doneBootstrap
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
func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.nodesMutex.Lock()
	defer ss.nodesMutex.Unlock()

	// if calling RegisterServer after bootstrapping
	if len(ss.nodes) == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = append(reply.Servers, ss.nodes...)
		return nil
	}

	// check if already exists
	for _, n := range ss.nodes {
		if n.NodeID == args.ServerInfo.NodeID {
			reply.Status = storagerpc.NotReady
			return nil
		}
	}

	ss.nodes = append(ss.nodes, args.ServerInfo)
	// if all nodes joined
	if len(ss.nodes) == ss.numNodes {
		sort.Sort(ByNodeID(ss.nodes))
		reply.Servers = append(reply.Servers, ss.nodes...)
		reply.Status = storagerpc.OK
		ss.doneBootstrap <- true
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

// GetServers retrieves a list of all connected nodes in the ring. It
// replies with status NotReady if not all nodes in the ring have joined.
func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.nodesMutex.RLock()
	defer ss.nodesMutex.RUnlock()
	if len(ss.nodes) < ss.numNodes {
		reply.Status = storagerpc.NotReady
	} else {
		reply.Servers = append(reply.Servers, ss.nodes...)
		reply.Status = storagerpc.OK
	}
	return nil
}

// key hash should be smaller than the current nodeID, but larger than the pervious one in the node ring
// if it's larger than the max nodeID, then it should be served by the smallest nodeID
func (ss *storageServer) checkKeyValid(key string) bool {
	hash := libstore.StoreHash(key)
	ss.nodesMutex.RLock()
	defer ss.nodesMutex.RUnlock()
	if ss.nodeInx == 0 {
		if hash > ss.nodes[ss.numNodes-1].NodeID {
			return true
		}
		if hash < ss.nodeInfo.NodeID {
			return true
		}
		return false
	}
	if hash > ss.nodes[ss.nodeInx-1].NodeID && hash < ss.nodeInfo.NodeID {
		return true
	}
	return false
}

// Get retrieves the specified key from the data store and replies with
// the key's value and a lease if one was requested. If the key does not
// fall within the storage server's range, it should reply with status
// WrongServer. If the key is not found, it should reply with status
// KeyNotFound.
func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storeMutex.RLock()
	defer ss.storeMutex.RUnlock()
	if _, ok := ss.store[args.Key]; ok {
		reply.Value = ss.store[args.Key].(string)
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
func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storeMutex.Lock()
	defer ss.storeMutex.Unlock()
	if _, ok := ss.store[args.Key]; ok {
		delete(ss.store, args.Key)
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
func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.storeMutex.RLock()
	defer ss.storeMutex.RUnlock()
	if _, ok := ss.store[args.Key]; ok {
		m := ss.store[args.Key].(map[string]bool)
		for k := range m {
			reply.Value = append(reply.Value, k)
		}
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

// Put inserts the specified key/value pair into the data store. If
// the key does not fall within the storage server's range, it should
// reply with status WrongServer.
func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storeMutex.Lock()
	defer ss.storeMutex.Unlock()
	ss.store[args.Key] = args.Value
	reply.Status = storagerpc.OK

	return nil
}

// AppendToList retrieves the specified key from the data store and appends
// the specified value to its list. If the key does not fall within the
// receiving server's range, it should reply with status WrongServer. If
// the specified value is already contained in the list, it should reply
// with status ItemExists.
func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storeMutex.Lock()
	defer ss.storeMutex.Unlock()

	if _, ok := ss.store[args.Key]; !ok {
		newList := make(map[string]bool)
		ss.store[args.Key] = newList
	}
	m := ss.store[args.Key].(map[string]bool)
	if _, ok := m[args.Value]; ok {
		reply.Status = storagerpc.ItemExists
	} else {
		m[args.Value] = true
		ss.store[args.Key] = m
		reply.Status = storagerpc.OK
	}
	return nil
}

// RemoveFromList retrieves the specified key from the data store and removes
// the specified value from its list. If the key does not fall within the
// receiving server's range, it should reply with status WrongServer. If
// the specified value is not already contained in the list, it should reply
// with status ItemNotFound.
func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storeMutex.Lock()
	defer ss.storeMutex.Unlock()

	m := ss.store[args.Key].(map[string]bool)
	if _, ok := m[args.Value]; ok {
		delete(m, args.Value)
		ss.store[args.Key] = m
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.ItemNotFound
	}

	return nil
}
