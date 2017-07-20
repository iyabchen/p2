package storageserver

import (
	"errors"
	"fmt"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type role int
type status int

type storageServer struct {
	// TODO: implement this!
	isMaster      bool
	activeNodes   nodeList
	masterHost    string
	masterPort    int
	numNodes      int
	listenPort    int
	nodeID        uint32
	joinNode      chan storagerpc.Node
	joinNodeReply chan storagerpc.Status
	userMap       resource
	userFriendMap resourceList
	userSubMap    resourceList
	userTribMap   resourceList
	tribs         resource
	prevNodeID    uint32
}

type resourceList struct {
	sync.RWMutex
	content map[string][]string
}

type resource struct {
	sync.RWMutex
	content map[string]string
}

type nodeList struct {
	sync.RWMutex
	nodes []storagerpc.Node
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	srv := new(storageServer)
	srv.userMap = resource{content: make(map[string]string)}
	srv.userSubMap = resourceList{content: make(map[string][]string)}
	srv.userFriendMap = resourceList{content: make(map[string][]string)}
	srv.userTribMap = resourceList{content: make(map[string][]string)}
	srv.tribs = resource{content: make(map[string]string)}

	if masterServerHostPort == "" {
		srv.isMaster = true
		srv.numNodes = numNodes
		srv.masterPort = port
		srv.joinNode = make(chan storagerpc.Node)

	}
	srv.listenPort = port
	srv.nodeID = nodeID

	if !srv.isMaster {
		cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, err
		}
		nodeInfo := storagerpc.Node{NodeID: nodeID, HostPort: fmt.Sprintf(":%d", port)}
		args := storagerpc.RegisterArgs{nodeInfo}
		var reply storagerpc.RegisterReply

		for {
			if err = cli.Call("StorageServer.RegisterServer", args, &reply); err != nil {
				return nil, err
			}
			if reply.Status == storagerpc.OK {
				break
			} else {
				time.Sleep(1 * time.Second)
			}
		}
		srv.activeNodes.nodes = reply.Servers
		for ix, n := range srv.activeNodes.nodes {
			if n.NodeID == srv.nodeID {
				if ix == 0 {
					srv.prevNodeID = 0
				} else {
					srv.prevNodeID = srv.activeNodes.nodes[ix-1].NodeID
				}

			}
		}
		return srv, nil
	} else { // if master, then listen to RPC calls, until all joined
		srv.activeNodes.Lock()

		srv.activeNodes = nodeList{nodes: make([]storagerpc.Node, 1)}
		srv.activeNodes.nodes[0] = storagerpc.Node{
			HostPort: srv.masterHost,
			NodeID:   srv.nodeID}
		srv.activeNodes.Unlock()

		if err := rpc.Register(&srv); err != nil {
			return nil, err
		}
		rpc.HandleHTTP()
		go func() {
			err := http.ListenAndServe(":"+strconv.Itoa(srv.listenPort), nil)
			if err != nil {
				fmt.Errorf("http server start failed with error: %v", err)
			}
		}()

		for {
			newNode := <-srv.joinNode
			srv.activeNodes.Lock()
			// insert based on nodeID
			srv.activeNodes.nodes = append(srv.activeNodes.nodes, newNode)
			for ix, n := range srv.activeNodes.nodes {
				if newNode.NodeID < n.NodeID {
					copy(srv.activeNodes.nodes[ix+1:], srv.activeNodes.nodes[ix:])
					srv.activeNodes.nodes[ix] = newNode
					break
				}
			}
			if len(srv.activeNodes.nodes) == srv.numNodes {
				srv.activeNodes.Unlock()
				break
			}
			status := storagerpc.NotReady
			if len(srv.activeNodes.nodes) == srv.numNodes {
				status = storagerpc.OK
			}
			srv.activeNodes.Unlock()
			srv.joinNodeReply <- status
			if status == storagerpc.OK {
				break
			}

		}
		return srv, nil
	}
}

func (ss *storageServer) getActiveNodes() []storagerpc.Node {
	ss.activeNodes.RLock()
	defer ss.activeNodes.RUnlock()

	ret := make([]storagerpc.Node, len(ss.activeNodes.nodes))
	copy(ss.activeNodes.nodes, ret)
	return ret
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.joinNode <- args.ServerInfo
	reply.Status = <-ss.joinNodeReply
	reply.Servers = ss.getActiveNodes()
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.activeNodes.RLock()
	defer ss.activeNodes.RUnlock()

	if len(ss.activeNodes.nodes) < ss.numNodes {
		reply.Status = storagerpc.NotReady
	} else {
		reply.Status = storagerpc.OK
	}
	reply.Servers = ss.getActiveNodes()

	return nil
}

func (ss *storageServer) checkKeyValid(key string) bool {
	hash := libstore.StoreHash(key)

	if hash > ss.prevNodeID && hash <= ss.nodeID {
		return true
	}
	return false
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	keyParts := strings.Split(args.Key, ":")
	usrID := keyParts[0]
	key := keyParts[1]

	switch {
	case key == "usrid":
		ss.userMap.RLock()
		if _, ok := ss.userMap.content[usrID]; ok {
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
		ss.userMap.RUnlock()

	case strings.HasPrefix(key, "post_"):
		ss.tribs.RLock()
		if val, ok := ss.tribs.content[key]; ok {
			reply.Value = val
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
		ss.tribs.RUnlock()

	default:
		return fmt.Errorf("get does not accept key %s", key)
	}

	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	keyParts := strings.Split(args.Key, ":")
	// usrID := keyParts[0]
	key := keyParts[1]

	switch {
	case key == "usrid":
		return fmt.Errorf("Cannot remove a user")

	case strings.HasPrefix(key, "post_"):
		ss.tribs.Lock()
		if _, ok := ss.tribs.content[key]; ok {
			delete(ss.tribs.content, key)
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
		ss.tribs.RUnlock()

	default:
		return fmt.Errorf("get does not accept key %s", key)
	}

	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	keyParts := strings.Split(args.Key, ":")
	usrID := keyParts[0]
	key := keyParts[1]
	switch {
	case key == "sublist":
		ss.userSubMap.RLock()
		if subs, ok := ss.userSubMap.content[usrID]; ok {
			reply.Status = storagerpc.OK
			for _, sub := range subs {
				ss.userTribMap.RLock()
				tribs := ss.userTribMap.content[sub]
				reply.Value = append(reply.Value, tribs...)
				ss.userTribMap.RUnlock()
			}
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
		ss.userSubMap.RUnlock()

	case key == "triblist":
		ss.tribs.RLock()
		if val, ok := ss.userTribMap.content[usrID]; ok {
			reply.Value = val
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
		ss.tribs.RUnlock()

	case key == "friendlist":
		ss.userFriendMap.RLock()
		if val, ok := ss.userFriendMap.content[usrID]; ok {
			reply.Value = val
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
		ss.tribs.RUnlock()

	default:
		return fmt.Errorf("get does not accept key %s", key)
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	keyParts := strings.Split(args.Key, ":")
	usrID := keyParts[0]
	key := keyParts[1]

	switch {
	case key == "usrid":
		ss.userMap.Lock()
		ss.userMap.content[usrID] = "true"
		ss.userMap.Unlock()

	case strings.HasPrefix(key, "post_"):
		ss.tribs.Lock()
		ss.tribs.content[key] = args.Value
		ss.tribs.RUnlock()

	default:
		return fmt.Errorf("put does not accept key %s", key)
	}

	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	keyParts := strings.Split(args.Key, ":")
	usrID := keyParts[0]
	key := keyParts[1]
	switch {
	case key == "sublist":
		ss.userSubMap.RLock()
		if subs, ok := ss.userSubMap.content[usrID]; ok {
			reply.Status = storagerpc.OK
			for _, sub := range subs {
				ss.userTribMap.RLock()
				tribs := ss.userTribMap.content[sub]
				reply.Value = append(reply.Value, tribs...)
				ss.userTribMap.RUnlock()
			}
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
		ss.userSubMap.RUnlock()

	case key == "triblist":
		ss.tribs.RLock()
		if val, ok := ss.userTribMap.content[usrID]; ok {
			reply.Value = val
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
		ss.tribs.RUnlock()

	case key == "friendlist":
		ss.userFriendMap.RLock()
		if val, ok := ss.userFriendMap.content[usrID]; ok {
			reply.Value = val
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
		ss.tribs.RUnlock()

	default:
		return fmt.Errorf("get does not accept key %s", key)
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	return nil
}
