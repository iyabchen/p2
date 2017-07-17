package storageserver

import (
	"errors"
	"fmt"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"

	"github.com/cmu440/tribbler/rpc/storagerpc"
	"strings"
	"time"
)

type role int
type status int

type storageServer struct {
	// TODO: implement this!
	isMaster      bool
	activeNodes   nodeMap
	masterHost    string
	masterPort    int
	numNodes      int
	listenPort    int
	nodeID        uint32
	joinNode      chan storagerpc.Node
	joinNodeReply chan storagerpc.Status
	userMap       resourceList
	userFriendMap resourceList
	userSubMap    resourceList
	userTribMap   resourceList
	tribMap       resource
}

type resourceList struct {
	sync.RWMutex
	content map[string][]string
}

type resource struct {
	sync.RWMutex
	content map[string]string
}

type nodeMap struct {
	sync.RWMutex
	nodes map[uint32]string
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
	srv.activeNodes = nodeMap{nodes: make(map[uint32]string)}
	srv.userMap = resourceList{content: make(map[string][]string)}
	srv.userSubMap = resourceList{content: make(map[string][]string)}
	srv.userFriendMap = resourceList{content: make(map[string][]string)}
	srv.userTribMap = resourceList{content: make(map[string][]string)}
	srv.tribMap = resource{content: make(map[string]string)}

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
		return nil, fmt.Errorf("Returned status %d", reply.Status)
	} else { // if master, then listen to RPC calls, until all joined
		srv.activeNodes.RLock()
		srv.activeNodes.nodes[nodeID] = masterServerHostPort
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
			srv.activeNodes.RLock()
			srv.activeNodes.nodes[newNode.NodeID] = newNode.HostPort
			if len(srv.activeNodes.nodes) == srv.numNodes {
				srv.activeNodes.RUnlock()
				break
			}
			status := storagerpc.NotReady
			if len(srv.activeNodes.nodes) == srv.numNodes {
				status = storagerpc.OK
			}
			srv.activeNodes.RUnlock()
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
	i := 0
	for id, hostport := range ss.activeNodes.nodes {
		ret[i].NodeID = id
		ret[i].HostPort = hostport
		i++
	}
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
	return true
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
		defer ss.userMap.RUnlock()
		if _, ok := ss.userMap.content[usrID]; !ok {
			reply.Status = storagerpc.KeyNotFound
			return nil
		} else {
			reply.Status = storagerpc.OK
		}
	case strings.HasPrefix(key, "post"):
		tribs, ok := ss.userTribMap[usrID]
		if !ok {
			return errors.New("internal error: content mismatch")
		}
		tribShard.RLock()
		defer tribShard.RUnlock()
		trib, ok := tribShard.tribMap[key]
		if !ok {
			reply.Status = storagerpc.KeyNotFound
		} else {
			reply.Value = trib
			reply.Status = storagerpc.OK
		}
	}

	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.checkKeyValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
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
		defer ss.userMap.Unlock()
		if _, ok := ss.userMap.content[key]; ok {
			reply.Status = storagerpc.ItemExists
		} else {
			ss.userMap.content[key] = []string{"true"}
			ss.userTribMap[key] = []string{}
			reply.Status = storagerpc.OK
		}

	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}
