package libstore

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type libstore struct {
	masterServerHostPort string
	myHostPort           string
	mode                 LeaseMode
	storageServerList    []storagerpc.Node
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
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
	ls := new(libstore)
	ls.masterServerHostPort = masterServerHostPort
	ls.myHostPort = myHostPort
	ls.mode = mode

	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

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
			return nil, fmt.Errorf("Calling StorageServer.GetServers returns abnormal status: %d", reply.Status)
		}
	}

	ls.storageServerList = reply.Servers

	return ls, nil
}

func (ls *libstore) schedule(key string) (*rpc.Client, error) {

	node := ls.storageServerList[0]
	cli, err := rpc.DialHTTP("tcp", node.HostPort)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func (ls *libstore) Get(key string) (string, error) {
	cli, err := ls.schedule(key)
	if err != nil {
		return "", err
	}

	args := storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myHostPort}
	var reply storagerpc.GetReply

	if err = cli.Call("StorageServer.Get", args, &reply); err != nil {
		return "", err
	}
	if reply.Status == storagerpc.OK {
		return reply.Value, nil
	}
	return "", fmt.Errorf("Return status %d", reply.Status)
}

func (ls *libstore) Put(key, value string) error {
	cli, err := ls.schedule(key)
	if err != nil {
		return err
	}

	args := storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply

	if err = cli.Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	}
	return fmt.Errorf("Return status %d", reply.Status)
}

func (ls *libstore) Delete(key string) error {
	cli, err := ls.schedule(key)
	if err != nil {
		return err
	}

	args := storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply

	if err = cli.Call("StorageServer.Delete", args, &reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	}
	return fmt.Errorf("Return status %d", reply.Status)
}

func (ls *libstore) GetList(key string) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	return errors.New("not implemented")
}

func (ls *libstore) AppendToList(key, newItem string) error {
	return errors.New("not implemented")
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
