package tribserver

import (
	"errors"
	"net/rpc"

	"encoding/json"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net/rpc"
	"time"
)

type tribServer struct {
	// TODO: implement this!
	hostPort             string
	masterServerHostPort string
	ls                   libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	ts := tribServer{}

	ts.hostPort = myHostPort
	ts.masterServerHostPort = masterServerHostPort
	ls, err := libstore.NewLibstore(ts.masterServerHostPort, ts.hostPort,
		libstore.Never)
	if err != nil {
		return nil, err
	}
	ts.ls = ls
	err = rpc.Register(&ts)
	if err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	err = http.ListenAndServe(ts.hostPort, nil)
	if err != nil {
		return nil, err
	}
	return &ts, nil
}

func (ts *tribServer) isUserExisted(userID string) (bool, error) {
	userKey := util.FormatUserKey(userID)
	result, err := ts.ls.Get(userKey)
	if err != nil {
		return false, err
	}
	if result == "" {
		return false, nil
	} else {
		return true, nil
	}

}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	userID := args.UserID
	userKey := util.FormatUserKey(userID)

	// check whether user exists
	existed, err := ts.isUserExisted(userID)
	if err != nil {
		return err
	}
	if existed {
		reply.Status = tribrpc.Exists
		return nil
	}

	// if user does not exist, create the user
	err = ts.ls.Put(userKey, "true")
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil

}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	// check whether the target user exists
	existed, err := ts.isUserExisted(args.TargetUserID)
	if err != nil {
		return err
	}
	if !existed {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	// check whether the request user exists
	existed, err = ts.isUserExisted(args.TargetUserID)
	if err != nil {
		return err
	}
	if !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// get existing subscription list
	userSublistKey := util.FormatSubListKey(args.UserID)
	subList, err := ts.ls.GetList(userSublistKey)
	if err != nil {
		return err
	}
	for _, u := range subList {
		if u == args.TargetUserID {
			reply.Status = tribrpc.Exists
			return nil
		}
	}
	err = ts.ls.AppendToList(userSublistKey, args.TargetUserID)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	// check whether the target user exists
	existed, err := ts.isUserExisted(args.TargetUserID)
	if err != nil {
		return err
	}
	if !existed {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	// check whether the request user exists
	existed, err = ts.isUserExisted(args.TargetUserID)
	if err != nil {
		return err
	}
	if !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// get existing subscription list
	userSublistKey := util.FormatSubListKey(args.UserID)
	subList, err := ts.ls.GetList(userSublistKey)
	if err != nil {
		return err
	}
	for _, u := range subList {
		if u == args.TargetUserID {
			err = ts.ls.RemoveFromList(userSublistKey, args.TargetUserID)
			if err != nil {
				return err
			}
			reply.Status = tribrpc.OK
			return nil
		}
	}
	// Target user not in the subscription list
	reply.Status = tribrpc.NoSuchTargetUser
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	// check whether the target user exists
	existed, err := ts.isUserExisted(args.UserID)
	if err != nil {
		return err
	}
	if !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// Complexity too high if search one by one, let backend
	// maintain the list
	friendListKey := util.FormatFriendListKey(args.UserID)
	list, err := ts.ls.GetList(friendListKey)
	if err != nil {
		return err
	}
	reply.UserIDs = list
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	// check whether the target user exists
	existed, err := ts.isUserExisted(args.UserID)
	if err != nil {
		return err
	}
	if !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// create the tribble
	t := tribrpc.Tribble{UserID: args.UserID, Contents: args.Contents, Posted: time.Now()}
	postKey := util.FormatPostKey(args.UserID, t.Posted.UnixNano())
	err = ts.ls.Put(postKey, args.Contents)
	if err != nil {
		return err
	}
	reply.PostKey = postKey
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	// check whether the target user exists
	existed, err := ts.isUserExisted(args.UserID)
	if err != nil {
		return err
	}
	if !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	err = ts.ls.Delete(args.PostKey)
	if err != nil {
		return err
	}

	// invalidate the cache in libstore?

	return nil
}

// returns a maximum of 100 tribs in reverse chronological order
func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	// check whether the target user exists
	existed, err := ts.isUserExisted(args.UserID)
	if err != nil {
		return err
	}
	if !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	tribListKey := util.FormatTribListKey(args.UserID)
	tribList, err := ts.ls.Get(tribListKey) // easier to unmarshal
	if err != nil {
		return err
	}

	err = json.Unmarshal([]byte(tribList), &reply.Tribbles)
	if err != nil {
		return err
	}
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	// check whether the target user exists
	existed, err := ts.isUserExisted(args.UserID)
	if err != nil {
		return err
	}
	if !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	subListKey := util.FormatSubListKey(args.UserID)
	tribList, err := ts.ls.Get(subListKey) // easier to unmarshal
	if err != nil {
		return err
	}

	err = json.Unmarshal([]byte(tribList), &reply.Tribbles)
	if err != nil {
		return err
	}
	return nil
}
