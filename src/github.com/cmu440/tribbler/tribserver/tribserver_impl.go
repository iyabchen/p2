package tribserver

import (
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type tribServer struct {
	// TODO: implement this!
	hostPort             string
	masterServerHostPort string
	ls                   libstore.Libstore
}

// Sorting tribkeys by reverse chronical timestamp
type ByTribkeyTimestamp []string

func (t ByTribkeyTimestamp) Len() int { return len(t) }
func (t ByTribkeyTimestamp) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
func (t ByTribkeyTimestamp) Less(i, j int) bool {
	tiArrs := strings.Split(t[i], "_")
	tjArrs := strings.Split(t[j], "_")
	if len(tiArrs) != 3 || len(tjArrs) != 3 {
		log.Fatal("Incorrect tribkey.")
	}
	return tiArrs[1] > tjArrs[1]
}

var defaultFetchSize int = 100
var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)

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
	if err = rpc.RegisterName("TribServer", tribrpc.Wrap(&ts)); err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	go func() {
		err = http.ListenAndServe(ts.hostPort, nil)
		if err != nil {
			LOGE.Fatalf("http server start failed with error: %v", err)
		}
	}()

	return &ts, nil
}

func (ts *tribServer) isUserExisted(userID string) bool {
	userKey := util.FormatUserKey(userID)
	if _, err := ts.ls.Get(userKey); err != nil {
		return false
	}
	return true

}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	userID := args.UserID
	userKey := util.FormatUserKey(userID)

	// check whether user exists
	if existed := ts.isUserExisted(userID); existed {
		reply.Status = tribrpc.Exists
		return nil
	}

	// if user does not exist, create the user
	err := ts.ls.Put(userKey, "true")
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil

}

// AddSubscription adds TargerUserID to UserID's list of subscriptions.
// Replies with status NoSuchUser if the specified UserID does not exist, and NoSuchTargerUser
// if the specified TargerUserID does not exist.
func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	// check whether the target user exists
	if existed := ts.isUserExisted(args.TargetUserID); !existed {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	// check whether the request user exists
	if existed := ts.isUserExisted(args.UserID); !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// get existing subscription list
	userSublistKey := util.FormatSubListKey(args.UserID)
	if subList, err := ts.ls.GetList(userSublistKey); err == nil {
		// if list exist, check duplicate
		for _, u := range subList {
			if u == args.TargetUserID {
				reply.Status = tribrpc.Exists
				return nil
			}
		}
	}
	err := ts.ls.AppendToList(userSublistKey, args.TargetUserID)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	// check whether the target user exists
	if existed := ts.isUserExisted(args.TargetUserID); !existed {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	// check whether the request user exists
	if existed := ts.isUserExisted(args.UserID); !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// get existing subscription list
	userSublistKey := util.FormatSubListKey(args.UserID)
	subList, err := ts.ls.GetList(userSublistKey)
	if err != nil {
		// list does not exist
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
	if existed := ts.isUserExisted(args.UserID); !existed {
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
	if existed := ts.isUserExisted(args.UserID); !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// create the tribble
	t := tribrpc.Tribble{UserID: args.UserID, Contents: args.Contents, Posted: time.Now()}
	postKey := util.FormatPostKey(args.UserID, t.Posted.UnixNano())
	err := ts.ls.Put(postKey, args.Contents)
	if err != nil {
		// LOGE.Printf("libstore.Put error: %s", err)
		return err
	}
	tribListKey := util.FormatTribListKey(args.UserID)
	err = ts.ls.AppendToList(tribListKey, postKey)
	if err != nil {
		// LOGE.Printf("libstore.AppendToList error: %s", err)
		return err
	}
	reply.PostKey = postKey
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	// check whether the target user exists
	if existed := ts.isUserExisted(args.UserID); !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	err := ts.ls.Delete(args.PostKey)
	if err != nil {
		return err
	}

	// invalidate the cache in libstore?

	return nil
}

// GetTribbles retrieves a list of at most 100 tribbles posted by the specified
// UserID in reverse chronological order (most recent first).
// Replies with status NoSuchUser if the specified UserID does not exist.
func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	// check whether the target user exists
	if existed := ts.isUserExisted(args.UserID); !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	tribListKey := util.FormatTribListKey(args.UserID)
	tribKeyList, err := ts.ls.GetList(tribListKey)
	if err != nil {
		// trib list not found
		reply.Tribbles = []tribrpc.Tribble{}
		reply.Status = tribrpc.OK
		return nil
	}

	sort.Sort(sort.Reverse(sort.StringSlice(tribKeyList)))
	i := 0
	for i < defaultFetchSize && i < len(tribKeyList) {
		tribKey := tribKeyList[i]
		trib, err := ts.ls.Get(tribKeyList[i])
		// ignore if there is error, probably coz trib is deleted
		if err == nil {
			usrID, posted := parseTribKey(tribKey)
			var t tribrpc.Tribble
			t.Contents = trib
			t.UserID = usrID
			t.Posted = posted
			reply.Tribbles = append(reply.Tribbles, t)
			i++
		}
	}
	reply.Status = tribrpc.OK
	return nil
}

// GetTribblesBySubscription retrieves a list of at most 100 tribbles posted by
// all users to which the specified UserID is subscribed in reverse chronological
// order (most recent first).  Replies with status NoSuchUser if the specified UserID
// does not exist.
func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	// check whether the target user exists
	if existed := ts.isUserExisted(args.UserID); !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	subscribedUsers, err := ts.ls.GetList(util.FormatSubListKey(args.UserID))
	if err != nil {
		// list not exist
		reply.Tribbles = []tribrpc.Tribble{}
		reply.Status = tribrpc.OK
		return nil
	}

	tribKeys := []string{}
	for i := len(subscribedUsers) - 1; i > 0; i-- {
		subUserID := subscribedUsers[i]
		subTribKeys, err := ts.ls.GetList(util.FormatTribListKey(subUserID))
		if err != nil {
			tribKeys = append(tribKeys, subTribKeys...)
		}
	}
	// sort keys based on timestamp
	sort.Sort(ByTribkeyTimestamp(tribKeys))
	for _, tribKey := range tribKeys {
		tribContent, err := ts.ls.Get(tribKey)
		if err != nil {
			usrID, timestamp := parseTribKey(tribKey)
			trib := tribrpc.Tribble{usrID, timestamp, tribContent}
			reply.Tribbles = append(reply.Tribbles, trib)
		}
		if len(reply.Tribbles) == 100 {
			break
		}
	}
	reply.Status = tribrpc.OK
	return nil
}

func parseTribKey(key string) (usrID string, timestamp time.Time) {
	strs := strings.Split(key, ":")
	if len(strs) != 2 {
		log.Fatal("Wrong format of tribkey")
	}
	usrID = strs[0]
	subStrs := strings.Split(strs[1], "_")
	if len(subStrs) != 3 {
		log.Fatal("Wrong format of tribkey")
	}

	i, err := strconv.ParseInt(subStrs[1], 16, 64)
	if err != nil {
		log.Fatal("Failed to convert timestamp to int")
	}
	// At post, key is formatted with time.UnixNano
	timestamp = time.Unix(0, i)
	return

}
