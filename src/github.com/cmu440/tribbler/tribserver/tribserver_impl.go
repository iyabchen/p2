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
	"sync"
	"time"
)

type tribServer struct {
	ls     libstore.Libstore
	server *http.Server
	sync.Mutex
}

// ByPostedTimestamp is a wrapper for tribble key, and used for
// sorting tribble keys by reverse chronological timestamp
type ByPostedTimestamp []string

func (t ByPostedTimestamp) Len() int { return len(t) }
func (t ByPostedTimestamp) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
func (t ByPostedTimestamp) Less(i, j int) bool {
	tiArrays := strings.Split(t[i], "_")
	tjArrays := strings.Split(t[j], "_")
	if len(tiArrays) != 3 || len(tjArrays) != 3 {
		log.Fatal("Incorrect tribble post key format.")
	}
	return tiArrays[1] > tjArrays[1]
}

var defaultFetchSize = 100
var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	ls, err := libstore.NewLibstore(masterServerHostPort, myHostPort,
		libstore.Never)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Addr: myHostPort}
	ts := tribServer{ls: ls, server: server}

	if err = rpc.RegisterName("TribServer", tribrpc.Wrap(&ts)); err != nil {
		return nil, err
	}
	rpc.HandleHTTP()

	go func() {
		err = server.ListenAndServe()
		if err != nil {
			LOGE.Fatalf("NewTribServer: http server failed to start: %v", err)
		}
	}()

	return &ts, nil
}

// CreateUser creates a user with the specified UserID.
// Replies with status Exists if the user has previously been created.
func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	ts.Lock()
	defer ts.Unlock()
	// check whether user exists
	if existed := ts.isUserExisted(args.UserID); existed {
		reply.Status = tribrpc.Exists
		return nil
	}

	// if user does not exist, create the user
	userKey := util.FormatUserKey(args.UserID)
	if err := ts.ls.Put(userKey, "true"); err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

// AddSubscription adds TargerUserID to UserID's list of subscriptions.
// Replies with status NoSuchUser if the specified UserID does not exist, and NoSuchTargerUser
// if the specified TargerUserID does not exist.
func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	ts.Lock()
	defer ts.Unlock()

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

	// check whether already subscribed
	if res := ts.isSubscribed(args.UserID, args.TargetUserID); res {
		reply.Status = tribrpc.Exists
		return nil
	}

	userSublistKey := util.FormatSubListKey(args.UserID)
	if err := ts.ls.AppendToList(userSublistKey, args.TargetUserID); err != nil {
		return err
	}

	reply.Status = tribrpc.OK
	return nil
}

// RemoveSubscription removes TargerUserID to UserID's list of subscriptions.
// Replies with status NoSuchUser if the specified UserID does not exist, and NoSuchTargerUser
// if the specified TargerUserID does not exist.
func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	ts.Lock()
	defer ts.Unlock()

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

	// check whether already subscribed
	if res := ts.isSubscribed(args.UserID, args.TargetUserID); !res {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	userSublistKey := util.FormatSubListKey(args.UserID)
	if err := ts.ls.RemoveFromList(userSublistKey, args.TargetUserID); err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

// GetFriends retrieves a list of friends of the given user.
// Replies with status NoSuchUser if the specified UserID does not exist.
func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	ts.Lock()
	defer ts.Unlock()

	// check whether the user exists
	if existed := ts.isUserExisted(args.UserID); !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// get subscription list
	userSubListKey := util.FormatSubListKey(args.UserID)
	subUserList, err := ts.ls.GetList(userSubListKey)
	if err != nil {
		// if no subscription
		reply.UserIDs = []string{}
		reply.Status = tribrpc.OK
		return nil
	}

	for _, u := range subUserList {
		if ts.isSubscribed(u, args.UserID) {
			reply.UserIDs = append(reply.UserIDs, u)
		}
	}
	reply.Status = tribrpc.OK
	return nil
}

// PostTribble posts a tribble on behalf of the specified UserID. The TribServer
// should timestamp the entry before inserting the Tribble into it's local Libstore.
// Replies with status NoSuchUser if the specified UserID does not exist.
func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	ts.Lock()
	defer ts.Unlock()

	// check whether the target user exists
	if existed := ts.isUserExisted(args.UserID); !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// create the tribble
	postKey := util.FormatPostKey(args.UserID, time.Now().UnixNano())
	if err := ts.ls.Put(postKey, args.Contents); err != nil {
		return err
	}

	// add to tribble list
	tribListKey := util.FormatTribListKey(args.UserID)
	if err := ts.ls.AppendToList(tribListKey, postKey); err != nil {
		return err
	}

	reply.PostKey = postKey
	reply.Status = tribrpc.OK
	return nil
}

// DeleteTribble delete a tribble with the specified PostKey.
// Replies with status NoSuchPost if the specified PostKey does not exist.
func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	ts.Lock()
	defer ts.Unlock()

	// check whether the target user exists
	if existed := ts.isUserExisted(args.UserID); !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// check whether the key exists
	if _, err := ts.ls.Get(args.PostKey); err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}

	if err := ts.ls.Delete(args.PostKey); err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

// GetTribbles retrieves a list of at most 100 tribbles posted by the specified
// UserID in reverse chronological order (most recent first).
// Replies with status NoSuchUser if the specified UserID does not exist.
func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	ts.Lock()
	defer ts.Unlock()

	// check whether the target user exists
	if existed := ts.isUserExisted(args.UserID); !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// get tribble list
	tribListKey := util.FormatTribListKey(args.UserID)
	tribKeys, err := ts.ls.GetList(tribListKey)
	if err != nil {
		// trib list not found
		reply.Tribbles = []tribrpc.Tribble{}
		reply.Status = tribrpc.OK
		return nil
	}

	// reverse sort the tribble keys based on timestamp
	sort.Sort(ByPostedTimestamp(tribKeys))
	i := 0
	for i < defaultFetchSize && i < len(tribKeys) {
		tribKey := tribKeys[i]
		// ignore if there is error, probably coz trib is delete
		if contents, err := ts.ls.Get(tribKey); err == nil {
			userID, posted := parseTribKey(tribKey)
			t := tribrpc.Tribble{UserID: userID, Posted: posted, Contents: contents}
			reply.Tribbles = append(reply.Tribbles, t)
		}
		i++
	}
	reply.Status = tribrpc.OK
	return nil
}

// GetTribblesBySubscription retrieves a list of at most 100 tribbles posted by
// all users to which the specified UserID is subscribed in reverse chronological
// order (most recent first).  Replies with status NoSuchUser if the specified UserID
// does not exist.
func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	ts.Lock()
	defer ts.Unlock()

	// check whether the target user exists
	if existed := ts.isUserExisted(args.UserID); !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// get sublist
	subscribedUsers, err := ts.ls.GetList(util.FormatSubListKey(args.UserID))
	if err != nil {
		// list not exist
		reply.Tribbles = []tribrpc.Tribble{}
		reply.Status = tribrpc.OK
		return nil
	}

	// get all subscribed users' post keys
	tribKeys := []string{}
	for i := 0; i < len(subscribedUsers); i++ {
		subUserID := subscribedUsers[i]
		subUserIDKey := util.FormatTribListKey(subUserID)
		if subUserTribKeys, err := ts.ls.GetList(subUserIDKey); err == nil {
			// if trib list exists
			tribKeys = append(tribKeys, subUserTribKeys...)
		}
	}

	// reverse sort keys based on timestamp
	sort.Sort(ByPostedTimestamp(tribKeys))
	i := 0
	for i < defaultFetchSize && i < len(tribKeys) {
		tribKey := tribKeys[i]
		// ignore if there is error, probably coz trib is delete
		if contents, err := ts.ls.Get(tribKey); err == nil {
			userID, posted := parseTribKey(tribKey)
			t := tribrpc.Tribble{UserID: userID, Posted: posted, Contents: contents}
			reply.Tribbles = append(reply.Tribbles, t)
		}
		i++
	}
	reply.Status = tribrpc.OK
	return nil
}

// -----------------
// helper functions
// -----------------

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

func (ts *tribServer) isUserExisted(userID string) bool {
	userKey := util.FormatUserKey(userID)
	if _, err := ts.ls.Get(userKey); err != nil {
		return false
	}
	return true

}

// both user id are valid
// check whether userID subscribe to target user ID
func (ts *tribServer) isSubscribed(userID, targetUserID string) bool {
	userSublistKey := util.FormatSubListKey(userID)
	if subList, err := ts.ls.GetList(userSublistKey); err == nil {
		// if list exist, check duplicate
		for _, u := range subList {
			if u == targetUserID {
				return true
			}
		}
	}
	return false
}
