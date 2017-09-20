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
	ls libstore.Libstore
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
	ts := tribServer{ls}

	if err = rpc.RegisterName("TribServer", tribrpc.Wrap(&ts)); err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	go func() {
		err = http.ListenAndServe(myHostPort, nil)
		if err != nil {
			LOGE.Fatalf("NewTribServer: http server failed to start: %v", err)
		}
	}()

	return &ts, nil
}

// CreateUser creates a user with the specified UserID.
// Replies with status Exists if the user has previously been created.
func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {

	// check whether user exists
	if existed := ts.isUserExisted(args.UserID); existed {
		reply.Status = tribrpc.Exists
		return nil
	}

	// if user does not exist, create the user
	userKey := util.FormatUserKey(args.UserID)
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

	if res := ts.isSubscribed(args.UserID, args.TargetUserID); res {
		reply.Status = tribrpc.Exists
		return nil
	}

	userSublistKey := util.FormatSubListKey(args.UserID)
	err := ts.ls.AppendToList(userSublistKey, args.TargetUserID)
	if err != nil {
		return err
	}

	reply.Status = tribrpc.OK
	return nil
}

// RemoveSubscription removes TargerUserID to UserID's list of subscriptions.
// Replies with status NoSuchUser if the specified UserID does not exist, and NoSuchTargerUser
// if the specified TargerUserID does not exist.
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

// GetFriends retrieves a list of friends of the given user.
// Replies with status NoSuchUser if the specified UserID does not exist.
func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	// check whether the user exists
	if existed := ts.isUserExisted(args.UserID); !existed {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	userSubListKey := util.FormatSubListKey(args.UserID)
	subUserList, err := ts.ls.GetList(userSubListKey)
	if err != nil {
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

// DeleteTribble delete a tribble with the specified PostKey.
// Replies with status NoSuchPost if the specified PostKey does not exist.
func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
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

	err := ts.ls.Delete(args.PostKey)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
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
	for i := 0; i < len(subscribedUsers); i++ {
		subUserID := subscribedUsers[i]
		subUserIDKey := util.FormatTribListKey(subUserID)
		if subUserTribKeys, err := ts.ls.GetList(subUserIDKey); err == nil {
			// if trib list exists
			tribKeys = append(tribKeys, subUserTribKeys...)
		}
	}
	// sort keys based on timestamp
	sort.Sort(ByPostedTimestamp(tribKeys))
	for _, k := range tribKeys {
		if tribContent, err := ts.ls.Get(k); err == nil {
			usrID, timestamp := parseTribKey(k)
			trib := tribrpc.Tribble{UserID: usrID, Posted: timestamp, Contents: tribContent}
			reply.Tribbles = append(reply.Tribbles, trib)
			if len(reply.Tribbles) == 100 {
				break
			}
		}

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
