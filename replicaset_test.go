// Copyright 2013-2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package replicaset

import (
	"fmt"
	"io"
	"sort"
	"strings"
	stdtesting "testing"
	"time"

	"github.com/juju/errors"
	"github.com/juju/mgo/v3"
	"github.com/juju/mgo/v3/bson"
	mgotesting "github.com/juju/mgo/v3/testing"
	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	"github.com/juju/utils/v3"
	gc "gopkg.in/check.v1"
)

const rsName = "juju"

func TestPackage(t *stdtesting.T) {
	gc.TestingT(t)
}

type MongoSuite struct {
	testing.IsolationSuite
	root *mgotesting.MgoInstance
}

func newServer(c *gc.C) *mgotesting.MgoInstance {
	inst := &mgotesting.MgoInstance{Params: []string{"--replSet", rsName}}
	err := inst.Start(nil)
	c.Assert(err, jc.ErrorIsNil)

	session, err := inst.DialDirect()
	if err != nil {
		inst.Destroy()
		c.Fatalf("error dialing mongo server: %v", err.Error())
		return nil
	}
	defer session.Close()

	session.SetMode(mgo.Monotonic, true)
	if err = session.Ping(); err != nil {
		inst.Destroy()
		c.Fatalf("error pinging mongo server: %v", err.Error())
	}
	buildInfo, err := session.BuildInfo()
	c.Assert(err, jc.ErrorIsNil)
	if !buildInfo.VersionAtLeast(4) {
		c.Fatalf("only mongo version 4 or greater supported, found %v", buildInfo.Version)
	}
	return inst
}

var _ = gc.Suite(&MongoSuite{})

func (s *MongoSuite) SetUpTest(c *gc.C) {
	s.IsolationSuite.SetUpTest(c)
	s.PatchEnvironment("JUJU_MONGO_STORAGE_ENGINE", "wiredTiger")
	s.root = newServer(c)
	s.AddCleanup(func(c *gc.C) { s.root.Destroy() })
	dialAndTestInitiate(c, s.root, s.root.Addr())
}

var initialTags = map[string]string{"foo": "bar"}

// assertMembers asserts the known field values of a retrieved and expected
// Members slice are equal.
func assertMembers(c *gc.C, mems []Member, expectedMembers []Member) {
	// 2.x and 3.2 seem to use different default values for bool. For
	// example, in 3.2 false is false, not nil, and we can't know the
	// pointer value to check with DeepEquals.
	c.Logf("comparing: %s\nto: %s",
		fmtConfigForLog(&Config{Name: "obtained", Members: mems[:]}),
		fmtConfigForLog(&Config{Name: "expected", Members: expectedMembers[:]}))
	for i := range mems {
		c.Check(mems[i].Id, gc.Equals, expectedMembers[i].Id)
		c.Check(mems[i].Address, gc.Equals, expectedMembers[i].Address)
		c.Check(mems[i].Tags, jc.DeepEquals, expectedMembers[i].Tags)
	}
}

func dialAndTestInitiate(c *gc.C, inst *mgotesting.MgoInstance, addr string) {
	session := inst.MustDialDirect()
	defer session.Close()

	mode := session.Mode()
	err := Initiate(session, addr, rsName, initialTags)
	c.Assert(err, jc.ErrorIsNil)

	// make sure we haven't messed with the session's mode
	c.Assert(session.Mode(), gc.Equals, mode)

	// Ids start at 1 for us, so we can differentiate between set and unset
	expectedMembers := []Member{{Id: 1, Address: addr, Tags: initialTags}}

	// need to set mode to strong so that we wait for the write to succeed
	// before reading and thus ensure that we're getting consistent reads.
	session.SetMode(mgo.Strong, false)

	mems, err := CurrentMembers(session)
	c.Assert(err, jc.ErrorIsNil)
	assertMembers(c, mems, expectedMembers)

	loadData(session, c)
}

func (s *MongoSuite) TestInitiateSetsProtocolVersion(c *gc.C) {
	s.root.Destroy()

	// create a new server that hasn't been initiated
	s.root = newServer(c)
	session := s.root.MustDialDirect()
	defer session.Close()

	mockBuildInfo := func(session mgoSession) (mgo.BuildInfo, error) {
		return mgo.BuildInfo{
			Version:      "4",
			GitVersion:   "4.0.0",
			VersionArray: []int{4, 0, 0},
		}, nil
	}
	called := false
	mockAttemptInitiate := func(monotonicSession mgoSession, cfg []Config) error {
		c.Assert(cfg, gc.HasLen, 2)
		if cfg[0].ProtocolVersion != 1 {
			c.Fatalf("obtained protocol version %d, expected 1", cfg[0].ProtocolVersion)
		}
		called = true
		return nil
	}
	mockCurrentStatus := func(session mgoSession) (*Status, error) {
		return &Status{
			Name:    "test",
			Members: []MemberStatus{{}},
		}, nil
	}

	s.PatchValue(&getBuildInfo, mockBuildInfo)
	s.PatchValue(&attemptInitiate, mockAttemptInitiate)
	s.PatchValue(&getCurrentStatus, mockCurrentStatus)
	err := Initiate(session, s.root.Addr(), rsName, initialTags)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(called, jc.IsTrue)
}

func (s *MongoSuite) TestInitiateWaitsForStatus(c *gc.C) {
	s.root.Destroy()

	// create a new server that hasn't been initiated
	s.root = newServer(c)
	session := s.root.MustDialDirect()
	defer session.Close()

	i := 0
	mockStatus := func(session mgoSession) (*Status, error) {
		status := &Status{}
		var err error
		i += 1
		if i < 20 {
			err = fmt.Errorf("bang!")
		} else if i > 20 {
			// when i == 20 then len(status.Members) == 0
			// so we will be called one more time until we populate
			// Members
			status.Members = append(status.Members, MemberStatus{Id: 1})
		}
		return status, err
	}

	s.PatchValue(&getCurrentStatus, mockStatus)
	err := Initiate(session, s.root.Addr(), rsName, initialTags)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(i, gc.Equals, 21)
}

func loadData(session mgoSession, c *gc.C) {
	type foo struct {
		Name    string
		Address string
		Count   int
	}

	for col := 0; col < 10; col++ {
		// Testing with mongodb3.2 showed the need to make foos a slice
		// if interface{} (Insert expects a slice not an empty
		// interface) passed in expanded. Passing a slice of foo to
		// Insert gives this error, with the slice perhaps not handled
		// by writeOp().
		// `Message:"error parsing element 0 of field documents :: caused by :: wrong type for '0' field, expected object`
		foos := make([]interface{}, 10000)
		for n := range foos {
			foos[n] = foo{
				Name:    fmt.Sprintf("name_%d_%d", col, n),
				Address: fmt.Sprintf("address_%d_%d", col, n),
				Count:   n * (col + 1),
			}
		}

		err := session.DB("testing").C(fmt.Sprintf("data%d", col)).Insert(foos...)
		c.Assert(err, jc.ErrorIsNil)
	}
}

func attemptLoop(c *gc.C, strategy utils.AttemptStrategy, desc string, f func() error) {
	var err error
	start := time.Now()
	attemptCount := 0
	for attempt := strategy.Start(); attempt.Next(); {
		attemptCount += 1
		if err = f(); err == nil || !attempt.HasNext() {
			break
		}
		c.Logf("%s failed: %v", desc, err)
	}
	c.Logf("%s: %d attempts in %s", desc, attemptCount, time.Since(start))
	c.Assert(err, jc.ErrorIsNil)
}

func (s *MongoSuite) TestAddRemoveSet(c *gc.C) {
	getAddr := func(inst *mgotesting.MgoInstance) string {
		return inst.Addr()
	}
	assertAddRemoveSet(c, s.root, getAddr)
}

func assertAddRemoveSet(c *gc.C, root *mgotesting.MgoInstance, getAddr func(*mgotesting.MgoInstance) string) {
	session := root.MustDial()
	defer session.Close()

	members := make([]Member, 0, 5)

	// Add should be idempotent, so re-adding root here shouldn't result in
	// two copies of root in the replica set
	members = append(members, Member{Address: getAddr(root), Tags: initialTags})

	// We allow for up to 2 minutes  per operation, since Add, Set, etc. call
	// replSetReconfig which may cause primary renegotiation. According
	// to the Mongo docs, "typically this is 10-20 seconds, but could be
	// as long as a minute or more."
	//
	// Note that the delay is set at 500ms to cater for relatively quick
	// operations without thrashing on those that take longer.
	strategy := utils.AttemptStrategy{Total: time.Minute * 2, Delay: time.Millisecond * 500}

	instances := make([]*mgotesting.MgoInstance, 5)
	instances[0] = root
	for i := 1; i < len(instances); i++ {
		inst := newServer(c)
		instances[i] = inst
		// no need to Remove the instances from the replicaset as
		// we're destroying the replica set immediately afterwards
		defer inst.Destroy()
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		tags := map[string]string{key: val}
		members = append(members, Member{Address: getAddr(inst), Tags: tags})
	}

	attemptLoop(c, strategy, "Add()", func() error {
		return Add(session, members...)
	})

	expectedMembers := make([]Member, len(members))
	for i, m := range members {
		// Ids should start at 1 (for the root) and go up
		m.Id = i + 1
		expectedMembers[i] = m
	}

	var cfg *Config
	attemptLoop(c, strategy, "CurrentConfig()", func() error {
		var err error
		cfg, err = CurrentConfig(session)
		return err
	})
	c.Assert(cfg.Name, gc.Equals, rsName)
	// 5 since it increments for each added/removed member.
	c.Assert(cfg.Version, gc.Equals, 5)

	mems := cfg.Members
	assertMembers(c, mems, expectedMembers)

	// Now remove the last two Members...
	attemptLoop(c, strategy, "Remove()", func() error {
		return Remove(session, members[3].Address, members[4].Address)
	})
	expectedMembers = expectedMembers[0:3]

	// ... and confirm that CurrentMembers reflects the removal.
	attemptLoop(c, strategy, "CurrentMembers()", func() error {
		var err error
		mems, err = CurrentMembers(session)
		return err
	})
	assertMembers(c, mems, expectedMembers)

	// now let's mix it up and set the new members to a mix of the previous
	// plus the new arbiter
	// Also have an explicitly large member Id to make sure we don't have possible
	// collisions
	mem4 := members[4]
	mem4.Id = 10
	mems = []Member{members[3], mems[2], mems[0], mem4}
	attemptLoop(c, strategy, "Set()", func() error {
		err := Set(session, mems)
		if err != nil {
			c.Logf("current session mode: %v", session.Mode())
			session.Refresh()
		}
		return err
	})

	attemptLoop(c, strategy, "Ping()", func() error {
		// can dial whichever replica address here, mongo will figure it out
		if session != nil {
			session.Close()
		}
		session = instances[0].MustDialDirect()
		return session.Ping()
	})

	// any new members will get an id of max(other_ids...)+1
	expectedMembers = []Member{members[3], expectedMembers[2], expectedMembers[0], members[4]}
	expectedMembers[0].Id = 11
	expectedMembers[3].Id = 10
	// CurrentMembers always sorts the member ids by Id so they can be nicely displayed and tracked.
	sort.Slice(expectedMembers, func(i, j int) bool { return expectedMembers[i].Id < expectedMembers[j].Id })

	attemptLoop(c, strategy, "CurrentMembers()", func() error {
		var err error
		mems, err = CurrentMembers(session)
		return err
	})
	assertMembers(c, mems, expectedMembers)
}

func (s *MongoSuite) TestIsMaster(c *gc.C) {
	session := s.root.MustDial()
	defer session.Close()

	expected := IsMasterResults{
		// The following fields hold information about the specific mongodb node.
		IsMaster:  true,
		Secondary: false,
		Arbiter:   false,
		Address:   s.root.Addr(),
		LocalTime: time.Time{},

		// The following fields hold information about the replica set.
		ReplicaSetName: rsName,
		Addresses:      []string{s.root.Addr()},
		Arbiters:       nil,
		PrimaryAddress: s.root.Addr(),
	}

	res, err := IsMaster(session)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(closeEnough(res.LocalTime, time.Now()), jc.IsTrue)
	res.LocalTime = time.Time{}
	c.Check(*res, jc.DeepEquals, expected)
}

func (s *MongoSuite) TestMasterHostPort(c *gc.C) {
	session := s.root.MustDial()
	defer session.Close()

	expected := s.root.Addr()
	result, err := MasterHostPort(session)

	c.Logf("TestMasterHostPort expected: %v, got: %v", expected, result)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(result, gc.Equals, expected)
}

func (s *MongoSuite) TestMasterHostPortOnUnconfiguredReplicaSet(c *gc.C) {
	inst := &mgotesting.MgoInstance{}
	err := inst.Start(nil)
	c.Assert(err, jc.ErrorIsNil)
	defer inst.Destroy()
	session := inst.MustDial()
	hp, err := MasterHostPort(session)
	c.Assert(err, gc.Equals, ErrMasterNotConfigured)
	c.Assert(hp, gc.Equals, "")
}

func (s *MongoSuite) TestIsReadyOne(c *gc.C) {
	s.PatchValue(&getCurrentStatus,
		func(session mgoSession) (*Status, error) {
			status := &Status{Members: []MemberStatus{{
				Id:      1,
				Healthy: true,
			}}}
			return status, nil
		},
	)
	session := s.root.MustDial()
	defer session.Close()

	ready, err := IsReady(session)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(ready, jc.IsTrue)
}

func (s *MongoSuite) TestIsReadyMultiple(c *gc.C) {
	s.PatchValue(&getCurrentStatus,
		func(session mgoSession) (*Status, error) {
			status := &Status{}
			for i := 1; i < 5; i++ {
				member := MemberStatus{Id: i + 1, Healthy: true}
				status.Members = append(status.Members, member)
			}
			return status, nil
		},
	)
	session := s.root.MustDial()
	defer session.Close()

	ready, err := IsReady(session)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(ready, jc.IsTrue)
}

func (s *MongoSuite) TestIsReadyNotOne(c *gc.C) {
	s.PatchValue(&getCurrentStatus,
		func(session mgoSession) (*Status, error) {
			status := &Status{Members: []MemberStatus{{
				Id:      1,
				Healthy: false,
			}}}
			return status, nil
		},
	)
	session := s.root.MustDial()
	defer session.Close()

	ready, err := IsReady(session)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(ready, jc.IsFalse)
}

func (s *MongoSuite) TestIsReadyMinority(c *gc.C) {
	s.PatchValue(&getCurrentStatus,
		func(session mgoSession) (*Status, error) {
			status := &Status{Members: []MemberStatus{{
				Id:      1,
				Healthy: true,
			},
				{
					Id:      2,
					Healthy: false,
				},
				{
					Id:      3,
					Healthy: false,
				}}}
			return status, nil
		},
	)
	session := s.root.MustDial()
	defer session.Close()

	ready, err := IsReady(session)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(ready, jc.IsFalse)
}

func (s *MongoSuite) checkConnectionFailure(c *gc.C, failure error) {
	s.PatchValue(&getCurrentStatus,
		func(session mgoSession) (*Status, error) { return nil, failure },
	)
	session := s.root.MustDial()
	defer session.Close()

	ready, err := IsReady(session)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(ready, jc.IsFalse)
}

func (s *MongoSuite) TestIsReadyConnectionDropped(c *gc.C) {
	s.checkConnectionFailure(c, io.EOF)
}

func (s *MongoSuite) TestIsReadyConnectionFailedWithErrno(c *gc.C) {
	for _, errno := range connectionErrors {
		c.Logf("Checking errno %#v (%v)", errno, errno)
		s.checkConnectionFailure(c, errno)
	}
}

func (s *MongoSuite) TestIsReadyError(c *gc.C) {
	failure := errors.New("failed!")
	s.PatchValue(&getCurrentStatus,
		func(session mgoSession) (*Status, error) { return nil, failure },
	)
	session := s.root.MustDial()
	defer session.Close()

	_, err := IsReady(session)
	c.Check(errors.Cause(err), gc.Equals, failure)
}

func (s *MongoSuite) TestWaitUntilReady(c *gc.C) {
	var isReadyCalled bool
	mockIsReady := func(session mgoSession) (bool, error) {
		isReadyCalled = true
		return true, nil
	}

	s.PatchValue(&isReady, mockIsReady)
	session := s.root.MustDial()
	defer session.Close()

	err := WaitUntilReady(session, 10)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(isReadyCalled, jc.IsTrue)
}

func (s *MongoSuite) TestWaitUntilReadyTimeout(c *gc.C) {
	mockIsReady := func(session mgoSession) (bool, error) {
		return false, nil
	}

	s.PatchValue(&isReady, mockIsReady)
	session := s.root.MustDial()
	defer session.Close()

	err := WaitUntilReady(session, 0)
	c.Assert(err, gc.ErrorMatches, "timed out after 0 seconds")
}

func (s *MongoSuite) TestWaitUntilReadyError(c *gc.C) {
	mockIsReady := func(session mgoSession) (bool, error) {
		return false, errors.New("foobar")
	}

	s.PatchValue(&isReady, mockIsReady)
	session := s.root.MustDial()
	defer session.Close()

	err := WaitUntilReady(session, 0)
	c.Assert(err, gc.ErrorMatches, "foobar")
}

func (s *MongoSuite) TestCurrentStatus(c *gc.C) {
	session := s.root.MustDial()
	defer session.Close()

	inst1 := newServer(c)
	defer inst1.Destroy()
	defer func() { _ = Remove(session, inst1.Addr()) }()

	inst2 := newServer(c)
	defer inst2.Destroy()
	defer func() { _ = Remove(session, inst2.Addr()) }()

	var err error
	strategy := utils.AttemptStrategy{Total: time.Minute * 2, Delay: time.Millisecond * 500}
	attempt := strategy.Start()
	for attempt.Next() {
		err = Add(session, Member{Address: inst1.Addr()}, Member{Address: inst2.Addr()})
		if err == nil || !attempt.HasNext() {
			break
		}
	}
	c.Assert(err, jc.ErrorIsNil)

	expected := &Status{
		Name: rsName,
		Members: []MemberStatus{{
			Id:      1,
			Address: s.root.Addr(),
			Self:    true,
			ErrMsg:  "",
			Healthy: true,
			State:   PrimaryState,
		}, {
			Id:      2,
			Address: inst1.Addr(),
			Self:    false,
			ErrMsg:  "",
			Healthy: true,
			State:   SecondaryState,
		}, {
			Id:      3,
			Address: inst2.Addr(),
			Self:    false,
			ErrMsg:  "",
			Healthy: true,
			State:   SecondaryState,
		}},
	}

	strategy.Total = time.Second * 90
	attempt = strategy.Start()
	var res *Status
	for attempt.Next() {
		var err error
		res, err = CurrentStatus(session)
		if err != nil {
			if !attempt.HasNext() {
				c.Errorf("Couldn't get status before timeout, got err: %v", err)
				return
			} else {
				// try again
				continue
			}
		}

		if res.Members[0].State == PrimaryState &&
			res.Members[1].State == SecondaryState &&
			res.Members[2].State == SecondaryState {
			break
		}
		if !attempt.HasNext() {
			c.Errorf("Servers did not get into final state before timeout.  Status: %#v", res)
			return
		}
	}

	for x := range res.Members {
		// non-empty uptime and ping
		c.Check(res.Members[x].Uptime, gc.Not(gc.Equals), 0)

		// ping is always going to be zero since we're on localhost
		// so we can't really test it right now

		// now overwrite Uptime so it won't throw off DeepEquals
		res.Members[x].Uptime = 0
	}
	c.Check(res, jc.DeepEquals, expected)
}

func closeEnough(expected, obtained time.Time) bool {
	t := obtained.Sub(expected)
	return (-500*time.Millisecond) < t && t < (500*time.Millisecond)
}

func findPrimary(c *gc.C, session mgoSession) int {
	status, err := CurrentStatus(session)
	c.Assert(err, jc.ErrorIsNil)
	for i, m := range status.Members {
		if m.State == PrimaryState {
			return i
		}
	}
	return -1
}

func (s *MongoSuite) TestStepDownPrimary(c *gc.C) {
	session := s.root.MustDial()
	defer func() {
		if session != nil {
			session.Close()
			session = nil
		}
	}()
	s0 := s.root
	s1 := newServer(c)
	defer s1.Destroy()
	s2 := newServer(c)
	defer s2.Destroy()
	strategy := utils.AttemptStrategy{Total: time.Minute * 2, Delay: time.Millisecond * 500}
	attemptLoop(c, strategy, "Add()", func() error {
		return Add(session, Member{
			Address: s1.Addr(),
			Tags:    map[string]string{"s1": "s1"},
		}, Member{
			Address: s2.Addr(),
			Tags:    map[string]string{"s2": "s2"},
		})
	})
	mems, err := CurrentMembers(session)
	c.Assert(err, jc.ErrorIsNil)
	assertMembers(c, mems, []Member{{
		Id:      1,
		Address: s0.Addr(),
		Tags:    initialTags,
	}, {
		Id:      2,
		Address: s1.Addr(),
		Tags:    map[string]string{"s1": "s1"},
	}, {
		Id:      3,
		Address: s2.Addr(),
		Tags:    map[string]string{"s2": "s2"},
	}})
	// find the current primary
	initialPrimary := findPrimary(c, session)
	c.Assert(initialPrimary, jc.GreaterThan, -1)
	// ensure the secondaries are up and happy
	// strategy = utils.AttemptStrategy{Total: time.Second, Delay: time.Millisecond * 50}
	attemptLoop(c, strategy, "secondaries are ready", func() error {
		status, err := CurrentStatus(session)
		if err != nil {
			return err
		}
		var notReady []string
		for _, m := range status.Members {
			if m.State != PrimaryState && m.State != SecondaryState {
				notReady = append(notReady, fmt.Sprintf("Member{Id: %d, Address: %s, State: %s}", m.Id, m.Address, m.State.String()))
			}
		}
		if len(notReady) > 0 {
			return errors.Errorf("members not ready: %s", strings.Join(notReady, ", "))
		}
		return nil
	})
	// Now that the secondaries are up, we should be able to ask the primary to step down and notice that the primary changes
	err = StepDownPrimary(session)
	c.Assert(err, jc.ErrorIsNil)
	// Changing the primary should cause us to get disconnected, so we need to reconnect
	session.Close()
	session = nil
	attemptLoop(c, strategy, "reconnect", func() error {
		session, err = s.root.Dial()
		if err != nil {
			session = nil
			return err
		}
		return nil
	})
	// Now that we are reconnected, we should definitely have a different primary
	newPrimary := findPrimary(c, session)
	c.Check(newPrimary, gc.Not(gc.Equals), initialPrimary)
}

func ipv6GetAddr(inst *mgotesting.MgoInstance) string {
	return fmt.Sprintf("[::1]:%v", inst.Port())
}

type MongoIPV6Suite struct {
	testing.IsolationSuite
}

var _ = gc.Suite(&MongoIPV6Suite{})

func (s *MongoIPV6Suite) SetUpTest(c *gc.C) {
	s.IsolationSuite.SetUpTest(c)
	s.PatchEnvironment("JUJU_MONGO_STORAGE_ENGINE", "wiredTiger")
}

func (s *MongoIPV6Suite) TestAddRemoveSetIPv6(c *gc.C) {
	root := newServer(c)
	defer root.Destroy()
	dialAndTestInitiate(c, root, ipv6GetAddr(root))
	assertAddRemoveSet(c, root, ipv6GetAddr)
}

func (s *MongoIPV6Suite) TestAddressFixing(c *gc.C) {
	root := newServer(c)
	defer root.Destroy()
	dialAndTestInitiate(c, root, ipv6GetAddr(root))
	session := root.MustDial()
	defer session.Close()

	status, err := CurrentStatus(session)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(len(status.Members), jc.DeepEquals, 1)
	c.Check(status.Members[0].Address, gc.Equals, ipv6GetAddr(root))

	cfg, err := CurrentConfig(session)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(len(cfg.Members), jc.DeepEquals, 1)
	c.Check(cfg.Members[0].Address, gc.Equals, ipv6GetAddr(root))

	result, err := IsMaster(session)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(result.Address, gc.Equals, ipv6GetAddr(root))
	c.Check(result.PrimaryAddress, gc.Equals, ipv6GetAddr(root))
	c.Check(result.Addresses, jc.DeepEquals, []string{ipv6GetAddr(root)})
}

type mockSession struct {
	mgoSession
	cfg         *Config
	repaired    bool
	reconfigErr error
	// failWhen selects which reconfig loses quorum; when nil the remove
	// leaving only member 666 fails.
	failWhen      func(cfg Config) bool
	retryFailWhen func(cfg Config) bool
}

func (m *mockSession) Run(cmd interface{}, _ interface{}) error {
	data, ok := cmd.(bson.D)
	if !ok {
		return fmt.Errorf("unexpected cmd data %v", cmd)
	}
	if len(data) < 1 || data[0].Name != "replSetReconfig" {
		return fmt.Errorf("unexpected cmd data %v", data)
	}
	cfg, ok := data[0].Value.(Config)
	if !ok {
		return fmt.Errorf("unexpected cmd data %v", data[0].Value)
	}
	forced := len(data) > 1
	if forced {
		force, ok := data[1].Value.(string)
		if !ok || force != "true" {
			return fmt.Errorf("unexpected force value %v", data[1].Value)
		}
		m.repaired = true
	}
	failWhen := m.failWhen
	if failWhen == nil {
		failWhen = func(cfg Config) bool {
			return len(cfg.Members) == 1 && cfg.Members[0].Id == 666
		}
	}
	initialFailure := !m.repaired && failWhen(cfg)
	retryFailure := m.repaired && !forced && m.retryFailWhen != nil && m.retryFailWhen(cfg)
	if initialFailure || retryFailure {
		if m.reconfigErr != nil {
			return m.reconfigErr
		}
		return &mgo.QueryError{Code: 11602, Message: "Quorum check failed because not enough voting nodes responded"}
	}
	m.cfg = &cfg
	return nil
}

func (*mockSession) Ping() error { return nil }
func (*mockSession) Refresh()    {}

type changesSuite struct {
	testing.IsolationSuite

	current []Member
}

var _ = gc.Suite(&changesSuite{})

func (s *changesSuite) SetUpTest(c *gc.C) {
	s.PatchValue(&getCurrentConfig, func(session mgoSession) (*Config, error) {
		return &Config{
			Members: append([]Member(nil), s.current...),
		}, nil
	})
	s.PatchValue(&confirmDeadInterval, time.Duration(0))
	s.PatchValue(&primaryPollInterval, time.Duration(0))
}

func (s *changesSuite) TestSetNoChanges(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, gc.IsNil)
}

var (
	one     = 1.0
	votes   = 1
	novotes = 0
)

func (s *changesSuite) TestSetAdds(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      1,
			Address: "10.0.0.1",
		}, {
			Id:       2,
			Address:  "10.0.0.2",
			Priority: &one,
			Votes:    &votes,
		}},
	})
}

func (s *changesSuite) TestSetUpdateAddress(c *gc.C) {
	s.current = []Member{{
		Id:      4,
		Address: "10.0.0.1",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      5,
		Address: "10.0.0.3",
	}, {
		Id:      4,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 2,
		Members: []Member{{
			Id:      4,
			Address: "10.0.0.2",
		}, {
			Id:       5,
			Address:  "10.0.0.3",
			Priority: &one,
			Votes:    &votes,
		}},
	})
}

func (s *changesSuite) TestSetUpdateVote(c *gc.C) {
	s.current = []Member{{
		Id:       4,
		Address:  "10.0.0.2",
		Priority: &one,
		Votes:    &votes,
	}, {
		Id:      5,
		Address: "10.0.0.3",
		Votes:   &novotes,
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:       5,
		Address:  "10.0.0.3",
		Priority: &one,
		Votes:    &votes,
	}, {
		Id:      4,
		Address: "10.0.0.2",
		Votes:   &novotes,
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 2,
		Members: []Member{{
			Id:      4,
			Address: "10.0.0.2",
			Votes:   &novotes,
		}, {
			Id:       5,
			Address:  "10.0.0.3",
			Priority: &one,
			Votes:    &votes,
		}},
	})
}

func (s *changesSuite) TestSetRemoves(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      1,
			Address: "10.0.0.1",
		}},
	})
}

func (s *changesSuite) TestSetUpdateAndRemoves(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      1,
		Address: "10.0.0.3",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 2,
		Members: []Member{{
			Id:      1,
			Address: "10.0.0.3",
		}},
	})
}

func (s *changesSuite) TestRepair(c *gc.C) {
	s.setupRepairStatus(c)

	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      666,
			Address: "10.0.0.2",
		}},
	})
	c.Assert(m.repaired, jc.IsTrue)
}

func (s *changesSuite) TestRepairQuorumCheckFailureMessage(c *gc.C) {
	s.setupRepairStatus(c)

	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}
	m := &mockSession{
		reconfigErr: &mgo.QueryError{
			Message: "Quorum check failed because not enough voting nodes responded",
		},
	}
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      666,
			Address: "10.0.0.2",
		}},
	})
	c.Assert(m.repaired, jc.IsTrue)
}

func (s *changesSuite) TestRepairDuringUpdate(c *gc.C) {
	s.setupRepairStatus(c)

	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}
	m := &mockSession{
		// Fail the update reconfig, before the remove step is reached.
		failWhen: func(cfg Config) bool { return len(cfg.Members) == 2 },
	}
	s.patchConfigFromSession(c, m)
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.3",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 2,
		Members: []Member{{
			Id:      666,
			Address: "10.0.0.3",
		}},
	})
	c.Assert(m.repaired, jc.IsTrue)
}

func (s *changesSuite) TestRepairReturnsContextWhenRetryLosesQuorum(c *gc.C) {
	// Report member 1 as persistently down and member 666 as healthy.
	s.setupRepairStatus(c)

	// Start with the dead removal target and the member whose address changes.
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}

	// Lose quorum on the initial update and again when it is retried after repair.
	m := &mockSession{
		failWhen:      func(cfg Config) bool { return len(cfg.Members) == 2 },
		retryFailWhen: func(cfg Config) bool { return len(cfg.Members) == 1 },
	}

	// Make the retry load the configuration written by the forced repair.
	s.patchConfigFromSession(c, m)

	// Request the surviving member's address update while removing the dead member.
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.3",
	}}
	err := Set(m, wantMembers)

	// Return useful public context instead of exposing the internal sentinel.
	c.Assert(err, gc.ErrorMatches, `cannot apply Set changes after repairing replicaset: repair needed: Quorum check failed because not enough voting nodes responded`)
	c.Assert(errors.Is(err, repairNeeded), jc.IsFalse)
	c.Assert(m.repaired, jc.IsTrue)
}

func (s *changesSuite) TestRepairDuringMultipleUpdates(c *gc.C) {
	// Configure one dead removal target and two members whose addresses change.
	s.setupRepairStatus(c)
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}, {
		Id:      667,
		Address: "10.0.0.3",
	}}

	// Let the first update succeed and make the second update lose quorum.
	m := &mockSession{
		failWhen: func(cfg Config) bool {
			return cfg.Members[1].Address == "10.0.0.4" && cfg.Members[2].Address == "10.0.0.5"
		},
	}
	// Return the last successful config when repair and retry reload it.
	s.patchConfigFromSession(c, m)

	// Request both address updates while omitting the dead member.
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.4",
	}, {
		Id:      667,
		Address: "10.0.0.5",
	}}
	err := Set(m, wantMembers)

	// The retry must preserve the first update and apply the second one.
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 4,
		Members: []Member{{
			Id:      666,
			Address: "10.0.0.4",
		}, {
			Id:      667,
			Address: "10.0.0.5",
		}},
	})
	// A forced reconfig must have removed the dead target before the retry.
	c.Assert(m.repaired, jc.IsTrue)
}

func (s *changesSuite) TestRepairDuringAdd(c *gc.C) {
	s.setupRepairStatus(c)

	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}
	m := &mockSession{
		// The first add succeeds, the second one loses quorum. The re-drive
		// must skip the already added member and the already removed one.
		failWhen: func(cfg Config) bool { return len(cfg.Members) == 4 },
	}
	s.patchConfigFromSession(c, m)
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.2",
	}, {
		Id:      667,
		Address: "10.0.0.3",
	}, {
		Id:      668,
		Address: "10.0.0.4",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 3,
		Members: []Member{{
			Id:      666,
			Address: "10.0.0.2",
		}, {
			Id:       667,
			Address:  "10.0.0.3",
			Priority: &one,
			Votes:    &votes,
		}, {
			Id:       668,
			Address:  "10.0.0.4",
			Priority: &one,
			Votes:    &votes,
		}},
	})
	c.Assert(m.repaired, jc.IsTrue)
}

func (s *changesSuite) TestRetrySkipsLocalhostEquivalentAdd(c *gc.C) {
	m := &mockSession{}
	err := doReplSetConfigChanges("Add", m, &Config{
		Members: []Member{{
			Id:      1,
			Address: "127.0.0.1",
		}},
	}, nil, []Member{{
		Id:      2,
		Address: "localhost",
	}}, nil)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, gc.IsNil)
}

func (s *changesSuite) TestQuorumCheckFailureTypedNil(c *gc.C) {
	var err *mgo.QueryError
	c.Assert(isQuorumCheckFailure(err), jc.IsFalse)
}

func (s *changesSuite) TestRepairErrorWhenNoTargetRemainsUnavailable(c *gc.C) {
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		return &Status{
			Members: []MemberStatus{{
				Id:      2,
				Healthy: false,
				State:   DownState,
			}},
		}, nil
	})

	err := repairReplicaSet(&mockSession{}, []int{1})
	c.Assert(err, gc.ErrorMatches,
		`cannot repair replicaset: no member of \[1\] remained DOWN or UNKNOWN across all samples, the replicaset may have recovered`)
}

func (s *changesSuite) TestRepairErrorWithNoRemovalTargets(c *gc.C) {
	err := repairReplicaSet(&mockSession{}, nil)
	c.Assert(err, gc.ErrorMatches, `cannot repair replicaset: no removal targets`)
}

func (s *changesSuite) TestRepairErrorGettingStatus(c *gc.C) {
	failure := errors.New("status failed")
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		return nil, failure
	})

	err := repairReplicaSet(&mockSession{}, []int{1})
	c.Check(errors.Cause(err), gc.Equals, failure)
	c.Check(err, gc.ErrorMatches, `getting rs status to assess repair safety: status failed`)
}

func (s *changesSuite) TestRepairRefusedForUnhealthyVoterOutsideTargets(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      666,
		Address: "10.0.0.3",
	}}
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}, {
				// Not one we were asked to remove, so the repair is ambiguous.
				Id:      2,
				Healthy: false,
				State:   DownState,
			}},
		}, nil
	})

	err := repairReplicaSet(&mockSession{}, []int{1})
	c.Assert(err, gc.ErrorMatches,
		`cannot repair replicaset: voters \[2\] are unhealthy but not in the removal targets \[1\]`)
}

func (s *changesSuite) TestRepairRefusedWithoutPrimaryEligibleMember(c *gc.C) {
	// Make the live voters an arbiter and a priority-zero data-bearing member.
	arbiter := true
	zeroPriority := 0.0
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:       2,
		Address:  "10.0.0.2",
		Arbiter:  &arbiter,
		Priority: &zeroPriority,
	}, {
		Id:       666,
		Address:  "10.0.0.3",
		Priority: &zeroPriority,
	}}

	// Report both non-electable voters as healthy and the target as dead.
	statusCalls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		statusCalls++
		return &Status{
			Members: []MemberStatus{{
				Id:      2,
				Healthy: true,
				State:   ArbiterState,
			}, {
				Id:      666,
				Healthy: true,
				State:   SecondaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}},
		}, nil
	})

	// Refuse the force because no live voter can become primary after eviction.
	m := &mockSession{}
	err := repairReplicaSet(m, []int{1})
	c.Assert(err, gc.ErrorMatches,
		`cannot repair replicaset: evicting \[1\] leaves no live primary-eligible member among voters \[2 666\]`)
	// The decision must use all confirmation samples without forcing a reconfig.
	c.Assert(statusCalls, gc.Equals, confirmDeadSamples)
	c.Assert(m.repaired, jc.IsFalse)
}

func (s *changesSuite) TestRepairErrorsWhenNoPrimaryIsElected(c *gc.C) {
	// Leave one normal data-bearing voter after the dead target is evicted.
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}

	// Keep the survivor healthy but secondary through assessment and polling.
	statusCalls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		statusCalls++
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   SecondaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}},
		}, nil
	})

	// A successful force must not be reported as a successful repair without a primary.
	m := &mockSession{}
	err := repairReplicaSet(m, []int{1})
	c.Assert(err, gc.ErrorMatches, `cannot repair replicaset: no primary elected after 5 attempts`)
	// Three assessment samples and five primary polls must have completed.
	c.Assert(statusCalls, gc.Equals, confirmDeadSamples+5)
	c.Assert(m.repaired, jc.IsTrue)
}

func (s *changesSuite) TestRepairEvictsPersistentlyDownAndUnknownTargets(c *gc.C) {
	// Both unavailable members are explicit removal targets, so evicting them
	// leaves the consistently healthy member able to form a majority by itself.
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      666,
		Address: "10.0.0.3",
	}}
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}, {
				Id:      2,
				Healthy: false,
				State:   UnknownState,
			}},
		}, nil
	})

	m := &mockSession{}
	err := repairReplicaSet(m, []int{1, 2})
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.repaired, jc.IsTrue)
	c.Assert(m.cfg.Members, jc.DeepEquals, []Member{{
		Id:      666,
		Address: "10.0.0.3",
	}})
}

func (s *changesSuite) TestRepairRefusedForFlappingVoter(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      666,
		Address: "10.0.0.3",
	}}
	statusCalls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		statusCalls++
		flapping := MemberStatus{
			Id:      2,
			Healthy: true,
			State:   SecondaryState,
		}
		if statusCalls == 2 {
			flapping.Healthy = false
			flapping.State = RecoveringState
		}
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}, flapping},
		}, nil
	})

	m := &mockSession{}
	err := repairReplicaSet(m, []int{1})
	c.Assert(err, gc.ErrorMatches,
		`cannot repair replicaset: evicting \[1\] leaves live voters \[666\] of \[2 666\], but a majority needs 2`)
	c.Assert(m.repaired, jc.IsFalse)
	c.Assert(statusCalls, gc.Equals, 3)
}

func (s *changesSuite) TestNoRepairForTransientlyDownMember(c *gc.C) {
	statusCalls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		member := MemberStatus{
			Id:      1,
			Healthy: false,
			State:   DownState,
		}
		statusCalls++
		if statusCalls > 1 {
			// The member has recovered by the second sample.
			member.Healthy = true
			member.State = SecondaryState
		}
		return &Status{
			Name: "test",
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, member},
		}, nil
	})

	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, gc.ErrorMatches,
		`repairing replicaset after "cannot remove member 1 from replicaset: repair needed: Quorum check failed because not enough voting nodes responded": cannot repair replicaset: no member of \[1\] remained DOWN or UNKNOWN across all samples, the replicaset may have recovered`)
	c.Assert(m.cfg, gc.IsNil)
	c.Assert(m.repaired, jc.IsFalse)
	c.Assert(statusCalls, gc.Equals, 2)
}

func (s *changesSuite) TestRepairEvictsOnlyPersistentlyUnavailableTargets(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      666,
		Address: "10.0.0.3",
	}, {
		Id:      667,
		Address: "10.0.0.4",
	}}
	statusCalls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		statusCalls++
		transient := MemberStatus{
			Id:      2,
			Healthy: false,
			State:   DownState,
		}
		if statusCalls > 1 {
			transient.Healthy = true
			transient.State = SecondaryState
		}
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      667,
				Healthy: true,
				State:   SecondaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}, transient},
		}, nil
	})

	m := &mockSession{}
	err := repairReplicaSet(m, []int{1, 2})
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.repaired, jc.IsTrue)
	c.Assert(m.cfg.Members, gc.HasLen, 3)
	gotIds := make([]int, len(m.cfg.Members))
	for i, member := range m.cfg.Members {
		gotIds[i] = member.Id
	}
	c.Check(gotIds, gc.DeepEquals, []int{2, 666, 667})
}

// patchConfigFromSession makes getCurrentConfig reflect the last reconfig
// applied to the mock session, so a post-repair retry sees the repaired
// config rather than the initial one.
func (s *changesSuite) patchConfigFromSession(c *gc.C, m *mockSession) {
	s.PatchValue(&getCurrentConfig, func(session mgoSession) (*Config, error) {
		if m.cfg == nil {
			return &Config{
				Members: append([]Member(nil), s.current...),
			}, nil
		}
		cfg := *m.cfg
		cfg.Members = append([]Member(nil), m.cfg.Members...)
		return &cfg, nil
	})
}

func (s *changesSuite) setupRepairStatus(c *gc.C) {
	// replSetGetStatus reports every member of the config, so a healthy member
	// that some tests add (667) is listed here too. Tests whose config does not
	// contain it are unaffected, because only config members are considered.
	mockCurrentStatus := func(session mgoSession) (*Status, error) {
		return &Status{
			Name: "test",
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      667,
				Healthy: true,
				State:   SecondaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}},
		}, nil
	}
	s.PatchValue(&getCurrentStatus, mockCurrentStatus)
}

type fmtConfigForLogSuite struct {
	testing.IsolationSuite
}

var _ = gc.Suite(&fmtConfigForLogSuite{})

func (s *fmtConfigForLogSuite) TestSimpleFormatting(c *gc.C) {
	anInt := func(v int) *int {
		return &v
	}
	cfg := &Config{
		Name:    "juju",
		Version: 1,
		Term:    1,
		Members: []Member{{
			Id:      2,
			Address: "192.168.0.10:37017",
			Tags:    map[string]string{"juju-machine-id": "1"},
			Votes:   anInt(1),
		}, {
			Id:      1,
			Address: "192.168.0.9:37017",
			Tags:    map[string]string{"juju-machine-id": "0"},
			Votes:   nil,
		}, {
			Id:      3,
			Address: "192.168.0.27:37017",
			Tags:    map[string]string{"juju-machine-id": "2"},
			Votes:   anInt(0),
		}},
	}
	c.Check(fmtConfigForLog(cfg), gc.Equals, `{
  Name: juju,
  Version: 1,
  Term: 1,
  Protocol Version: 0,
  Members: {
    {1 "192.168.0.9:37017" juju-machine-id:0 voting},
    {2 "192.168.0.10:37017" juju-machine-id:1 voting},
    {3 "192.168.0.27:37017" juju-machine-id:2 not-voting},
  },
}`)
	// no side effect, the config is not sorted:
	c.Check(cfg.Members[0].Id, gc.Equals, 2)
	c.Check(cfg.Members[1].Id, gc.Equals, 1)
	c.Check(cfg.Members[2].Id, gc.Equals, 3)
	cfg2 := &Config{
		Name:            "juju",
		Version:         2,
		Term:            1,
		ProtocolVersion: 1,
		Members:         append([]Member(nil), cfg.Members...),
	}
	cfg2.Members[1].Votes = anInt(0)
	cfg2.Members[2].Votes = anInt(1)
	c.Check(fmtConfigForLog(cfg2), gc.Equals, `{
  Name: juju,
  Version: 2,
  Term: 1,
  Protocol Version: 1,
  Members: {
    {1 "192.168.0.9:37017" juju-machine-id:0 not-voting},
    {2 "192.168.0.10:37017" juju-machine-id:1 voting},
    {3 "192.168.0.27:37017" juju-machine-id:2 voting},
  },
}`)
}

func (s *changesSuite) TestRepairEvictsDeadNonVoter(c *gc.C) {
	// A dead non-voting member can be evicted without affecting the majority,
	// so the repair proceeds even though only two voters remain.
	noVotes := 0
	zeroPriority := 0.0
	s.current = []Member{{
		Id:       1,
		Address:  "10.0.0.1",
		Priority: &zeroPriority,
		Votes:    &noVotes,
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      666,
		Address: "10.0.0.3",
	}}
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      2,
				Healthy: true,
				State:   SecondaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}},
		}, nil
	})

	m := &mockSession{}
	err := repairReplicaSet(m, []int{1})
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.repaired, jc.IsTrue)
	c.Assert(m.cfg.Members, gc.HasLen, 2)
	for _, member := range m.cfg.Members {
		c.Check(member.Id, gc.Not(gc.Equals), 1)
	}
}
