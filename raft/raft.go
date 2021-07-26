// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

var randGen = rand.New(rand.NewSource(time.Now().UnixNano()))

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// maybeUpdate returns false if the given n index comes from an outdated message.
// Otherwise it updates the progress and returns true.
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
	}
	if pr.Next < n+1 {
		pr.Next = n + 1
	}
	return updated
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	transferElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// randomizedElectionTimeout is a random number between
	// [ electionTimeout , 2 * electionTimeout - 1]. It gets resets
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hard, conf, _ := c.Storage.InitialState()
	r := &Raft{
		id:               c.ID,
		Term:             hard.Term,
		Vote:             hard.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		Lead:             None,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}
	if c.peers == nil {
		c.peers = conf.Nodes
	}
	for _, p := range c.peers {
		r.Prs[p] = &Progress{Next: r.RaftLog.LastIndex() + 1}
	}
	// if apply not be set to the record in DB
	// peerMsgHandler will use RaftLog.nextEnts() to get log from ( applied , committed ] to apply log
	// the original value of apply is fistindex-1 which will lead a log be apply twice,and lead serious mistake!!!
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	r.becomeFollower(r.Term, None)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	m := pb.Message{}
	m.To = to
	var ents []pb.Entry
	var erre error
	term, errt := r.RaftLog.Term(pr.Next - 1)
	if errt == nil {
		ents, erre = r.RaftLog.slice(pr.Next, r.RaftLog.LastIndex()+1)
	}
	if errt != nil || erre != nil {
		//TODO:send snapshot
		snapshot, _ := r.RaftLog.storage.Snapshot()
		// When calling Snapshot(), it actually sends a task RegionTaskGen to the region worker.
		// maybe this method is a async method
		for snapshot.Metadata == nil {
			snapshot, _ = r.RaftLog.storage.Snapshot()
		}
		m.MsgType = pb.MessageType_MsgSnapshot
		m.Snapshot = &snapshot
		r.Prs[to].Next = snapshot.Metadata.Index + 1
	} else {
		var ent []*pb.Entry = nil
		for i := range ents {
			ent = append(ent, &ents[i])
		}
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ent
		m.Commit = r.RaftLog.committed
	}
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  commit,
	}
	r.send(m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateFollower || r.State == StateCandidate {
		// tickElection is run by followers and candidates after r.electionTimeout
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
		}
	} else {
		if r.leadTransferee != None {
			r.transferElapsed++
			if r.transferElapsed >= r.electionTimeout*2 {
				r.transferElapsed = 0
				r.leadTransferee = None
			}
		}
		// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
		r.heartbeatElapsed++
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
		}
		if r.State != StateLeader {
			return
		}
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1, Data: nil})
	// update it's sync record
	r.Prs[r.id].maybeUpdate(r.RaftLog.LastIndex())
	r.maybeCommit()
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// if newer term received, should become follower
	if m.Term > r.Term {
		r.leadTransferee = None
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}
	// If an election timeout happened, the node should pass, start election
	if m.MsgType == pb.MessageType_MsgHup && r.State != StateLeader {
		r.becomeCandidate()
		if len(r.Prs) == 1 {
			// under this situation is a stand alone single server
			r.becomeLeader()
			return nil
		} else {
			for id := range r.Prs {
				if id != r.id {
					r.send(pb.Message{Term: r.Term, To: id, MsgType: pb.MessageType_MsgRequestVote, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.LastTerm()})
				}
			}
		}
	}
	// dispose vote
	if m.MsgType == pb.MessageType_MsgRequestVote {
		if (r.Vote == None || m.Term > r.Term || r.Vote == m.From) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse})
			// Only record real votes.
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	}
	switch r.State {
	case StateFollower:
		stepFollower(r, m)
	case StateCandidate:
		stepCandidate(r, m)
	case StateLeader:
		stepLeader(r, m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse})
		return
	}
	//if m.Index < r.RaftLog.committed {
	//	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
	//	return
	//}
	// if the log match
	if i, err := r.RaftLog.Term(m.Index); err == nil && i == m.LogTerm {
		ci := uint64(0)
		lastnewi := m.Index + uint64(len(m.Entries))
		for _, ne := range m.Entries {
			if j, err := r.RaftLog.Term(ne.Index); err == nil && j == ne.Term {
				ci = ne.GetIndex()
				continue
			}
		}
		// m.Entries before ci already in the r.RaftLog , discard it
		var entry []pb.Entry
		for i := range m.Entries {
			if m.Entries[i].Index > ci {
				entry = append(entry, *m.Entries[i])
			}

		}
		// start append maybe truncate
		if len(entry) != 0 {
			after := entry[0].Index
			if len(r.RaftLog.entries) != 0 {
				switch {
				case after == r.RaftLog.entries[len(r.RaftLog.entries)-1].Index+1:
					// after is the next index in the u.entries
					// directly append
					r.RaftLog.entries = append(r.RaftLog.entries, entry...)
				case after <= r.RaftLog.entries[0].Index:
					// The log is being truncated to before our current offset
					// portion, so set the offset and replace the entries
					r.RaftLog.stabled = after - 1
					r.RaftLog.entries = entry
				default:
					// truncate to after and copy to u.entries
					// then append
					t, _ := r.RaftLog.slice(r.RaftLog.entries[0].Index, after)
					r.RaftLog.entries = append(t, entry...)
				}
			} else {
				r.RaftLog.entries = append(r.RaftLog.entries, entry...)
			}
			r.RaftLog.stabled = min(r.RaftLog.stabled, after-1)
		}
		r.RaftLog.committed = max(r.RaftLog.committed, min(lastnewi, m.Commit))
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: lastnewi})
	} else {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex(), Reject: true})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.committed = max(r.RaftLog.committed, min(m.Commit, r.RaftLog.LastIndex()))
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}
	r.becomeFollower(max(r.Term, m.Snapshot.Metadata.Index), m.From)
	if len(r.RaftLog.entries) > 0 {
		r.RaftLog.entries = nil
	}
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{Next: r.RaftLog.LastIndex() + 1}
	}
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex()})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: 1}
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		if r.State == StateLeader {
			r.maybeCommit()
		}
	}
	r.PendingConfIndex = None
}

// reset state when identity changed
func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + randGen.Intn(r.electionTimeout)
	//TODO:resetRandomizedElectionTimeout
	r.votes = make(map[uint64]bool)
	for id, pr := range r.Prs {
		*pr = Progress{Next: r.RaftLog.LastIndex() + 1}
		if id == r.id {
			pr.Match = r.RaftLog.LastIndex()
		}
	}
}

// send persists state to stable storage and then sends to its mailbox.
func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}
		// do not attach term to MsgProp, MsgReadGIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		m.Term = r.Term
	}
	r.msgs = append(r.msgs, m)
}

func stepLeader(r *Raft, m pb.Message) {
	// These message types do not require any progress for m.From.
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for id := range r.Prs {
			if r.id != id {
				commit := min(r.Prs[id].Match, r.RaftLog.committed)
				m := pb.Message{
					To:      id,
					MsgType: pb.MessageType_MsgHeartbeat,
					Commit:  commit,
				}
				r.send(m)
			}
		}
		return
	case pb.MessageType_MsgPropose:
		// when transfer leader, the server can't write
		if r.leadTransferee != None {
			return
		}
		if _, ok := r.Prs[r.id]; !ok {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return
		}
		for _, entry := range m.Entries {
			if entry.EntryType == pb.EntryType_EntryConfChange {
				if r.PendingConfIndex != None {
					continue
				}
				r.PendingConfIndex = entry.Index
			}
		}
		var entry []pb.Entry
		li := r.RaftLog.LastIndex()
		for i := range m.Entries {
			m.Entries[i].Term = r.Term
			m.Entries[i].Index = li + 1 + uint64(i)
			entry = append(entry, *m.Entries[i])
		}
		r.RaftLog.entries = append(r.RaftLog.entries, entry...)
		r.Prs[r.id].maybeUpdate(r.RaftLog.LastIndex())
		r.maybeCommit()
		for id := range r.Prs {
			if r.id != id {
				r.sendAppend(id)
			}
		}
		return
	case pb.MessageType_MsgTransferLeader:
		if m.From == r.id {
			return
		}
		if r.leadTransferee != None && r.leadTransferee == m.From {
			return
		}
		if _, ok := r.Prs[m.From]; !ok {
			return
		}
		r.leadTransferee = m.From
		r.transferElapsed = 0
		if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgTimeoutNow,
				To:      m.From,
			})
		} else {
			r.sendAppend(m.From)
		}
	}

	// All other message types require a progress for m.From (pr).
	pr := r.Prs[m.From]
	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			pr.Next--
			r.sendAppend(m.From)
		} else {
			// update pr info
			pr.maybeUpdate(m.Index)
			// try to advance commit
			//r.maybeCommit()
			if r.maybeCommit() {
				for i := range r.Prs {
					if i != r.id {
						r.sendAppend(i)
					}
				}
			}
		}
		if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgTimeoutNow,
				To:      m.From,
			})
			r.leadTransferee = None
		}
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
}

func stepCandidate(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		//no leader at this term; dropping proposal
		return
	case pb.MessageType_MsgAppend:
		r.becomeFollower(r.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(r.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if _, ok := r.votes[m.From]; !ok {
			r.votes[m.From] = !m.Reject
		}
		granted := 0
		for _, vv := range r.votes {
			if vv {
				granted++
			}
		}
		switch len(r.Prs)/2 + 1 {
		case granted:
			r.becomeLeader()
		case len(r.votes) - granted:
			// The election fails because a quorum of nodes
			// know about the election that already happened at term r.Term. Node's
			// term is pushed ahead to r.Term.
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			//no leader at this term ; dropping proposal
			return
		}
		m.To = r.Lead
		r.send(m)
	}
}

func stepFollower(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			//no leader at this term ; dropping proposal
			return
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			//no leader at this term ; dropping proposal
			return
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgTimeoutNow:
		// when a MessageType_MsgTimeoutNow arrives at
		// a node that has been removed from the group, nothing happens.
		if _, ok := r.Prs[r.id]; !ok {
			return
		}
		r.becomeCandidate()
		if len(r.Prs) == 1 {
			// under this situation is a stand alone single server
			r.becomeLeader()
		} else {
			for id := range r.Prs {
				if id != r.id {
					r.send(pb.Message{Term: r.Term, To: id, MsgType: pb.MessageType_MsgRequestVote, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.LastTerm()})
				}
			}
		}
	}
}

func (l *RaftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && lasti >= l.LastIndex())
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *Raft) maybeCommit() bool {
	mis := make(uint64Slice, 0, len(r.Prs))
	for _, p := range r.Prs {
		mis = append(mis, p.Match)
	}
	sort.Sort(sort.Reverse(mis))
	mci := mis[len(r.Prs)/2]
	// only commit log in leader's term!
	commitTerm, _ := r.RaftLog.Term(mci)
	if mci > r.RaftLog.committed && commitTerm == r.Term {
		r.RaftLog.committed = max(r.RaftLog.committed, mci)
		return true
	}
	return false
}
