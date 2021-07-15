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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/log"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated

//每一条日志Entry需要经过unstable、stable、committed、applied、compacted五个阶段，接下来总结一下日志的状态转换过程：
//
// 1. 刚刚收到一条日志会被存储在unstable中，日志在没有被持久化之前如果遇到了换届选举，这个日志可能会被相同索引值的新日志覆盖，这个一点可以在raftLog.maybeAppend()和unstable.truncateAndAppend()找到相关的处理逻辑。
// 2. unstable中存储的日志会被使用者写入持久存储（文件）中，这些持久化的日志就会从unstable转移到MemoryStorage中。读者可能会问MemoryStorage并不是持久存储啊，其实日志是被双写了，文件和MemoryStorage各存储了一份，而raft包只能访问MemoryStorage中的内容。这样设计的目的是用内存缓冲文件中的日志，在频繁操作日志的时候性能会更高。此处需要注意的是，MemoryStorage中的日志仅仅代表日志是可靠的，与提交和应用没有任何关系。
// 3. leader会搜集所有peer的接收日志状态，只要日志被超过半数以上的peer接收，那么就会提交该日志，peer接收到leader的数据包更新自己的已提交的最大索引值，这样小于等于该索引值的日志就是可以被提交的日志。
// 4. 已经被提交的日志会被使用者获得，并逐条应用，进而影响使用者的数据状态。
// 5. 已经被应用的日志意味着使用者已经把状态持久化在自己的存储中了，这条日志就可以删除了，避免日志一直追加造成存储无限增大的问题。不要忘了所有的日志都存储在MemoryStorage中，不删除已应用的日志对于内存是一种浪费，这也就是日志的compacted。

type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// stabled = offset -1
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	raftLog := &RaftLog{storage: storage}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	// Initialize our committed and applied pointers to the time of the last compaction.
	h, _, _ := storage.InitialState()
	if h.Commit != 0 {
		raftLog.committed = h.Commit
	} else {
		raftLog.committed = firstIndex - 1
	}
	raftLog.applied = firstIndex - 1
	raftLog.stabled = lastIndex
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return make([]pb.Entry, 0, 1)
	}
	return l.entries
}

// nextEnts returns all the committed but not applied entries = ( applied , committed ]
// If applied is smaller than the index of snapshot, it returns all committed
// entries after the index of snapshot.
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	firstIndex, _ := l.storage.FirstIndex()
	off := max(l.applied+1, firstIndex)
	if l.committed+1 > off {
		ents, err := l.slice(off, l.committed+1)
		if err != nil {
			panic("unexpected error when getting unapplied entries")
		}
		return ents
	}
	return nil
}

// slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	if (lo == uint64(1) && hi == uint64(0)) || lo >= hi {
		return nil, nil
	}
	var ents []pb.Entry
	if len(l.entries) != 0 {

	} else {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.LastIndex()))
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			panic("entries is unavailable from storage")
		} else if err != nil {
			panic(err)
		}
		ents = storedEnts
	}
	// data start in storage
	if lo < l.stabled+1 {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.stabled+1))
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			panic("entries is unavailable from storage")
		} else if err != nil {
			panic(err)
		}
		// check if ents has reached the demand
		if uint64(len(storedEnts)) < min(hi, l.stabled+1)-lo {
			return storedEnts, nil
		}
		ents = storedEnts
	}
	// some data in unstable entries
	if hi > l.stabled+1 {
		unstable := l.entries[max(lo, l.stabled+1)-l.stabled-1 : hi-l.stabled-1]
		if len(ents) > 0 {
			ents = append(ents, unstable...)
		} else {
			ents = unstable
		}
	}
	return ents, nil
}

// LastIndex return the last index of the log entries
// FirstIndex use l.storage.FirstIndex()
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if ls := len(l.entries); ls != 0 {
		return l.entries[ls-1].Index
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// the valid term range is [index of dummy entry, last index]
	firstIndex, _ := l.storage.FirstIndex()
	dummyIndex := firstIndex - 1
	if i < dummyIndex || i > l.LastIndex() || i == 0 {
		return 0, nil
	}
	if len(l.entries) != 0 {
		unStableIndex := i - l.entries[0].Index
		if 0 <= unStableIndex && unStableIndex <= uint64(len(l.entries)-1) {
			return l.entries[unStableIndex].Term, nil
		}
	}
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

func (l *RaftLog) FirstIndex() uint64 {
	t, _ := l.storage.FirstIndex()
	return t
}

func (l *RaftLog) LastTerm() uint64 {
	t, _ := l.Term(l.LastIndex())
	return t
}

func (l *RaftLog) stableTo(i uint64) {
	if i >= l.stabled+1 {
		l.entries = l.entries[i-l.stabled:]
		l.stabled = i
	}
}

func (l *RaftLog) Entries() []pb.Entry {
	firstIndex, _ := l.storage.FirstIndex()
	g, _ := l.slice(firstIndex, l.LastIndex()+1)
	return g
}
