package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.

import (
	"bytes"
	"cs350/labgob"
	"cs350/labrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // This peer's index into peers[]
	dead              int32               // Set by Kill()
	curterm           int
	votedFor          int
	logs              []Log
	nextIndex         []int
	matchIndex        []int
	myVotes           int
	state             string
	recieveheartbeat  bool
	CommittingCorrect int
	commitIndex       int
	//lastApplied       int
	applyCh chan ApplyMsg

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}
type Log struct {
	Term    int
	Command interface{}
}

const (
	f = "follower"
	c = "candidate"
	l = "leader"
)

type EntriesRPCArgs struct {
	Leterm         int
	LeID           int
	LeprevLogIndex int
	LeprevLogTerm  int
	LeEntries      []Log
	LeCommit       int
}

type EntriesRPCReply struct {
	LeCurterm int
	Success   bool
}

func (rf *Raft) AppendEntries(args *EntriesRPCArgs, reply *EntriesRPCReply) {
	// Your code here (4A, 4B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Leterm < rf.curterm {
		//fmt.Println("args.Leterm < rf.curterm------")
		reply.LeCurterm = rf.curterm
		reply.Success = false

		return
	}

	rf.state = f
	rf.recieveheartbeat = true
	rf.curterm = args.Leterm

	if args.LeprevLogIndex < len(rf.logs)-1 && rf.logs[args.LeprevLogIndex].Term != args.LeprevLogTerm {

		reply.Success = false

		return

	}
	if args.LeprevLogIndex >= len(rf.logs) || rf.logs[args.LeprevLogIndex].Term != args.LeprevLogTerm {
		reply.Success = false

		return
	}

	for _, entry := range args.LeEntries {
		if entry.Command == nil {
			fmt.Println("NIL ALERT ~~~~~~~~~~~~~~~~~~")
		}
	}

	//fmt.Printf("Server %d got append entries request: (%d, %d, %d, %d)\n", rf.me, args.LeID, args.LeprevLogIndex, args.LeprevLogTerm, args.Leterm)

	rf.logs = append(rf.logs[:args.LeprevLogIndex+1], args.LeEntries...)
	rf.persist()
	//fmt.Println("log:", rf.logs, "peer", rf.me, "Entries we are adding:", args.LeEntries, "CI", rf.commitIndex, args.LeprevLogTerm)

	reply.Success = true
	rf.state = f

	for args.LeCommit > rf.commitIndex && rf.commitIndex < len(rf.logs)-1 {
		rf.commitIndex += 1

		rf.applyCh <- ApplyMsg{true, rf.logs[rf.commitIndex].Command, rf.commitIndex}
	}

}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int

	rf.mu.Lock()

	a := rf.state
	term = rf.curterm

	rf.mu.Unlock()

	return term, a == l
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curterm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curterm int
	var votedFor int
	var logs []Log
	if d.Decode(&curterm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		return
	} else {
		rf.curterm = curterm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

// Example RequestVote RPC arguments structure.

type RequestVoteArgs struct {
	Canterm         int
	Canreqv         int
	CanlastLogIndex int
	CanlastLogTerm  int
}

// Example RequestVote RPC reply structure.

type RequestVoteReply struct {
	Cancurterm int
	Canvote    bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()

	if args.Canterm <= rf.curterm {

		reply.Canvote = false

		rf.mu.Unlock()
		return
	}

	//fmt.Println("Candidate", args.Canreqv, "asking", rf.me, "voter has", rf.votedFor, rf.curterm, len(rf.logs)-1)
	//fmt.Println("Candidate", args.Canreqv, "asking", rf.me, "candidate has", args.Canterm, args.CanlastLogIndex, args.CanlastLogTerm)

	lastIndex := len(rf.logs) - 1
	rf.curterm = args.Canterm
	rf.state = f
	rf.persist()
	if (args.CanlastLogTerm > rf.logs[lastIndex].Term) || (args.CanlastLogTerm == rf.logs[lastIndex].Term && args.CanlastLogIndex >= lastIndex) {
		reply.Canvote = true
		rf.curterm = args.Canterm
		rf.votedFor = args.Canreqv
		rf.recieveheartbeat = true
	} else {

		reply.Canvote = false

	}

	rf.mu.Unlock()

}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// Look at the comments in ../labrpc/labrpc.go for more details.

func (rf *Raft) sendAppendEntries(server int, args *EntriesRPCArgs, reply *EntriesRPCReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppend(n int, s int, e int) bool {
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	if state == f {
		return true
	}
	rf.mu.Lock()
	termstore := rf.curterm
	prevIndex := rf.nextIndex[n] - 1
	prevTerm := rf.logs[prevIndex].Term
	entries := make([]Log, 0)
	entries = append(entries, rf.logs[prevIndex+1:]...)
	commit := rf.commitIndex
	args := EntriesRPCArgs{
		Leterm:         termstore,
		LeID:           rf.me,
		LeprevLogIndex: prevIndex,
		LeprevLogTerm:  prevTerm,
		LeEntries:      entries,
		LeCommit:       commit,
	}

	reply := EntriesRPCReply{}
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(n, &args, &reply)
	start := prevIndex + 1
	end := start + len(entries)
	//this means that the append entries has been successfully replicated, and we want to update match index and nextIndex.
	if ok {
		if reply.Success {
			//this is NOT getting updated before leader becomes a follower. FIXXX
			rf.mu.Lock()
			for j := start; j < end; j++ {
				rf.matchIndex[j] += 1
			}
			rf.nextIndex[n] = end
			rf.mu.Unlock()

		} else {
			rf.mu.Lock()
			if reply.LeCurterm > rf.curterm {
				rf.state = f
				//fmt.Println(rf.me, "am updating my term", rf.curterm, "to", reply.LeCurterm, rf.state)
				rf.curterm = reply.LeCurterm
				rf.votedFor = -1
				//return ok
			}

			if s > 1 && rf.state != f {
				rf.nextIndex[n] = prevIndex
			}
			rf.mu.Unlock()
			return rf.sendAppend(n, s-1, e)
		}
	}
	return ok

}

// func (rf *Raft) Election() {

// }

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	rf.mu.Lock()
	lockstore := rf.state
	rf.mu.Unlock()

	if lockstore == l {
		rf.mu.Lock()
		isLeader = true
		term = rf.curterm
		index = len(rf.logs)
		logs := rf.logs
		rf.nextIndex[rf.me]++
		rf.logs = append(logs, Log{term, command})
		rf.matchIndex = append(rf.matchIndex, 1)
		rf.persist()
		rf.mu.Unlock()

	}

	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		rf.mu.Lock()
		check := rf.state

		rf.mu.Unlock()

		peertimer := time.Duration(300+rand.Intn(201)) * time.Millisecond

		if check == l {
			rf.mu.Lock()
			// fmt.Println(rf.logs)
			// fmt.Println(rf.matchIndex)
			// fmt.Println(rf.nextIndex)
			store := rf.me
			if rf.commitIndex < (len(rf.logs)-1) && rf.commitIndex+1 < (len(rf.matchIndex)) && rf.logs[rf.commitIndex+1].Term != rf.curterm {
				for rf.CommittingCorrect < len(rf.logs) && rf.CommittingCorrect < len(rf.matchIndex) && rf.logs[rf.CommittingCorrect].Term == rf.curterm && rf.matchIndex[rf.CommittingCorrect] >= (len(rf.peers)/2+1) {
					for rf.commitIndex < rf.CommittingCorrect {
						rf.commitIndex += 1

						msg := ApplyMsg{
							CommandValid: true,
							Command:      rf.logs[rf.commitIndex].Command,
							CommandIndex: rf.commitIndex,
						}

						rf.applyCh <- msg
					}
					rf.CommittingCorrect += 1
				}
			} else {
				for rf.commitIndex < len(rf.matchIndex)-1 && rf.matchIndex[rf.commitIndex+1] > len(rf.peers)/2 {
					rf.commitIndex += 1

					msg := ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[rf.commitIndex].Command,
						CommandIndex: rf.commitIndex,
					}

					rf.applyCh <- msg

				}

			}

			rf.mu.Unlock()

			//this is the leader sending heartbeats
			for i := range rf.peers {
				if i != store {
					rf.mu.Lock()
					state := rf.state
					rf.mu.Unlock()
					if state == f {
						break
					}
					rf.mu.Lock()
					start := rf.nextIndex[i]
					end := len(rf.logs)
					rf.mu.Unlock()
					go rf.sendAppend(i, start, end)

				}

			}
			//we want to wait every 100 milliseconds after the leader sends heartbeats
			time.Sleep(100 * time.Millisecond)
			//everyone starts off as follower
		} else if check == f {

			//there's a timer to check if there's heartbeat, we a decrementing this timer every 10 milliseconds. resetting when get a heartbeat
			for peertimer > 0 {

				time.Sleep(10 * time.Millisecond)
				peertimer -= (10 * time.Millisecond)

				rf.mu.Lock()
				//if you recieve a heartbeat by the leader, then you restart your electiontimer. (Leader hasn't been killed yet)
				if rf.recieveheartbeat {
					peertimer = time.Duration(300+rand.Intn(201)) * time.Millisecond
					rf.recieveheartbeat = false

				}
				rf.mu.Unlock()
			}
			rf.mu.Lock()
			rf.state = c
			check = c
			rf.mu.Unlock()

		} else if check == c {

			//send out votes since you're a canidate, it also takes into account duplicate canidates since the state won't change.
			electiontimer := time.Duration(300+rand.Intn(201)) * time.Millisecond

			rf.mu.Lock()

			rf.curterm += 1

			rf.votedFor = rf.me
			rf.myVotes = 1
			rf.persist()
			rf.mu.Unlock()

			for i := range rf.peers {

				if i != rf.me {
					args := RequestVoteArgs{
						Canterm:         rf.curterm,
						Canreqv:         rf.me,
						CanlastLogIndex: len(rf.logs) - 1,
						CanlastLogTerm:  rf.logs[len(rf.logs)-1].Term,
					}
					// fmt.Printf("Server %d, args:\n", rf.me)
					// fmt.Println(args)

					go func(i int, a *RequestVoteArgs) {

						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(i, a, &reply)

						if ok {
							rf.mu.Lock()

							//adding up vote when the reply from follower is true
							if reply.Canvote {
								rf.myVotes += 1
							}
							rf.mu.Unlock()
						}

					}(i, &args)
				}

			}

			for electiontimer > 0 {
				//this timer is checking to see if we are the winner. If it its, we are leader, if runs out, restart
				time.Sleep(10 * time.Millisecond)
				electiontimer -= 10 * time.Millisecond
				rf.mu.Lock()
				didSomething := false
				if rf.recieveheartbeat {
					rf.state = f
					rf.votedFor = -1
					didSomething = true
				} else if rf.myVotes > (len(rf.peers) / 2) {
					rf.state = l

					rf.matchIndex = make([]int, len(rf.logs))
					for i := 0; i < len(rf.logs); i++ {
						rf.matchIndex[i] = 0
					}

					//For each new leader, they will have their own nextIndex and matchIndex
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.logs)
					}
					rf.votedFor = -1
					didSomething = true
					rf.CommittingCorrect = len(rf.logs)
				}
				rf.mu.Unlock()
				if didSomething {
					break
				}

			}

		}

	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.curterm = 0
	rf.votedFor = -1
	rf.state = f
	rf.myVotes = 1
	rf.mu = sync.Mutex{}
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.CommittingCorrect = 0
	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())
	//dummy log since log index starts at 1, but for code we use 0 for everything else
	if len(rf.logs) == 0 {
		rf.logs = append(rf.logs, Log{0, 0})
	}
	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}
