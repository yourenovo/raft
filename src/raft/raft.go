package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//  创建一个新的Raft服务器。
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   询问Raft的当前任期，以及它是否认为自己是领导者
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"awesomeProject1/6.824/src/labgob"
	"awesomeProject1/6.824/src/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type LogEntry struct {
	Commad interface{}
	Term   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//服务器看到的最新期限(在第一次引导时初始化为0，单调递增)
	currentTerm int
	//当前currentime收到的候选人的id
	votedFor int
	//日志条目;每个条目包含状态机的命令，以及leader接收条目的term(第一个索引为1)。
	log []LogEntry
	//已知提交的最高日志项的索引(初始化为0，单调递增)
	commitIndex int
	//应用于状态机的最高日志项的索引(初始化为0，单调递增)
	lastApplied int
	//对于每个服务器，要发送到该服务器的下一个日志条目的索引(初始化为leader最后一个日志索引+ 1)
	nextIndex []int
	//对于每个服务器，已知在服务器上复制的最高日志条目的索引(初始化为0，单调递增)
	matchIndex []int
	//0表示follower 1表示候选 2表示leader
	state int
	//表示上次指令到达的时间
	lastTime time.Time
	//得票数
	voteNum int
	//
	applych chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == 2
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = log
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//候选人的term
	Term int
	//要求投票的候选人id
	CandidateId int
	//候选人最后一次日志记录的索引
	LastLogIndex int
	//候选人最后一次日志记录的term
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	//currentTerm用于候选者自我更新
	Term int
	//True表示候选人获得了选票
	VoteGranted bool
	//判断对方是否是leader
	IsLeader bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	//对发过来的args在该服务器进行处理，返回值放置reply体中
	//如果是sever是leader直接返回,如果term < currentTerm，则返回false
	rf.mu.Lock()
	term := args.Term
	candidataId := args.CandidateId
	LastLogIndex := args.LastLogIndex
	LastLogTerm := args.LastLogTerm

	//if rf.state == 2 {
	//	return
	//}
	//如果term < currentTerm，则返回false
	//if args.Term < rf.currentTerm {
	//	reply.VoteGranted = false
	//	reply.Term, reply.IsLeader = rf.GetState()
	//	return
	//}
	//如果term > currentTerm，则更新currentTerm，并转换为follower
	if term > rf.currentTerm {
		rf.state = 0
		rf.votedFor = candidataId
	}
	reply.Term = rf.currentTerm
	//如果votedFor为null或candidateId，并且候选人的日志至少与接收者的日志一样最新，则授予投票
	//最新的条件，最后一条目录的term要大于该rf的最后一条目录任期，或者等于的情况下，index要更大
	if term == rf.currentTerm && rf.votedFor == candidataId {
		if LastLogTerm > rf.log[len(rf.log)-1].Term {
			reply.VoteGranted = true
		} else if LastLogTerm == rf.log[len(rf.log)-1].Term && LastLogIndex >= len(rf.log)-1 {
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}

	if reply.VoteGranted {
		//收到call更新时间
		rf.lastTime = time.Now()
	}

	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	//fmt.Println("接受append")

	//1. 如果term < currentTerm(§5.1)则返回false
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = 0
	}
	if args.Term == rf.currentTerm {
		//fmt.Println("appendEntries", rf.me)
		rf.state = 0
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.lastTime = time.Now()
		//如果日志中不包含与prevLogTerm(§5.3)匹配的项，则返回false
		//如果一个已存在的表项与一个新的表项冲突(相同的索引但不同的术语)，删除已存在的表项和它后面的所有表项(§5.3)。
		if len(rf.log) <= args.PrevLogIndex {
			reply.Success = false
		} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			rf.log = rf.log[:args.PrevLogIndex+1]
		} else {
			rf.log = rf.log[:args.PrevLogIndex+1]
			for _, entry := range args.Entries {
				rf.log = append(rf.log, entry)
				fmt.Println("append log follower", "Term:", rf.currentTerm, "Id:", rf.me, "Leader:", args.LeaderId, "Index:", args.PrevLogIndex+1, "Log.Term:", entry.Term, "Command:", entry.Commad, "log.len:", len(rf.log))
			}

			//如果leaderCommit > commitIndex，则设置commitIndex = min(leaderCommit，最后一个新条目的索引)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			}
			reply.Success = true
		}

	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	//添加条目发送apply
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i = i + 1 {
		msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Commad, CommandIndex: i}
		rf.lastApplied++
		rf.applych <- msg
		//fmt.Println("apply log follower", rf.currentTerm, rf.me, i, rf.log[i].Command)
	}

	rf.persist()
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
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
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	//紧接在新条目之前的日志条目的索引
	PrevLogIndex int
	//prevLogIndex的term
	PrevLogTerm int
	//要存储的日志条目(心跳为空;
	Entries []LogEntry
	//领导人的commitIndex
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term int
	//如果follower包含匹配prevLogIndex和prevLogTerm的条目，则为true
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

// 使用Raft的服务（例如，kv服务器）希望开始就要附加到Raft的日志的下一个命令达成一致。
// 如果此服务器不是领导者，则返回false。
// 否则，启动协议并立即返回。
// 不能保证这个命令会被提交给raft日志，因为领导人可能会失败或输掉选举。
// 即使Raft实例已被终止，此函数也应正常返回。
// 第一个返回值是如果命令被提交，它将出现在的索引。
// 第二个返回值是当前项。如果此服务器认为它是领导者，
// 则第三个返回值为true。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	state := rf.state
	if state != 2 {
		rf.mu.Unlock()
		return 0, rf.currentTerm, false
	}
	index = len(rf.log)
	term = rf.currentTerm
	fmt.Println("start leader", rf.currentTerm, rf.me, index, command)

	//leader将命令作为新条目追加到其日志中，
	rf.log = append(rf.log, LogEntry{Commad: command, Term: rf.currentTerm})
	//然后并行地向其他每个服务器发出AppendEntries rpc以复制该条目。
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(x int) {
		submit:
			rf.mu.Lock()
			if rf.state != 2 {
				rf.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[x] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[x]-1].Term,
				//发送follower对应的nextIndex日志
				Entries:      rf.log[rf.nextIndex[x]:],
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{
				Term:    0,
				Success: false,
			}
			re_send := false
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(x, &args, &reply)
			if !ok {
				time.Sleep(30 * time.Millisecond)
				goto submit
			}
			//如果成功:更新追随者的nextIndex和matchIndex(§5.3)
			rf.mu.Lock()
			if rf.state == 2 && rf.currentTerm == reply.Term && reply.Success == true {
				rf.nextIndex[x] = len(rf.log)
				rf.matchIndex[x] = len(rf.log) - 1
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = 0
				rf.votedFor = 0
			} else {
				if rf.nextIndex[x] > 1 {
					re_send = true
					rf.nextIndex[x]--
				}
			}
			rf.mu.Unlock()
			if re_send {
				goto submit
			}
		}(i)
	}
	rf.persist()
	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
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
	// Your initialization code here (2A, 2B, 2C).
	//服务器看到的最新期限(在第一次引导时初始化为0，单调递增)
	rf.currentTerm = 0
	rf.votedFor = -1
	//已知提交的最高日志项的索引(初始化为0，单调递增)
	rf.commitIndex = 0
	//应用于状态机的最高日志项的索引(初始化为0，单调递增)
	rf.lastApplied = 0
	//对于每个服务器，要发送到该服务器的下一个日志条目的索引(初始化为leader最后一个日志索引+ 1)
	//rf.nextIndex = append(rf.nextIndex)
	//对于每个服务器，已知在服务器上复制的最高日志条目的索引(初始化为0，单调递增)
	//rf.matchIndex = make([]int, 100)
	//0表示follower 1表示候选 2表示leader
	rf.state = 0
	//初始化上次指令最晚时间
	rf.lastTime = time.Now()
	//初始化得票数
	rf.voteNum = 0
	//日志初始化,第一个日志存储空值，最后一个索引要-1
	rf.log = append(rf.log, LogEntry{
		Commad: nil,
		Term:   0,
	})
	//chan初始化
	rf.applych = applyCh
	//leader初始化nextIndex[]数组
	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.readPersist(persister.ReadRaftState())
	//修改 Make()以创建一个后台 goroutine，
	//领导者发送心跳
	go func() {
		for {
			for rf.state != 2 {
				time.Sleep(time.Millisecond * 10)
			}

			for i := 0; i < len(peers); i++ {
				if i == rf.me {
					continue
				}

				go func(x int) {
					//初始化args
					rf.mu.Lock()
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.commitIndex,
						//PrevLogTerm:  rf.log[rf.commitIndex-1].Term,
						PrevLogTerm:  rf.log[rf.commitIndex].Term,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}

					reply := AppendEntriesReply{
						Term:    rf.commitIndex,
						Success: false,
					}
					rf.mu.Unlock()
					//fmt.Println("我的状态", rf.state)
					//fmt.Println("发给心跳", i)
					rf.sendAppendEntries(x, &args, &reply)
					//心跳失败
					if !reply.Success {
						rf.mu.Lock()
						if rf.currentTerm < reply.Term {
							//fmt.Println("退出选举")
							rf.currentTerm = reply.Term
							rf.state = 0
							rf.votedFor = 0
						}
						rf.mu.Unlock()
					}
				}(i)
			}
			time.Sleep(time.Millisecond * 150)
		}
	}()
	//选举go
	go func() {
		//当它有一段时间没有收到其他对等点的消息时，
		for {
			time.Sleep(time.Millisecond * 20)
			if rf.state == 2 {
				continue
			}
			//确保不在同一时间开始选举,超时器启动选举
			if time.Since(rf.lastTime) > time.Duration(rand.Intn(500)+500)*time.Millisecond {
				//它将通过发送RequestVote RPC来定期启动领导者选举
				//开启选举，给除开自己以外的server发送requestVote信息
				//fmt.Println("开始选举", me)
				rf.mu.Lock()
				rf.state = 1
				rf.lastTime = time.Now()
				//获得自己的选票
				rf.voteNum = 1
				rf.currentTerm++
				for i := 0; i < len(peers); i++ {
					if i == rf.me {
						continue
					}
					go func(x int) {
						rf.mu.Lock()

						//初始化args
						args := RequestVoteArgs{
							Term:         rf.currentTerm,
							CandidateId:  rf.me,
							LastLogIndex: rf.commitIndex,
							LastLogTerm:  rf.log[rf.commitIndex].Term,
						}
						reply := RequestVoteReply{
							Term:        0,
							VoteGranted: false,
						}
						//fmt.Println("请求票")
						rf.mu.Unlock()
						rf.sendRequestVote(x, &args, &reply)
						//接受选票在requestVote中运行
						//多方选举发生脑裂
						rf.mu.Lock()

						if reply.Term > rf.currentTerm || reply.IsLeader {
							//fmt.Println("退出选举", rf.me)
							//退出选举
							rf.currentTerm = reply.Term
							rf.state = 0
							//重置选票
							rf.voteNum = 0
							rf.lastTime = time.Now()
							//time.Sleep(time.Millisecond * 50)
							return
						}
						if reply.VoteGranted {
							//获得call方的选票
							//fmt.Println("获得票：：", i)
							rf.voteNum++
						}
						rf.mu.Unlock()
					}(i)

				}
				rf.mu.Unlock()
			}
			//处理选举结构
			//如果选举失败，超出选举时间
			//如果 Candidate 在一定时间内没有获得足够的投票，那么就会进行一轮新的选举，直到其成为 Leader
			//或者其他结点成为了新的 Leader，自己变成 Follower。
			for rf.voteNum < len(rf.peers)/2+1 && rf.state == 1 {
				time.Sleep(15 * time.Millisecond)
				if time.Since(rf.lastTime) > 500*time.Millisecond {
					break
				}
			}
			rf.mu.Lock()
			if rf.voteNum >= len(rf.peers)/2+1 && rf.state == 1 {
				rf.state = 2
				//选举成功修改同步数组
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
				rf.lastTime = time.Now()
			}
			rf.mu.Unlock()
		}
	}()
	//领导者维护commit指令

	go func() { // 收集commitIndex
		for {
			time.Sleep(100 * time.Millisecond)
			if rf.state != 2 {
				continue
			}

			rf.mu.Lock()
			ready := 0
			for i := 0; i < len(rf.peers); i = i + 1 {
				if rf.matchIndex[i] > ready {
					ready = rf.matchIndex[i]
				}
			}

			for i := rf.commitIndex + 1; i <= ready; i = i + 1 {
				count := 1

				for j := 0; j < len(rf.peers); j = j + 1 {
					if j == rf.me {
						continue
					}
					if rf.matchIndex[j] >= i {
						count++
					}
				}
				if count >= len(rf.peers)/2+1 && rf.log[i].Term == rf.currentTerm {
					fmt.Println("update commitindex", len(peers), "Term:", rf.currentTerm, "LeaderId", me, "Index:", i, "count:", count)
					rf.commitIndex = i
				}
			}

			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Commad, CommandIndex: i}
				rf.lastApplied++
				rf.applych <- msg
				//fmt.Println("apply log leader", rf.currentTerm, rf.me, i, rf.log[i].Command)
			}
			rf.mu.Unlock()
		}
	}()
	// 从崩溃前状态初始化
	//rf.readPersist(persister.ReadRaftState())
	return rf
}
