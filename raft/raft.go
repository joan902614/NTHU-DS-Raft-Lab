package raft

import (
	"context"
	"time"

	"github.com/justin0u0/raft/pb"
	"go.uber.org/zap"
)

type Raft struct {
	pb.UnimplementedRaftServer

	*raftState
	persister Persister

	id    uint32
	peers map[uint32]Peer

	config *Config
	logger *zap.Logger

	// lastHeartbeat stores the last time of a valid RPC received from the leader
	lastHeartbeat time.Time

	// rpcCh stores incoming RPCs
	rpcCh chan *rpc
	// applyCh stores logs that can be applied
	applyCh chan *pb.Entry
}

var _ pb.RaftServer = (*Raft)(nil)

func NewRaft(id uint32, peers map[uint32]Peer, persister Persister, config *Config, logger *zap.Logger) *Raft {
	raftState := &raftState{
		state:       Follower,
		currentTerm: 0,
		votedFor:    0,
		logs:        make([]*pb.Entry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[uint32]uint64),
		matchIndex:  make(map[uint32]uint64),
	}

	return &Raft{
		raftState:     raftState,
		persister:     persister,
		id:            id,
		peers:         peers,
		config:        config,
		logger:        logger.With(zap.Uint32("id", id)),
		lastHeartbeat: time.Now(),
		rpcCh:         make(chan *rpc),
		applyCh:       make(chan *pb.Entry),
	}
}

// RPC handlers

// follower: reject
// candidate: reject
// leader: add log entry to log
func (r *Raft) applyCommand(req *pb.ApplyCommandRequest) (*pb.ApplyCommandResponse, error) {
	// TODO: (B.1)* - if not leader, reject client operation and returns `errNotLeader`
	if r.state != Leader {
		return nil, errNotLeader
	}
	// TODO: (B.1)* - create a new log entry, append to the local entries
	// Hint:
	// - use `getLastLog` to get the last log ID
	// - use `appendLogs` to append new log
	entry_id, _ := r.getLastLog()
	entry_term := r.currentTerm
	new_entry := &pb.Entry{Id: entry_id + 1, Term: entry_term, Data: req.GetData()}
	var new_logs []*pb.Entry
	new_logs = append(new_logs, new_entry)
	r.appendLogs(new_logs)
	// TODO: (B.1)* - return the new log entry
	return &pb.ApplyCommandResponse{Entry: new_entry}, nil
}

// leader: 1, 2
// candidate: 1, 2
// follower: 1, 3, 4, 5
// 1. reject old term rpc
// 2. change to follower
// 3. update currentTerm
// 4. update entry
// 5. update commitIndex and apply entry in state machine
func (r *Raft) appendEntries(req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	// TODO: (A.1) - reply false if term < currentTerm
	// Log: r.logger.Info("reject append entries since current term is older")
	if req.GetTerm() < r.currentTerm {
		r.logger.Info("reject append entries since current term is older")
		return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: false}, nil
	}

	// TODO: (A.2)* - reset the `lastHeartbeat`
	// Description: start from the current line, the current request is a valid RPC
	r.lastHeartbeat = time.Now()

	// TODO: (A.3) - if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	// Hint: use `toFollower` to convert to follower
	// Log: r.logger.Info("increase term since receive a newer one", zap.Uint64("term", r.currentTerm))
	if req.GetTerm() > r.currentTerm {
		r.toFollower(req.GetTerm())
		r.logger.Info("increase term since receive a newer one", zap.Uint64("term", r.currentTerm))
	}
	// TODO: (A.4) - if AppendEntries RPC received from new leader(many candidate in the same term): convert to follower
	// Log: r.logger.Info("receive request from leader, fallback to follower", zap.Uint64("term", r.currentTerm))
	if req.GetTerm() == r.currentTerm && r.state != Follower {
		r.toFollower(req.GetTerm())
		r.logger.Info("receive request from leader, fallback to follower", zap.Uint64("term", r.currentTerm))
	}

	prevLogId := req.GetPrevLogId()
	prevLogTerm := req.GetPrevLogTerm()
	if prevLogId != 0 && prevLogTerm != 0 {
		// TODO: (B.2) - reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		// Hint: use `getLog` to get log with ID equals to prevLogId
		// Log: r.logger.Info("the given previous log from leader is missing or mismatched", zap.Uint64("prevLogId", prevLogId), zap.Uint64("prevLogTerm", prevLogTerm), zap.Uint64("logTerm", log.GetTerm()))
		if r.getLog(prevLogId).GetTerm() != prevLogTerm {
			r.logger.Info("the given previous log from leader is missing or mismatched", zap.Uint64("prevLogId", prevLogId), zap.Uint64("prevLogTerm", prevLogTerm), zap.Uint64("logTerm", r.getLog(prevLogId).GetTerm()))
			return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: false}, nil
		}
	}
	if len(req.GetEntries()) != 0 {
		// TODO: (B.3) - if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
		// TODO: (B.4) - append any new entries not already in the log
		// Hint: use `deleteLogs` follows by `appendLogs`
		// Log: r.logger.Info("receive and append new entries", zap.Int("newEntries", len(req.GetEntries())), zap.Int("numberOfEntries", len(r.logs)))
		r.deleteLogs(prevLogId)
		r.appendLogs(req.GetEntries())
		r.logger.Info("receive and append new entries", zap.Int("newEntries", len(req.GetEntries())), zap.Int("numberOfEntries", len(r.logs)))
	}

	// TODO: (B.5) - if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// Hint: use `getLastLog` to get the index of last new entry
	// Hint: use `applyLogs` to apply(commit) new logs in background
	// Log: r.logger.Info("update commit index from leader", zap.Uint64("commitIndex", r.commitIndex))
	if req.GetLeaderCommitId() > r.commitIndex {
		lastEntryId, _ := r.getLastLog()
		if req.GetLeaderCommitId() < lastEntryId {
			r.setCommitIndex(req.GetLeaderCommitId())
		} else {
			r.setCommitIndex(lastEntryId)
		}
		r.applyLogs(r.applyCh)
		r.logger.Info("update commit index from leader", zap.Uint64("commitIndex", r.commitIndex))
	}

	return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: true}, nil
}

// leader: 1, 2
// candidate: 1, 2
// follower: 1, 3, 4
// 1. reject old term rpc
// 2. change to follower
// 3. update currentTerm
// 4. voteFor
func (r *Raft) requestVote(req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	// TODO: (A.5) - reply false if term < currentTerm
	// Log: r.logger.Info("reject request vote since current term is older")
	if req.GetTerm() < r.currentTerm {
		r.logger.Info("reject request vote since current term is older")
		return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}, nil
	}

	// TODO: (A.6) - if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	// Hint: use `toFollower` to convert to follower
	// Log: r.logger.Info("increase term since receive a newer one", zap.Uint64("term", r.currentTerm))
	if req.GetTerm() > r.currentTerm {
		r.toFollower(req.GetTerm())
		r.logger.Info("increase term since receive a newer one", zap.Uint64("term", r.currentTerm))
	}

	// TODO: (A.7) - if votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	// Hint: (fix the condition) if already vote for another candidate, reply false
	if r.votedFor != 0 {
		r.logger.Info("reject since already vote for another candidate",
			zap.Uint64("term", r.currentTerm),
			zap.Uint32("votedFor", r.votedFor))
		return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}, nil
	}
	// Hint: (fix the condition) if the local last entry is more up-to-date than the candidate's last entry, reply false
	// Hint: use `getLastLog` to get the last log entry
	lastEntryId, lastEntryTerm := r.getLastLog()
	if (req.GetLastLogTerm() < lastEntryTerm) || (req.GetLastLogTerm() == lastEntryTerm && req.GetLastLogId() < lastEntryId) {
		r.logger.Info("reject since last entry is more up-to-date")
		return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}, nil
	}
	// Hint: now vote should be granted, use `voteFor` to set votedFor
	r.voteFor(req.GetCandidateId(), false)
	r.logger.Info("vote for another candidate", zap.Uint32("votedFor", r.votedFor))

	// TODO: (A.8)* - reset the `lastHeartbeat`
	// Description: start from the current line, the current request is a valid RPC
	r.lastHeartbeat = time.Now()

	return &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: true}, nil
}

// raft main loop
func (r *Raft) Run(ctx context.Context) {
	if err := r.loadRaftState(r.persister); err != nil {
		r.logger.Error("fail to load raft state", zap.Error(err))
		return
	}

	r.logger.Info("starting raft",
		zap.Uint64("term", r.currentTerm),
		zap.Uint32("votedFor", r.votedFor),
		zap.Int("logs", len(r.logs)))

	// raft running
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("raft server stopped gracefully")
			return
		default:
		}

		switch r.state {
		case Follower:
			r.runFollower(ctx)
		case Candidate:
			r.runCandidate(ctx)
		case Leader:
			r.runLeader(ctx)
		}
	}
}

// apply to log machine channel
func (r *Raft) ApplyCh() <-chan *pb.Entry {
	return r.applyCh
}

// follower related

// follower main loop
// setting: timeout cnannel
// action:
// 1. get rpc request
// 2. in HeartbeatTimeout doesn't get rpc, change to candidate
func (r *Raft) runFollower(ctx context.Context) {
	r.logger.Info("running follower")

	// setting timeout
	timeoutCh := randomTimeout(r.config.HeartbeatTimeout)

	for r.state == Follower {
		select {
		case <-ctx.Done(): // shutdown
			return

		case <-timeoutCh: // timeout
			timeoutCh = randomTimeout(r.config.HeartbeatTimeout)

			if time.Now().Sub(r.lastHeartbeat) > r.config.HeartbeatTimeout {
				r.handleFollowerHeartbeatTimeout()
			}

		case rpc := <-r.rpcCh: // get rpc
			r.handleRPCRequest(rpc)
		}
	}
}

func (r *Raft) handleFollowerHeartbeatTimeout() {
	// TODO: (A.9) - if election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
	// Hint: use `toCandidate` to convert to candidate
	r.toCandidate()
	r.logger.Info("heartbeat timeout, change state from follower to candidate")
}

// candidate related

// vote result from other server, result + server id
type voteResult struct {
	*pb.RequestVoteResponse
	peerId uint32
}

// candidate man loop
// setting: vote related varible, requestvote rpc response channel, election timeout channel
// action:
// 1. vote for itself
// 2. send requestvote rpc to all the other server
// 3. result
// 4. get rpc request
func (r *Raft) runCandidate(ctx context.Context) {
	r.logger.Info("running candidate")

	// set votes count inital
	grantedVotes := 0                     // votes which it aleady has
	votesNeeded := (len(r.peers) + 1) / 2 // to win votes count
	// will get vote result(response) from channel
	voteCh := make(chan *voteResult, len(r.peers))
	// set election timeout
	timeoutCh := randomTimeout(r.config.ElectionTimeout)

	// vote for itself
	r.voteForSelf(&grantedVotes)

	// requestvote rpc to peers
	r.broadcastRequestVote(ctx, voteCh)

	// wait until:
	// 1. it wins the election
	// 2. another server establishes itself as leader (see AppendEntries)
	// 3. election timeout
	for r.state == Candidate {
		select {
		case <-ctx.Done(): // shutdown
			return

		case vote := <-voteCh: // get rpc response
			r.handleVoteResult(vote, &grantedVotes, votesNeeded)

		case <-timeoutCh: // timeout election time
			r.logger.Info("election timeout reached, restarting election")
			return

		case rpc := <-r.rpcCh: // get rpc request
			r.handleRPCRequest(rpc)
		}
	}
}

func (r *Raft) voteForSelf(grantedVotes *int) {
	// TODO: (A.10) increment currentTerm
	// TODO: (A.10) voteFor change to its id
	// Hint: use `voteFor` to vote for self
	(*grantedVotes)++
	r.voteFor(r.id, true) // vote to who's id, itself?
	r.logger.Info("vote for self", zap.Uint64("term", r.currentTerm))
}

func (r *Raft) broadcastRequestVote(ctx context.Context, voteCh chan *voteResult) {
	r.logger.Info("broadcast request vote", zap.Uint64("term", r.currentTerm))

	// set requestvote rpc information
	candidateLastLogId, candidateLastLogTerm := r.getLastLog()
	req := &pb.RequestVoteRequest{
		// TODO: (A.11) - set all fields of `req`
		// Hint: use `getLastLog` to get the last log entry
		Term:        r.currentTerm,
		CandidateId: r.id,
		LastLogId:   candidateLastLogId,
		LastLogTerm: candidateLastLogTerm,
	}

	// TODO: (A.11) - send RequestVote RPCs to all other servers (modify the code to send `RequestVote` RPCs in parallel)
	// var wg sync.WaitGroup
	for peerId, peer := range r.peers {
		peerId := peerId
		peer := peer

		// send rpc
		// wg.Add(1)
		go func() {
			// defer wg.Done()
			resp, err := peer.RequestVote(ctx, req)
			if err != nil {
				r.logger.Error("fail to send RequestVote RPC", zap.Error(err), zap.Uint32("peer", peerId))
				return
			}

			voteCh <- &voteResult{RequestVoteResponse: resp, peerId: peerId}
		}()
	}
	// wg.Wait() no be lock at here
	// close(voteCh)
}

// 1. candidate's term < rpc response's term -> follower
// 2. get vote
// 3. if candidate's votes > majority -> leader
func (r *Raft) handleVoteResult(vote *voteResult, grantedVotes *int, votesNeeded int) {
	// TODO: (A.12) - if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	// Hint: use `toFollower` to convert to follower
	// Log: r.logger.Info("receive new term on RequestVote response, fallback to follower", zap.Uint32("peer", vote.peerId))
	if vote.GetTerm() > r.currentTerm {
		r.toFollower(vote.GetTerm())
		r.logger.Info("receive new term on RequestVote response, fallback to follower", zap.Uint32("peer", vote.peerId))
	}

	// candidate get vote
	if vote.VoteGranted {
		(*grantedVotes)++
		r.logger.Info("vote granted", zap.Uint32("peer", vote.peerId), zap.Int("grantedVote", (*grantedVotes)))
	}

	// TODO: (A.13) - if votes received from majority of servers: become leader
	// Log: r.logger.Info("election won", zap.Int("grantedVote", (*grantedVotes)), zap.Uint64("term", r.currentTerm))
	// Hint: use `toLeader` to convert to leader
	if *grantedVotes > votesNeeded {
		r.toLeader()
		r.logger.Info("election won", zap.Int("grantedVote", (*grantedVotes)), zap.Uint64("term", r.currentTerm))
	}
}

// leader related
// appendentry rpc reponse, server id + result + information
type appendEntriesResult struct {
	*pb.AppendEntriesResponse
	req    *pb.AppendEntriesRequest
	peerId uint32
}

// leader main loop
// setting: heartbeat time channel, nextIndex[], matchIndex[], appendentry rpc reponse channel
// 2. handle request, handle response, send heatbeat, append
func (r *Raft) runLeader(ctx context.Context) {
	// setting when to send heartbeat
	timeoutCh := randomTimeout(r.config.HeartbeatInterval)
	// appendentry rpc reponse channel
	appendEntriesResultCh := make(chan *appendEntriesResult, len(r.peers))
	// reset `nextIndex` and `matchIndex`
	lastLogId, _ := r.getLastLog()
	for peerId := range r.peers {
		r.nextIndex[peerId] = lastLogId + 1
		r.matchIndex[peerId] = 0
	}

	for r.state == Leader {
		select {
		case <-ctx.Done(): // shutdown
			return

		case <-timeoutCh: // send heartbeat/appendentry to all the other server
			timeoutCh = randomTimeout(r.config.HeartbeatInterval)
			r.broadcastAppendEntries(ctx, appendEntriesResultCh)

		case result := <-appendEntriesResultCh: // get appendentry rpc response
			r.handleAppendEntriesResult(result)

		case rpc := <-r.rpcCh: // receive rpc request
			r.handleRPCRequest(rpc)
		}
	}
}

func (r *Raft) broadcastAppendEntries(ctx context.Context, appendEntriesResultCh chan *appendEntriesResult) {
	r.logger.Info("broadcast append entries")

	// var wg sync.WaitGroup
	for peerId, peer := range r.peers {
		peerId := peerId
		peer := peer

		// nextindex is leader next send's log entry
		// if the nextIndex's log entry is empty -> heatbeat
		// otherwise -> append entry

		// TODO: (A.14) - send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts
		// Hint: set `req` with the correct fields (entries, prevLogId, prevLogTerm can be ignored for heartbeat)
		entries := r.getLogs(r.nextIndex[peerId])
		req := &pb.AppendEntriesRequest{
			Term:           r.currentTerm,
			LeaderId:       r.id,
			LeaderCommitId: r.commitIndex,
			Entries:        entries,
		}
		// TODO: (B.6) - send AppendEntries RPC with log entries starting at nextIndex
		// Hint: set `req` with the correct fields (entries, prevLogId and prevLogTerm MUST be set)
		// Hint: use `getLog` to get specific log, `getLogs` to get all logs after and include the specific log Id
		// Log: r.logger.Debug("send append entries", zap.Uint32("peer", peerId), zap.Any("request", req), zap.Int("entries", len(entries)))
		if req.GetEntries() != nil {
			req.PrevLogId = r.getLog(r.nextIndex[peerId] - 1).GetId()
			req.PrevLogTerm = r.getLog(r.nextIndex[peerId] - 1).GetTerm()
		} else {
			req.PrevLogId = 0
			req.PrevLogTerm = 0
		}
		r.logger.Debug("send append entries", zap.Uint32("peer", peerId), zap.Any("request", req), zap.Int("entries", len(entries)))

		// TODO: (A.14) & (B.6)
		// Hint: modify the code to send `AppendEntries` RPCs in parallel
		// send appendentry rpc request
		// wg.Add(1)
		go func() {
			// defer wg.Done()
			resp, err := peer.AppendEntries(ctx, req)
			if err != nil {
				r.logger.Error("fail to send AppendEntries RPC", zap.Error(err), zap.Uint32("peer", peerId))
				// connection issue, should not be handled
				return
			}

			// send this appendentry rpc's response to channel
			appendEntriesResultCh <- &appendEntriesResult{
				AppendEntriesResponse: resp,
				req:                   req,
				peerId:                peerId,
			}
		}()
	}
	// wg.Wait()
	// close(appendEntriesResultCh)
}

// 1. discover higher term change into follower
// 2. success append entry rpc: update nextIndex[response server id] = itself + rpc.entry.length, matchIndex[response server id] = nextIndex[response server id] - 1
// 2. fail append entry rpc: update nextIndex[response server id] = itself - 1, matchIndex[response server id] = itself
// 3. handle commit
func (r *Raft) handleAppendEntriesResult(result *appendEntriesResult) {
	// TODO: (A.15) - if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	// Hint: use `toFollower` to convert to follower
	// Log: r.logger.Info("receive new term on AppendEntries response, fallback to follower", zap.Uint32("peer", result.peerId))
	if result.GetTerm() > r.currentTerm {
		r.toFollower(result.GetTerm())
		r.logger.Info("receive new term on AppendEntries response, fallback to follower", zap.Uint32("peer", result.peerId))
	}

	// result update to leader
	// matchIndex: in every server the lastest be replicated log entry index
	entries := result.req.GetEntries()
	if !result.GetSuccess() {
		// TODO: (B.7) - if AppendEntries fails because of log inconsistency: decrease nextIndex and retry
		// Hint: use `setNextAndMatchIndex` to decrease nextIndex
		// Log: logger.Info("append entries failed, decrease next index", zap.Uint64("nextIndex", nextIndex), zap.Uint64("matchIndex", matchIndex))
		nextIndex := r.nextIndex[result.peerId] - 1
		matchIndex := r.matchIndex[result.peerId]
		r.setNextAndMatchIndex(result.peerId, nextIndex, matchIndex)

		r.logger.Info("append entries failed, decrease next index", zap.Uint64("nextIndex", nextIndex), zap.Uint64("matchIndex", matchIndex))
	} else if len(entries) != 0 {
		// TODO: (B.8) - if successful: update nextIndex and matchIndex for follower
		// Hint: use `setNextAndMatchIndex` to update nextIndex and matchIndex
		// Log: logger.Info("append entries successfully, set next index and match index", zap.Uint32("peer", result.peerId), zap.Uint64("nextIndex", nextIndex), zap.Uint64("matchIndex", matchIndex))
		nextIndex := r.nextIndex[result.peerId] + uint64(len(result.req.GetEntries()))
		matchIndex := nextIndex - 1
		r.setNextAndMatchIndex(result.peerId, nextIndex, matchIndex)
		r.logger.Info("append entries successfully, set next index and match index", zap.Uint32("peer", result.peerId), zap.Uint64("nextIndex", nextIndex), zap.Uint64("matchIndex", matchIndex))
	}

	// commit log entry
	majority := (len(r.peers) + 1) / 2
	uncommitLogs := r.getLogs(r.commitIndex + 1) // all of not commit entry in leader
	// find commit possible entry from highest entry
	// its index bigger then commitIndex -> before commitIndex already commit
	// half of matchIndex[i] bigger than its index -> more than half server has replicate that entry
	// term equal to currentTerm -> leader's create
	for i := len(uncommitLogs) - 1; i >= 0; i-- {
		// TODO: (B.9) if there exist an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N
		// Hint: find if such N exists
		// Hint: if such N exists, use `setCommitIndex` to set commit index
		// Hint: if such N exists, use `applyLogs` to apply logs
		replicas := 1 // leader itself
		// check every server
		for serverId, _ := range r.peers {
			if r.matchIndex[serverId] >= uncommitLogs[i].GetId() && uncommitLogs[i].GetTerm() == r.currentTerm {
				replicas++
			}
		}
		// set commitId, apply commit entry to leader's state machine
		if replicas > majority {
			r.setCommitIndex(uncommitLogs[i].GetId())
			r.applyLogs(r.applyCh)
			break
		}
	}
}
