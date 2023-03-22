package raft

import "time"

func canVote(votedFor int, candidateId int, candidateTerm int, candidateIndex int, term int, index int) bool {
	if !(votedFor == -1 || votedFor == candidateId) {
		return false
	}
	return isMoreUpToDate(candidateTerm, candidateIndex, term, index)
}

func (rf *Raft) leaderHang(lastRecvHeartBeat int64) bool {
	nowMills := time.Now().UnixMilli()
	if nowMills-int64(lastRecvHeartBeat) > ElectionTimeout {
		return true
	}
	return false
}
