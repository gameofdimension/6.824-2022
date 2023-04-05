package shardctrler

import (
	"fmt"
)

func (sc *ShardCtrler) applier() {
	for m := range sc.applyCh {
		if m.SnapshotValid {
		} else if m.CommandValid {
			sc.mu.Lock()
			if m.CommandIndex <= sc.lastApplied {
				panic(fmt.Sprintf("index error %d vs %d", m.CommandIndex, sc.lastApplied))
			}
			sc.lastApplied = m.CommandIndex
			op := m.Command.(Op)
			clientId, seq := op.ClientId, op.Seq
			lastSeq, ok := sc.clientSeq[clientId]
			// new op from clientId
			DPrintf("server %d receive op %v at index %d", sc.me, op, m.CommandIndex)
			if !ok || seq != lastSeq {
				DPrintf("server %d will run op %v at index %d", sc.me, op, m.CommandIndex)
				if seq < lastSeq {
					panic(fmt.Sprintf("smaller seq %d vs %d for %d", seq, lastSeq, clientId))
				}
				if op.Type == OpQuery {
					args := op.Args.(QueryArgs)
					version := args.Num
					if version == -1 {
						version = len(sc.configs) - 1
					}
					if version < len(sc.configs) && version > 0 {
						sc.cache[clientId] = sc.configs[version]
					} else {
						sc.cache[clientId] = nil
					}
				} else if op.Type == OpJoin {
					args := op.Args.(JoinArgs)
					config := sc.makeConfigForJoin(&args)
					DPrintf("server %d latest config after join %v", sc.me, config)
					if config != nil {
						sc.configs = append(sc.configs, *config)
					}
					sc.cache[clientId] = true
				} else if op.Type == OpLeave {
					args := op.Args.(LeaveArgs)
					config := sc.makeConfigForLeave(&args)
					DPrintf("server %d latest config after leave %v", sc.me, config)
					sc.configs = append(sc.configs, *config)
					sc.cache[clientId] = true
				} else if op.Type == OpMove {
					args := op.Args.(MoveArgs)
					config := sc.makeConfigForMove(&args)
					DPrintf("server %d latest config after move %v", sc.me, config)
					sc.configs = append(sc.configs, *config)
					sc.cache[clientId] = true
				}
				sc.clientSeq[clientId] = seq
			}
			sc.mu.Unlock()
		}
	}

}

func (sc *ShardCtrler) makeConfigForJoin(args *JoinArgs) *Config {
	// sc.mu.Lock()
	// defer sc.mu.Unlock()
	version := len(sc.configs)
	groups := make(map[int][]string)
	last := sc.configs[version-1]
	// if len(last.Groups) >= NShards {
	// return nil
	// }
	for k, v := range last.Groups {
		groups[k] = v
	}
	for k, v := range args.Servers {
		groups[k] = v
		// if !SliceContains(last.Shards[:], k) {
		// gids = append(gids, k)
		// }
	}

	gids := []int{}
	for k, v := range groups {

	}

	shards := add(last.Shards[:], gids)
	config := Config{
		Num:    version,
		Groups: groups,
		Shards: SliceToArr(shards),
	}
	return &config
}

func (sc *ShardCtrler) makeConfigForLeave(args *LeaveArgs) *Config {
	// sc.mu.Lock()
	// defer sc.mu.Unlock()
	version := len(sc.configs)
	groups := make(map[int][]string)
	last := sc.configs[version-1]
	for k, v := range last.Groups {
		if !SliceContains(args.GIDs, k) {
			groups[k] = v
		}
	}
	dedup := []int{}
	for _, gid := range args.GIDs {
		if SliceContains(last.Shards[:], gid) {
			dedup = append(dedup, gid)
		}
	}
	shards := remove(last.Shards[:], dedup)
	config := Config{
		Num:    version,
		Groups: groups,
		Shards: SliceToArr(shards),
	}
	return &config
}

func (sc *ShardCtrler) makeConfigForMove(args *MoveArgs) *Config {
	// sc.mu.Lock()
	// defer sc.mu.Unlock()
	version := len(sc.configs)
	groups := make(map[int][]string)
	for k, v := range sc.configs[version-1].Groups {
		groups[k] = v
	}
	shards := sc.configs[version-1].Shards[:]
	oldGid := shards[args.Shard]
	cc := 0
	for _, v := range shards {
		if v == oldGid {
			cc += 1
		}
	}
	if cc == 1 {
		delete(groups, oldGid)
	}

	shards[args.Shard] = args.GID
	config := Config{
		Num:    version,
		Groups: groups,
		Shards: SliceToArr(shards),
	}
	return &config
}

func (sc *ShardCtrler) pollGet(term int, index int, clientId int64, seq int64, reply *QueryReply) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ct, cl := sc.rf.GetState()
	if !cl || ct != term {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return true
	}
	if sc.lastApplied < index {
		return false
	}
	if logSeq, ok := sc.clientSeq[clientId]; !ok || logSeq != seq {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return true
	}
	val := sc.cache[clientId]

	if val == nil {
		reply.Err = ErrNoVersion
		return true
	}
	reply.Err = OK
	// reply.Value = val.(string)
	reply.Config = val.(Config)
	return true
}

func (sc *ShardCtrler) pollUpdate(term int, index int, clientId int64, seq int64) int {
	DPrintf("server %d pollUpdate", sc.me)
	ct, cl := sc.rf.GetState()
	if !cl || ct != term {
		// reply.Err = ErrWrongLeader
		// return true
		return -1
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	DPrintf("server %d pollUpdate 1111111111", sc.me)
	if sc.lastApplied < index {
		// return false
		return 1
	}
	DPrintf("server %d pollUpdate %d vs %d", sc.me, sc.lastApplied, index)
	if logSeq, ok := sc.clientSeq[clientId]; !ok || logSeq != seq {
		// reply.Err = ErrWrongLeader
		// return true
		return -1
	}
	DPrintf("server %d pollUpdate 222222222222", sc.me)
	// reply.Err = OK
	// return true
	return 0
}
