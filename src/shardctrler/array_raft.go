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
	version := len(sc.configs)
	groups := make(map[int][]string)
	last := sc.configs[version-1]
	for k, v := range last.Groups {
		groups[k] = v
	}
	for k, v := range args.Servers {
		groups[k] = v
	}

	finalGids := []int{}
	for k := range groups {
		finalGids = append(finalGids, k)
	}

	shards := add(last.Shards[:], finalGids)
	config := Config{
		Num:    version,
		Groups: groups,
		Shards: SliceToArr(shards),
	}
	return &config
}

func (sc *ShardCtrler) makeConfigForLeave(args *LeaveArgs) *Config {
	version := len(sc.configs)
	groups := make(map[int][]string)
	last := sc.configs[version-1]
	finalGids := make([]int, 0)
	for k, v := range last.Groups {
		if !SliceContains(args.GIDs, k) {
			groups[k] = v
			finalGids = append(finalGids, k)
		}
	}
	shards := remove(last.Shards[:], finalGids)
	config := Config{
		Num:    version,
		Groups: groups,
		Shards: SliceToArr(shards),
	}
	return &config
}

func (sc *ShardCtrler) makeConfigForMove(args *MoveArgs) *Config {
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

func (sc *ShardCtrler) pollAgreement(term int, index int, clientId int64, seq int64) (bool, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ct, cl := sc.rf.GetState()
	if !cl || ct != term {
		return true, false
	}
	if sc.lastApplied < index {
		return false, false
	}
	if logSeq, ok := sc.clientSeq[clientId]; !ok || logSeq != seq {
		return true, false
	}
	return true, true
}
