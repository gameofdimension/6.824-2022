package shardkv

import (
	"fmt"
	"time"

	"6.824/shardctrler"
)

const (
	NotAssigned = 0
	NoData      = 1
	Frozen      = 2
	Ready       = 3
)

type ShardStatus uint

func (cm *ShardKV) GetShardStatus(shard int) ShardStatus {
	return cm.status[shard]
}

func (cm *ShardKV) raftSyncOp(op *Op) bool {
	index, term, isLeader := cm.rf.Start(*op)
	if !isLeader {
		return false
	}
	for !cm.rf.Killed() {
		stop, success := cm.pollAgreement(term, index, op.ClientId, op.Seq)
		if stop {
			return success
		}
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

// 根据课程说明要求失去 shard 的 group 发送数据，而不是得到 shard 的拉取数据
func (cm *ShardKV) updateConfig(config *shardctrler.Config) {
	cm.mu.Lock()
	newConfig := *config
	newVersion := newConfig.Num
	prefix := fmt.Sprintf("update config %d of group %d version [%d vs %d]", cm.me, cm.gid, cm.currentVersion, newVersion)
	var status [shardctrler.NShards]ShardStatus
	if cm.currentVersion == 0 {
		for i := 0; i < shardctrler.NShards; i += 1 {
			if newConfig.Shards[i] == cm.gid {
				status[i] = Ready
			} else {
				status[i] = NotAssigned
			}
		}

		clientId := cm.id
		cm.migrateSeq += 1
		seq := cm.migrateSeq
		cm.mu.Unlock()

		change := ConfigChange{
			CurrentVersion: newVersion,
			CurrentConfig:  newConfig,
			NextVersion:    0,
			Status:         status,
		}
		op := Op{
			Type:     OpConfig,
			ClientId: clientId,
			Seq:      seq,
			Change:   change,
		}
		ok := cm.raftSyncOp(&op)
		DPrintf("%s sync init change to raft ok? %t", prefix, ok)
		return
	}
	DPrintf("%s start %v vs %v", prefix, cm.status, newConfig.Shards)
	for i := 0; i < shardctrler.NShards; i += 1 {
		if cm.status[i] == NotAssigned {
			if newConfig.Shards[i] != cm.gid {
				status[i] = NotAssigned
			} else {
				status[i] = NoData
			}
		} else if cm.status[i] == Ready {
			if newConfig.Shards[i] != cm.gid {
				status[i] = Frozen
			} else {
				status[i] = Ready
			}
		} else {
			panic(fmt.Sprintf("status %d wrong for in use config, group %d, shard %d", cm.status[i], cm.gid, i))
		}
	}
	cm.mu.Unlock()

	change := ConfigChange{
		CurrentVersion: cm.currentVersion,
		CurrentConfig:  cm.currentConfig,
		NextVersion:    newVersion,
		NextConfig:     newConfig,
		Status:         status,
	}

	cm.mu.Lock()
	clientId := cm.id
	cm.migrateSeq += 1
	seq := cm.migrateSeq
	cm.mu.Unlock()

	op := Op{
		Type:     OpConfig,
		ClientId: clientId,
		Seq:      seq,
		Change:   change,
	}
	ok := cm.raftSyncOp(&op)
	DPrintf("%s sync change switching [%d->%d] to raft ok? %t", prefix, cm.currentVersion, newVersion, ok)
}

func (cm *ShardKV) migrateData(servers []string, shard int, oldVersion int, newVersion int, gid int) bool {
	data, lastSeq, lastResult := cm.dump(shard)
	DPrintf("migrate from %d of group %d, version [%d vs %d], shard: %d, %v",
		cm.me, cm.gid, oldVersion, newVersion, shard, data)
	cm.mu.Lock()
	clientId := cm.id
	cm.migrateSeq += 1
	seq := cm.migrateSeq
	cm.mu.Unlock()
	for !cm.rf.Killed() {
		for i := 0; i < len(servers); i += 1 {
			srv := cm.make_end(servers[i])
			prefix := fmt.Sprintf("migrate from %d of group %d, version [%d vs %d], shard: %d, %d",
				cm.me, cm.gid, oldVersion, newVersion, shard, len(data))
			args := DumpArgs{
				Id:               clientId,
				Seq:              seq,
				OldVersion:       oldVersion,
				NewVersion:       newVersion,
				Shard:            shard,
				ShardData:        data,
				LastClientSeq:    lastSeq,
				LastClientResult: lastResult,
			}
			reply := DumpReply{}
			DPrintf("%s start call server %s", prefix, servers[i])
			ok := srv.Call("ShardKV.Migrate", &args, &reply)
			if ok && reply.Err == OK {
				DPrintf("%s ok", prefix)
				return true
			}
			DPrintf("%s error %t, %v", prefix, ok, reply)
			time.Sleep(50 * time.Millisecond)
		}
	}
	return false
}

func (cm *ShardKV) isSwitching() bool {
	// 需确保调用方持有锁
	DPrintf("fetch server %d of group %d isSwitching? %t %d vs %d",
		cm.me, cm.gid, cm.nextVersion != 0, cm.currentVersion, cm.nextVersion)
	if cm.nextVersion != 0 {
		if cm.nextVersion <= cm.currentVersion {
			panic(fmt.Sprintf(" %d vs %d", cm.currentVersion, cm.nextVersion))
		}
		return true
	}
	return false
}

func (cm *ShardKV) sendNoop() bool {
	cm.mu.Lock()
	clientId := cm.id
	cm.migrateSeq += 1
	seq := cm.migrateSeq
	cm.mu.Unlock()
	op := Op{
		Type:     OpNoop,
		ClientId: clientId,
		Seq:      seq,
	}
	return cm.raftSyncOp(&op)
}

func (cm *ShardKV) configFetcher() {
	// wait until all log applied
	for !cm.rf.Killed() {
		if cm.sendNoop() {
			break
		}
		time.Sleep(37 * time.Millisecond)
	}
	for !cm.rf.Killed() {
		_, isLeader := cm.rf.GetState()
		if isLeader {
			prefix := fmt.Sprintf("fetch server %d of group %d", cm.me, cm.gid)
			cm.mu.Lock()
			switching := cm.isSwitching()
			nextVersion := cm.currentVersion + 1
			cm.mu.Unlock()
			if !switching {
				config := cm.mck.Query(nextVersion)
				if config.Num == 0 {
					DPrintf("%s no config for version %d", prefix, nextVersion)
					time.Sleep(83 * time.Millisecond)
					continue
				}
				DPrintf("%s get config %v of version %d", prefix, config.Shards, config.Num)
				if config.Num != nextVersion {
					panic(fmt.Sprintf("%s expect version %d got %d", prefix, nextVersion, config.Num))
				}
				cm.updateConfig(&config)
			} else {
				cm.mu.Lock()
				shardAddress := make(map[int][]string, 0)
				var status [shardctrler.NShards]ShardStatus
				for i := 0; i < shardctrler.NShards; i += 1 {
					status[i] = cm.status[i]
					if cm.status[i] == Frozen {
						gid := cm.nextConfig.Shards[i]
						shardAddress[i] = cm.nextConfig.Groups[gid]
					}
				}
				cm.mu.Unlock()
				if len(shardAddress) > 0 {
					DPrintf("%s need to move data for %v", prefix, shardAddress)
					for sd, servers := range shardAddress {
						cm.migrateData(servers, sd, cm.currentVersion, cm.nextVersion, cm.gid)
						status[sd] = NotAssigned
					}

					change := ConfigChange{
						CurrentVersion: cm.currentVersion,
						CurrentConfig:  cm.currentConfig,
						NextVersion:    cm.nextVersion,
						NextConfig:     cm.nextConfig,
						Status:         status,
					}

					cm.mu.Lock()
					clientId := cm.id
					cm.migrateSeq += 1
					seq := cm.migrateSeq
					cm.mu.Unlock()

					op := Op{
						Type:     OpConfig,
						ClientId: clientId,
						Seq:      seq,
						Change:   change,
					}
					ok := cm.raftSyncOp(&op)
					cm.mu.Lock()
					DPrintf("%s sync change migration [%d->%d] to raft ok? %t", prefix, cm.currentVersion, cm.nextVersion, ok)
					cm.mu.Unlock()
				}

				cm.mu.Lock()
				switchDone := true
				DPrintf("%s config switching shard status %v", prefix, cm.status)
				for _, st := range cm.status {
					if !(st == NotAssigned || st == Ready) {
						switchDone = false
						break
					}
				}
				cm.mu.Unlock()
				if switchDone {
					cm.mu.Lock()
					clientId := cm.id
					cm.migrateSeq += 1
					seq := cm.migrateSeq
					cm.mu.Unlock()
					change := ConfigChange{
						CurrentVersion: cm.nextVersion,
						CurrentConfig:  cm.nextConfig,
						NextVersion:    0,
						Status:         cm.status,
					}
					op := Op{
						Type:     OpConfig,
						ClientId: clientId,
						Seq:      seq,
						Change:   change,
					}
					ok := cm.raftSyncOp(&op)
					DPrintf("%s sync change switched [%d->%d] to raft ok? %t", prefix, cm.currentVersion, cm.nextVersion, ok)
				}
			}
		}
		time.Sleep(73 * time.Millisecond)
	}
}
