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

func (kv *ShardKV) GetShardStatus(shard int) ShardStatus {
	return kv.status[shard]
}

func (kv *ShardKV) raftSyncOp(op *Op) bool {
	index, term, isLeader := kv.rf.Start(*op)
	if !isLeader {
		return false
	}
	for !kv.rf.Killed() {
		stop, success := kv.pollAgreement(term, index, op.ClientId, op.Seq)
		if stop {
			return success
		}
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

// 根据课程说明要求失去 shard 的 group 发送数据，而不是得到 shard 的拉取数据
func (kv *ShardKV) updateConfig(config *shardctrler.Config) {
	kv.mu.Lock()
	newConfig := *config
	newVersion := newConfig.Num
	prefix := fmt.Sprintf("update config %d of group %d version [%d vs %d]", kv.me, kv.gid, kv.currentVersion, newVersion)
	var status [shardctrler.NShards]ShardStatus
	if kv.currentVersion == 0 {
		for i := 0; i < shardctrler.NShards; i += 1 {
			if newConfig.Shards[i] == kv.gid {
				status[i] = Ready
			} else {
				status[i] = NotAssigned
			}
		}

		clientId := kv.id
		kv.migrateSeq += 1
		seq := kv.migrateSeq
		kv.mu.Unlock()

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
		ok := kv.raftSyncOp(&op)
		DPrintf("%s sync init change to raft ok? %t", prefix, ok)
		return
	}
	DPrintf("%s start %v vs %v", prefix, kv.status, newConfig.Shards)
	for i := 0; i < shardctrler.NShards; i += 1 {
		if kv.status[i] == NotAssigned {
			if newConfig.Shards[i] != kv.gid {
				status[i] = NotAssigned
			} else {
				status[i] = NoData
			}
		} else if kv.status[i] == Ready {
			if newConfig.Shards[i] != kv.gid {
				status[i] = Frozen
			} else {
				status[i] = Ready
			}
		} else {
			panic(fmt.Sprintf("status %d wrong for in use config, group %d, shard %d", kv.status[i], kv.gid, i))
		}
	}
	kv.mu.Unlock()

	change := ConfigChange{
		CurrentVersion: kv.currentVersion,
		CurrentConfig:  kv.currentConfig,
		NextVersion:    newVersion,
		NextConfig:     newConfig,
		Status:         status,
	}

	kv.mu.Lock()
	clientId := kv.id
	kv.migrateSeq += 1
	seq := kv.migrateSeq
	kv.mu.Unlock()

	op := Op{
		Type:     OpConfig,
		ClientId: clientId,
		Seq:      seq,
		Change:   change,
	}
	ok := kv.raftSyncOp(&op)
	DPrintf("%s sync change switching [%d->%d] to raft ok? %t", prefix, kv.currentVersion, newVersion, ok)
}

func (kv *ShardKV) migrateShard(servers []string, shard int, oldVersion int, newVersion int, gid int) bool {
	data, lastSeq, lastResult := kv.dump(shard)
	DPrintf("migrate from %d of group %d, version [%d vs %d], shard: %d, %v",
		kv.me, kv.gid, oldVersion, newVersion, shard, data)
	kv.mu.Lock()
	clientId := kv.id
	kv.migrateSeq += 1
	seq := kv.migrateSeq
	kv.mu.Unlock()
	for !kv.rf.Killed() {
		for i := 0; i < len(servers); i += 1 {
			srv := kv.make_end(servers[i])
			prefix := fmt.Sprintf("migrate from %d of group %d, version [%d vs %d], shard: %d, %d",
				kv.me, kv.gid, oldVersion, newVersion, shard, len(data))
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

func (kv *ShardKV) isSwitching() bool {
	// 需确保调用方持有锁
	DPrintf("fetch server %d of group %d isSwitching? %t %d vs %d",
		kv.me, kv.gid, kv.nextVersion != 0, kv.currentVersion, kv.nextVersion)
	if kv.nextVersion != 0 {
		if kv.nextVersion <= kv.currentVersion {
			panic(fmt.Sprintf(" %d vs %d", kv.currentVersion, kv.nextVersion))
		}
		return true
	}
	return false
}

func (kv *ShardKV) sendNoop() bool {
	kv.mu.Lock()
	clientId := kv.id
	kv.migrateSeq += 1
	seq := kv.migrateSeq
	kv.mu.Unlock()
	op := Op{
		Type:     OpNoop,
		ClientId: clientId,
		Seq:      seq,
	}
	return kv.raftSyncOp(&op)
}

func (kv *ShardKV) configFetcher() {
	// wait until all log applied
	for !kv.rf.Killed() {
		if kv.sendNoop() {
			break
		}
		time.Sleep(37 * time.Millisecond)
	}
	for !kv.rf.Killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			prefix := fmt.Sprintf("fetch server %d of group %d", kv.me, kv.gid)
			kv.mu.Lock()
			switching := kv.isSwitching()
			nextVersion := kv.currentVersion + 1
			kv.mu.Unlock()
			if !switching {
				config := kv.mck.Query(nextVersion)
				if config.Num == 0 {
					DPrintf("%s no config for version %d", prefix, nextVersion)
					time.Sleep(83 * time.Millisecond)
					continue
				}
				DPrintf("%s get config %v of version %d", prefix, config.Shards, config.Num)
				if config.Num != nextVersion {
					panic(fmt.Sprintf("%s expect version %d got %d", prefix, nextVersion, config.Num))
				}
				kv.updateConfig(&config)
			} else {
				kv.tryMigrateShards()

				kv.mu.Lock()
				switchDone := true
				DPrintf("%s config switching shard status %v", prefix, kv.status)
				for _, st := range kv.status {
					if !(st == NotAssigned || st == Ready) {
						switchDone = false
						break
					}
				}
				kv.mu.Unlock()
				if switchDone {
					kv.mu.Lock()
					clientId := kv.id
					kv.migrateSeq += 1
					seq := kv.migrateSeq
					kv.mu.Unlock()
					change := ConfigChange{
						CurrentVersion: kv.nextVersion,
						CurrentConfig:  kv.nextConfig,
						NextVersion:    0,
						Status:         kv.status,
					}
					op := Op{
						Type:     OpConfig,
						ClientId: clientId,
						Seq:      seq,
						Change:   change,
					}
					ok := kv.raftSyncOp(&op)
					kv.mu.Lock()
					DPrintf("%s sync change switched [%d->%d] to raft ok? %t", prefix, kv.currentVersion, kv.nextVersion, ok)
					kv.mu.Unlock()
				}
			}
		}
		time.Sleep(73 * time.Millisecond)
	}
}

func (kv *ShardKV) tryMigrateShards() {
	kv.mu.Lock()
	prefix := fmt.Sprintf("fetch server %d of group %d", kv.me, kv.gid)
	shardAddress := make(map[int][]string, 0)
	var status [shardctrler.NShards]ShardStatus
	for i := 0; i < shardctrler.NShards; i += 1 {
		status[i] = kv.status[i]
		if kv.status[i] == Frozen {
			gid := kv.nextConfig.Shards[i]
			shardAddress[i] = kv.nextConfig.Groups[gid]
		}
	}
	kv.mu.Unlock()
	if len(shardAddress) > 0 {
		DPrintf("%s need to move data for %v", prefix, shardAddress)
		for sd, servers := range shardAddress {
			kv.migrateShard(servers, sd, kv.currentVersion, kv.nextVersion, kv.gid)
			status[sd] = NotAssigned
		}

		change := ConfigChange{
			CurrentVersion: kv.currentVersion,
			CurrentConfig:  kv.currentConfig,
			NextVersion:    kv.nextVersion,
			NextConfig:     kv.nextConfig,
			Status:         status,
		}

		kv.mu.Lock()
		clientId := kv.id
		kv.migrateSeq += 1
		seq := kv.migrateSeq
		kv.mu.Unlock()

		op := Op{
			Type:     OpConfig,
			ClientId: clientId,
			Seq:      seq,
			Change:   change,
		}
		ok := kv.raftSyncOp(&op)
		kv.mu.Lock()
		DPrintf("%s sync change migration [%d->%d] to raft ok? %t", prefix, kv.currentVersion, kv.nextVersion, ok)
		kv.mu.Unlock()
	}
}
