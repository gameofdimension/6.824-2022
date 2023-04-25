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

// 根据课程说明要求失去 shard 的 group 发送数据，而不是得到 shard 的拉取数据
func (cm *ShardKV) updateConfig(config *shardctrler.Config) {
	cm.mu.Lock()
	newConfig := *config
	newVersion := newConfig.Num
	prefix := fmt.Sprintf("update config %d of group %d version [%d vs %d]", cm.me, cm.gid, cm.currentVersion, newVersion)
	if cm.currentVersion == 0 {
		for i := 0; i < shardctrler.NShards; i += 1 {
			if newConfig.Shards[i] == cm.gid {
				cm.status[i] = Ready
			} else {
				cm.status[i] = NotAssigned
			}
		}

		cm.nextVersion = 0
		DPrintf("%s status %v after init", prefix, cm.status)
		cm.currentVersion = newVersion
		cm.currentConfig = newConfig
		cm.mu.Unlock()
		return
	}
	DPrintf("%s start %v vs %v", prefix, cm.status, newConfig.Shards)
	shardAddress := make(map[int][]string, 0)
	for i := 0; i < shardctrler.NShards; i += 1 {
		if cm.status[i] == NotAssigned {
			if newConfig.Shards[i] != cm.gid {
				cm.status[i] = NotAssigned
			} else {
				cm.status[i] = NoData
			}
		} else if cm.status[i] == Ready {
			if newConfig.Shards[i] != cm.gid {
				cm.status[i] = Frozen
				gid := newConfig.Shards[i]
				shardAddress[i] = newConfig.Groups[gid]
			} else {
				cm.status[i] = Ready
			}
		} else {
			panic(fmt.Sprintf("status %d wrong for in use config, group %d, shard %d", cm.status[i], cm.gid, i))
		}
	}
	version := cm.currentVersion
	gid := cm.gid
	cm.mu.Unlock()

	DPrintf("%s need to move data for %v", prefix, shardAddress)
	for sd, servers := range shardAddress {
		cm.migrateData(servers, sd, version, newVersion, gid)
	}

	cm.mu.Lock()
	cm.nextVersion = newVersion
	cm.nextConfig = newConfig
	DPrintf("%s done next version %d: %v", prefix, cm.nextVersion, cm.status)
	cm.mu.Unlock()
}

func (cm *ShardKV) migrateData(servers []string, shard int, version int, newVersion int, gid int) bool {
	data := cm.dump(shard)
	cm.mu.Lock()
	clientId := cm.id
	cm.migrateSeq += 1
	seq := cm.migrateSeq
	cm.mu.Unlock()
	for {
		for i := 0; i < len(servers); i += 1 {
			srv := cm.make_end(servers[i])
			prefix := fmt.Sprintf("migrate from %d of group %d, version [%d vs %d], shard: %d, %d",
				cm.me, cm.gid, version, newVersion, shard, len(data))
			args := DumpArgs{
				OldVersion: version,
				NewVersion: newVersion,
				Shard:      shard,
				ShardData:  data,
				Id:         clientId,
				Seq:        seq,
			}
			reply := DumpReply{}
			DPrintf("%s start call server %s", prefix, servers[i])
			ok := srv.Call("ShardKV.Migrate", &args, &reply)
			if ok && reply.Err == OK {
				cm.mu.Lock()
				cm.status[shard] = NotAssigned
				cm.mu.Unlock()
				DPrintf("%s ok", prefix)
				return true
			}
			DPrintf("%s error %t, %v", prefix, ok, reply)
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (cm *ShardKV) isSwitching() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
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
	prefix := fmt.Sprintf("send noop server %d of group %d to raft", cm.me, cm.gid)
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
	index, term, isLeader := cm.rf.Start(op)
	DPrintf("%s start", prefix)
	if !isLeader {
		DPrintf("%s not leader %d, %d, %t", prefix, index, term, isLeader)
		return false
	}
	for {
		stop, success := cm.pollAgreement(term, index, clientId, seq)
		if stop {
			return success
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (cm *ShardKV) configFetcher() {
	for {
		if cm.sendNoop() {
			break
		}
		time.Sleep(37 * time.Millisecond)
	}
	for {
		_, isLeader := cm.rf.GetState()
		if isLeader {
			prefix := fmt.Sprintf("fetch server %d of group %d", cm.me, cm.gid)
			if !cm.isSwitching() {
				config := cm.mck.Query(cm.currentVersion + 1)
				if config.Num == 0 {
					DPrintf("%s no config for version %d", prefix, cm.currentVersion+1)
					time.Sleep(103 * time.Millisecond)
					continue
				}
				DPrintf("%s get config %v of version %d", prefix, config.Shards, config.Num)
				if config.Num != cm.currentVersion+1 {
					panic(fmt.Sprintf("%s expect version %d got %d", prefix, cm.currentVersion+1, config.Num))
				}
				cm.updateConfig(&config)
			} else {
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
					for {
						if cm.syncConfig(&cm.nextConfig) {
							break
						}
						DPrintf("%s sync config fail will retry", prefix)
					}
				}
			}
		}
		time.Sleep(73 * time.Millisecond)
	}
}

// 将关于配置的信息同步到本 group 的所有 follower
func (cm *ShardKV) syncConfig(nextConfig *shardctrler.Config) bool {
	config := *nextConfig
	cm.mu.Lock()
	clientId := cm.id
	cm.migrateSeq += 1
	seq := cm.migrateSeq
	cm.mu.Unlock()
	op := Op{
		Type:     OpConfig,
		ClientId: clientId,
		Seq:      seq,
		Config:   config,
	}
	index, term, isLeader := cm.rf.Start(op)
	DPrintf("send config change server %d of group %d to raft %d, %d, %t", cm.me, cm.gid, index, term, isLeader)
	if !isLeader {
		return false
	}
	for {
		stop, success := cm.pollAgreement(term, index, clientId, seq)
		if stop {
			return success
		}
		time.Sleep(1 * time.Millisecond)
	}
}
