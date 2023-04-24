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
	status := [shardctrler.NShards]ShardStatus{}
	prefix := fmt.Sprintf("update config %d of group %d version [%d vs %d]", cm.me, cm.gid, cm.currentVersion, newVersion)
	if cm.currentVersion == 0 {
		for i := 0; i < shardctrler.NShards; i += 1 {
			if newConfig.Shards[i] == cm.gid {
				status[i] = Ready
			} else {
				status[i] = NotAssigned
			}
		}

		cm.nextVersion = 0
		DPrintf("%s status %v after init", prefix, status)
		cm.status = status
		cm.currentVersion = newVersion
		cm.currentConfig = newConfig
		cm.mu.Unlock()
		return
	}
	DPrintf("%s start", prefix)
	shardAddress := make(map[int][]string, 0)
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
				gid := newConfig.Shards[i]
				shardAddress[i] = newConfig.Groups[gid]
			} else {
				status[i] = Ready
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
		status[sd] = Ready
	}

	cm.mu.Lock()
	cm.nextVersion = newVersion
	cm.status = status
	cm.nextConfig = newConfig
	cm.mu.Unlock()
}

func (cm *ShardKV) migrateData(servers []string, shard int, version int, newVersion int, gid int) bool {
	data := cm.dump(shard)
	for {
		for i := 0; i < len(servers); i += 1 {
			srv := cm.make_end(servers[i])
			prefix := fmt.Sprintf("migrate from %d of group %d, version [%d vs %d], data: %d, %d",
				cm.me, cm.gid, version, newVersion, shard, len(data))
			args := DumpArgs{
				OldVersion: version,
				NewVersion: newVersion,
				Shard:      shard,
				CallerGid:  gid,
				ShardData:  data,
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

func (cm *ShardKV) configFetcher() {
	for {
		_, isLeader := cm.rf.GetState()
		if isLeader {
			prefix := fmt.Sprintf("fetch server %d of group %d", cm.me, cm.gid)
			if !cm.isSwitching() {
				config := cm.mck.Query(-1)
				DPrintf("%s get config %v of version %d", prefix, config.Shards, config.Num)
				if config.Num > cm.currentVersion {
					cm.updateConfig(&config)
				} else if config.Num == cm.currentVersion {
					DPrintf("%s version not change %d vs %d", prefix, config.Num, cm.currentVersion)
				} else {
					panic(fmt.Sprintf("%s smaller config version", prefix))
				}
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
					if cm.syncConfig(&cm.nextConfig) {
						cm.mu.Lock()
						cm.currentConfig = cm.nextConfig
						cm.currentVersion = cm.nextVersion
						cm.nextVersion = 0
						cm.mu.Unlock()
					}
					// todo 同步数据到 follower
				}
			}
		}
		time.Sleep(73 * time.Millisecond)
	}
}

// 将关于配置的信息同步到本 group 的所有 follower
func (cm *ShardKV) syncConfig(nextConfig *shardctrler.Config) bool {
	config := *nextConfig
	op := Op{
		Type:     OpConfig,
		ClientId: cm.id,
		Seq:      int64(nextConfig.Num),
		Config:   config,
	}
	cm.rf.Start(op)
	return true
}
