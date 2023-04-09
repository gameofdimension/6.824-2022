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
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.status[shard]
}

// 根据课程说明要求失去 shard 的 group 发送数据，而不是得到 shard 的拉取数据
func (cm *ShardKV) updateConfig(config *shardctrler.Config) {
	cm.mu.Lock()
	newConfig := *config
	newVersion := newConfig.Num
	status := [shardctrler.NShards]ShardStatus{}
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

	for sd, servers := range shardAddress {
		cm.migrateData(servers, sd, version, gid)
		status[sd] = Ready
	}

	cm.mu.Lock()
	cm.nextVersion = newVersion
	cm.status = status
	cm.nextConfig = newConfig
	cm.mu.Unlock()
}

func (cm *ShardKV) migrateData(servers []string, shard int, version int, gid int) bool {
	data := cm.dump(shard)
	for {
		for i := 0; i < len(servers); i += 1 {
			srv := cm.make_end(servers[i])
			args := DumpArgs{
				Version:   version,
				Shard:     shard,
				CallerGid: gid,
				ShardData: data,
			}
			reply := DumpReply{}
			ok := srv.Call("ShardKV.Migrate", &args, &reply)
			if ok && reply.Err == OK {
				return true
			}
			DPrintf("server %d of group %d move data error %t, %v", cm.me, cm.gid, ok, reply)
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (cm *ShardKV) isSwitching() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.nextVersion != 0 {
		if cm.nextVersion <= cm.currentVersion {
			panic(fmt.Sprintf("version error %d vs %d", cm.currentVersion, cm.nextVersion))
		}
		return true
	}
	return false
}

func (cm *ShardKV) configFetcher() {
	for {
		_, isLeader := cm.rf.GetState()
		if isLeader {
			if !cm.isSwitching() {
				config := cm.mck.Query(-1)
				DPrintf("server %d of group %d get config %v of version %d", cm.me, cm.gid, config, config.Num)
				if config.Num > cm.currentVersion {
					cm.updateConfig(&config)
				} else {
					DPrintf("version error %d vs %d", config.Num, cm.currentVersion)
				}
			} else {
				cm.mu.Lock()
				ok := true
				DPrintf("server %d of group %v shard status %v", cm.me, cm.gid, cm.status)
				for _, st := range cm.status {
					if !(st == NotAssigned || st == Ready) {
						ok = false
						break
					}
				}
				cm.mu.Unlock()
				if ok {
					if cm.syncConfig(cm.nextConfig) {
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
