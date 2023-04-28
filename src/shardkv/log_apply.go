package shardkv

import (
	"bytes"
	"fmt"
	"time"

	"6.824/labgob"
	"6.824/shardctrler"
)

func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		if m.SnapshotValid {
			kv.mu.Lock()
			DPrintf("server %d of group %d resume snapshot of term %d at index %d", kv.me, kv.gid, m.SnapshotTerm, m.SnapshotIndex)
			kv.loadSnapshot(m.Snapshot)
			kv.mu.Unlock()
		} else if m.CommandValid {
			kv.mu.Lock()
			if m.CommandIndex <= kv.lastApplied {
				panic(fmt.Sprintf("index error %d vs %d", m.CommandIndex, kv.lastApplied))
			}
			kv.lastApplied = m.CommandIndex
			op := m.Command.(Op)
			clientId, seq := op.ClientId, op.Seq
			lastSeq, ok := kv.clientSeq[clientId]
			// new op from clientId
			if !ok || seq != lastSeq {
				DPrintf("server %d of group %d will run op %v at index %d", kv.me, kv.gid, op.Type, m.CommandIndex)
				if seq < lastSeq {
					panic(fmt.Sprintf("smaller seq %d vs %d for %d, %v", seq, lastSeq, clientId, op))
				}
				if op.Type == OpGet {
					if val, ok := kv.repo[op.Key]; ok {
						kv.cache[clientId] = val
					} else {
						kv.cache[clientId] = nil
					}
				} else if op.Type == OpPut {
					kv.repo[op.Key] = op.Value
					kv.cache[clientId] = true
				} else if op.Type == OpAppend {
					kv.repo[op.Key] = kv.repo[op.Key] + op.Value
					kv.cache[clientId] = true
				} else if op.Type == OpConfig {
					DPrintf("server %d,%d of group %d apply config change [%d,%d]->[%d,%d], status %v",
						kv.me, kv.id, kv.gid, kv.currentVersion, kv.nextVersion, op.Change.CurrentVersion,
						op.Change.NextVersion, op.Change.Status)
					kv.currentVersion = op.Change.CurrentVersion
					kv.currentConfig = op.Change.CurrentConfig
					kv.nextVersion = op.Change.NextVersion
					kv.nextConfig = op.Change.NextConfig
					kv.status = op.Change.Status
					kv.deleteShardData()
				} else if op.Type == OpMigrate {
					kv.merge(op.ShardData.Data)
					kv.status[op.ShardData.Shard] = Ready
					for k, v := range op.ShardData.LastClientSeq {
						if tmp, ok := kv.clientSeq[k]; !ok || v > tmp {
							kv.clientSeq[k] = v
							kv.cache[k] = op.ShardData.LastClientResult[k]
						}
					}
				}
				kv.clientSeq[clientId] = seq
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
				data := kv.makeSnapshot()
				DPrintf("server %d of group %d take snapshot at index %d", kv.me, kv.gid, m.CommandIndex)
				kv.rf.Snapshot(m.CommandIndex, data)
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) deleteShardData() {
	// 要扫描所有数据，效率有点低
	for k, _ := range kv.repo {
		if kv.status[key2shard(k)] == NotAssigned {
			delete(kv.repo, k)
		}
	}
}

func (kv *ShardKV) dump(shard int) (map[string]string, map[int64]int64, map[int64]interface{}) {
	// ensure all pending logs applied
	for !kv.rf.Killed() {
		if kv.sendNoop() {
			break
		}
		time.Sleep(31 * time.Millisecond)
	}
	kv.mu.Lock()
	tmp := make(map[string]string, 0)
	for k, v := range kv.repo {
		if key2shard(k) == shard {
			tmp[k] = v
		}
	}
	copySeq := make(map[int64]int64)
	for k, v := range kv.clientSeq {
		copySeq[k] = v
	}
	copyResult := make(map[int64]interface{})
	for k, v := range kv.cache {
		copyResult[k] = v
	}
	kv.mu.Unlock()
	return tmp, copySeq, copyResult
}

func (kv *ShardKV) merge(data map[string]string) {
	for k, v := range data {
		if len(v) < len(kv.repo[k]) {
			DPrintf("merge data server %d,%d of group %d, key %s %v->%v", kv.me, kv.id, kv.gid, k, kv.repo[k], v)
		}
		kv.repo[k] = v
	}
}

func (kv *ShardKV) pollAgreement(term int, index int, clientId int64, seq int64) (bool, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ct, cl := kv.rf.GetState()
	if !cl || ct != term {
		return true, false
	}
	if kv.lastApplied < index {
		return false, false
	}
	if logSeq, ok := kv.clientSeq[clientId]; !ok || logSeq != seq {
		return true, false
	}
	return true, true
}

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.repo)
	e.Encode(kv.clientSeq)
	e.Encode(kv.cache)
	e.Encode(kv.currentVersion)
	e.Encode(kv.currentConfig)
	e.Encode(kv.nextVersion)
	e.Encode(kv.nextConfig)
	e.Encode(kv.status)
	// e.Encode(kv.migrateSeq)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) loadSnapshot(data []byte) {
	// DPrintf("loadSnapshot %d", len(data))
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var repo map[string]string
	var clientSeq map[int64]int64
	var cache map[int64]interface{}
	var currentVersion int
	var currentConfig shardctrler.Config
	var nextVersion int
	var nextConfig shardctrler.Config
	var status [shardctrler.NShards]ShardStatus
	// var migrateSeq int64
	if d.Decode(&repo) != nil ||
		d.Decode(&clientSeq) != nil ||
		d.Decode(&cache) != nil ||
		d.Decode(&currentVersion) != nil ||
		d.Decode(&currentConfig) != nil ||
		d.Decode(&nextVersion) != nil ||
		d.Decode(&nextConfig) != nil ||
		d.Decode(&status) != nil {
		panic("readPersist fail, Decode")
	} else {
		DPrintf("loadSnapshot server %d, %d of group %d, [%d,%d]->[%d,%d], status: %v",
			kv.me, kv.id, kv.gid, kv.currentVersion, kv.nextVersion, currentVersion, nextVersion, status)
		kv.repo = repo
		kv.clientSeq = clientSeq
		kv.cache = cache
		kv.currentVersion = currentVersion
		kv.currentConfig = currentConfig
		kv.nextVersion = nextVersion
		kv.nextConfig = nextConfig
		kv.status = status
	}
}
