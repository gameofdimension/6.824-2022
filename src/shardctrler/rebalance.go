package shardctrler

import (
	"fmt"
	"sort"
)

type Pair struct {
	Key   int
	Value []int
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return len(p[i].Value) < len(p[j].Value) }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func add(shards []int, gids []int) []int {
	if shards[0] == 0 {
		for _, v := range shards {
			if v != 0 {
				panic("all shard should belong to 0 group")
			}
		}
		avg := len(shards) / len(gids)
		rmd := len(shards) % len(gids)
		newShards := make([]int, 0)
		for i := 0; i < len(gids); i += 1 {
			val := avg
			if i < rmd {
				val += 1
			}
			for j := 0; j < val; j += 1 {
				newShards = append(newShards, gids[i])
			}
		}
		return newShards
	}
	oldCount := sortByShards(shards)
	if len(oldCount) >= NShards {
		return shards
	}
	sn := len(shards)
	gn := len(gids) + len(oldCount)
	newCount := make(PairList, gn)
	for _, v := range newCount {
		v.Value = make([]int, 0)
	}
	avg := sn / gn
	rmd := sn % gn
	pool := make([]int, 0)
	for i := 0; i < gn; i += 1 {
		val := avg
		if i < rmd {
			val += 1
		}
		nold := len(oldCount)
		if i < nold {
			delta := len(oldCount[i].Value) - val
			if delta > 0 {
				newCount[i] = Pair{oldCount[i].Key, oldCount[i].Value[:val]}
				pool = append(pool, oldCount[i].Value[val:]...)
			} else if delta < 0 {
				newCount[i] = Pair{oldCount[i].Key, oldCount[i].Value}
				newCount[i].Value = append(newCount[i].Value, pool[:-delta]...)
				pool = pool[-delta:]
			} else {
				newCount[i] = Pair{oldCount[i].Key, oldCount[i].Value}
			}
		} else {
			newCount[i] = Pair{gids[i-nold], pool[:val]}
			pool = pool[val:]
		}
	}
	if len(pool) != 0 {
		panic("something wrong")
	}

	newShards := make([]int, len(shards))
	for _, pair := range newCount {
		g := pair.Key
		for _, s := range pair.Value {
			newShards[s] = g
		}
	}
	return newShards
}

func remove(shards []int, gids []int) []int {
	DPrintf("mmmmmmmmmmmmmmm %v, %v", shards, gids)
	oldCount := sortByShards(shards)
	sn := len(shards)
	gn := len(oldCount) - len(gids)
	if gn == 0 {
		return make([]int, len(shards))
	}
	newCount := make(PairList, 0)
	// for _, v := range newCount {
	// v.Value = make([]int, 0)
	// }
	avg := sn / gn
	rmd := sn % gn
	pool := make([]int, 0)
	// i := 0
	DPrintf("jjjjjjjj %v, %v", gids, oldCount)

	for i := 0; i < len(oldCount); i += 1 {
		if SliceContains(gids, oldCount[i].Key) {
			DPrintf("bdgddbdbdbbd")
			pool = append(pool, oldCount[i].Value...)
		} else {
			newCount = append(newCount, oldCount[i])
		}
	}

	for i := 0; i < gn; i += 1 {
		// DPrintf("111>>>>>>>>>>>>>> %d, %t, %v", oldCount[jj].Key, SliceContains(gids, oldCount[jj].Key), pool)
		// if SliceContains(gids, oldCount[jj].Key) {
		// 	pool = append(pool, oldCount[jj].Value...)
		// 	continue
		// }
		// DPrintf(">>>>>>>>>>>>>> %v", pool)

		val := avg
		if i < rmd {
			val += 1
		}

		DPrintf("ttttttarget %d, %d, %d, %v", i, rmd, val, newCount)

		delta := len(newCount[i].Value) - val
		if delta > 0 {
			// newCount[i] = Pair{newCount[i].Key, newCount[i].Value[:val]}
			pool = append(pool, newCount[i].Value[val:]...)
			newCount[i].Value = newCount[i].Value[:val]
		} else if delta < 0 {
			// newCount[i] = Pair{newCount[i].Key, newCount[i].Value}
			DPrintf("<<<<<<<<<< %v, delta %d, %v", pool, delta, newCount)
			newCount[i].Value = append(newCount[i].Value, pool[:-delta]...)
			pool = pool[-delta:]
		} else {
			// newCount[i] = Pair{newCount[i].Key, newCount[i].Value}
		}
		// i += 1
	}
	if len(pool) != 0 {
		panic(fmt.Sprintf("something wrong %v", pool))
	}

	newShards := make([]int, len(shards))
	for _, pair := range newCount {
		g := pair.Key
		for _, s := range pair.Value {
			newShards[s] = g
		}
	}
	DPrintf("fffffffff, %v", shards)
	return newShards
}

func sortByShards(shards []int) PairList {
	gidShards := make(map[int][]int)
	for s, v := range shards {
		gidShards[v] = append(gidShards[v], s)
	}

	i := 0
	pl := make(PairList, len(gidShards))
	for k, v := range gidShards {
		pl[i] = Pair{k, v}
		i += 1
	}
	sort.Sort(sort.Reverse(pl))
	return pl
}
