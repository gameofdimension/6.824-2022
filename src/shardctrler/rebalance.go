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
	oldCount := sortByShards(shards)
	sn := len(shards)
	gn := len(gids) + len(oldCount)
	newCount := make(PairList, gn)
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
	oldCount := sortByShards(shards)
	sn := len(shards)
	gn := len(oldCount) - len(gids)
	newCount := make(PairList, gn)
	avg := sn / gn
	rmd := sn % gn
	pool := make([]int, 0)
	cc := 0
	for i := 0; i < len(oldCount); i += 1 {
		if SliceContains(gids, oldCount[i].Key) {
			pool = append(pool, oldCount[i].Value...)
			continue
		}

		val := avg
		if cc < rmd {
			val += 1
		}

		delta := len(oldCount[i].Value) - val
		if delta > 0 {
			newCount[cc] = Pair{oldCount[i].Key, oldCount[i].Value[:val]}
			pool = append(pool, oldCount[i].Value[val:]...)
		} else if delta < 0 {
			newCount[cc] = Pair{oldCount[i].Key, oldCount[i].Value}
			newCount[cc].Value = append(newCount[cc].Value, pool[:-delta]...)
			pool = pool[-delta:]
		} else {
			newCount[cc] = Pair{oldCount[i].Key, oldCount[i].Value}
		}
		cc += 1
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
