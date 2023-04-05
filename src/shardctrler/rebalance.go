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

func (p PairList) Len() int { return len(p) }
func (p PairList) Less(i, j int) bool {
	if len(p[i].Value) != len(p[j].Value) {
		return len(p[i].Value) < len(p[j].Value)
	}
	return p[i].Key < p[j].Key
}
func (p PairList) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func add(shards []int, gids []int) []int {
	count0 := 0
	for _, v := range shards {
		if v == 0 {
			count0 += 1
		}
	}
	if count0 > 0 && count0 < NShards {
		panic(fmt.Sprintf("count of group 0 %d error", count0))
	}
	oldCount := sortByShards(shards)
	if oldCount.Len() == 1 && oldCount[0].Key == 0 {
		oldCount = make(PairList, 0)
	}

	disjoint := make([]int, 0)
	for _, v := range gids {
		if !SliceContains(shards, v) {
			disjoint = append(disjoint, v)
		}
	}
	pool := []int{}
	if oldCount.Len() == 0 {
		for i := 0; i < NShards; i += 1 {
			pool = append(pool, i)
		}
	}
	return assign(oldCount, disjoint, pool)
}

func assign(used PairList, notUsed []int, pool []int) []int {
	if len(used) > NShards {
		panic(fmt.Sprintf("group count %d bigger than %d", len(used), NShards))
	}
	shards := make([]int, 0)
	for _, pair := range used {
		shards = append(shards, pair.Key)
	}
	if len(used) == NShards {
		return shards
	}
	var gids []int
	if len(notUsed)+used.Len() <= NShards {
		gids = notUsed
	} else {
		gids = notUsed[:NShards-used.Len()]
	}
	gn := used.Len() + len(gids)
	if gn == 0 {
		return make([]int, NShards)
	}
	avg := NShards / gn
	rmd := NShards % gn

	newCount := make(PairList, gn)
	for i := 0; i < gn; i += 1 {
		val := avg
		if i < rmd {
			val += 1
		}
		if i < used.Len() {
			newCount[i] = Pair{Key: used[i].Key, Value: used[i].Value}
			if len(used[i].Value) > val {
				pool = append(pool, used[i].Value[val:]...)
				newCount[i].Value = used[i].Value[:val]
			} else if len(used[i].Value) < val {
				newCount[i].Value = append(newCount[i].Value, pool[:val-len(used[i].Value)]...)
				pool = pool[val-len(used[i].Value):]
			}
		} else {
			newCount[i] = Pair{Key: gids[i-used.Len()], Value: pool[:val]}
			pool = pool[val:]
		}
	}
	if len(pool) != 0 {
		panic(fmt.Sprintf("not balance %v", pool))
	}

	newShards := make([]int, NShards)
	for i, _ := range newCount {
		gid := newCount[i].Key
		for _, v := range newCount[i].Value {
			newShards[v] = gid
		}
	}
	return newShards
}

func remove(shards []int, gids []int) []int {
	oldCount := sortByShards(shards)
	diff := make([]int, 0)
	for _, pair := range oldCount {
		if !SliceContains(gids, pair.Key) {
			diff = append(diff, pair.Key)
		}
	}
	newCount := make(PairList, 0)
	inUse := make([]int, 0)
	pool := make([]int, 0)
	for i := 0; i < len(oldCount); i += 1 {
		if SliceContains(diff, oldCount[i].Key) {
			pool = append(pool, oldCount[i].Value...)
		} else {
			newCount = append(newCount, oldCount[i])
			inUse = append(inUse, oldCount[i].Key)
		}
	}
	notUsed := make([]int, 0)
	for _, v := range gids {
		if !SliceContains(inUse, v) {
			notUsed = append(notUsed, v)
		}
	}
	return assign(newCount, notUsed, pool)
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
