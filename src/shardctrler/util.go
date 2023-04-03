package shardctrler

import "fmt"

func SliceContains(hs []int, n int) bool {
	for _, v := range hs {
		if v == n {
			return true
		}
	}
	return false
}

func SliceToArr(shards []int) [NShards]int {
	if len(shards) != NShards {
		panic(fmt.Sprintf("shard len error %d vs %d", len(shards), NShards))
	}
	var arr [NShards]int
	for i, v := range shards {
		arr[i] = v
	}
	return arr
}
