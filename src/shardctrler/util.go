package shardctrler

import (
	"fmt"
	"log"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

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
