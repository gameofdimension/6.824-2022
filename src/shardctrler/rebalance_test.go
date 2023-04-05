package shardctrler

import "testing"

func TestAdd(t *testing.T) {
	shards := []int{4, 1, 1, 1, 2, 2, 2, 2, 2, 2}
	newShards := add(shards, []int{1, 2, 3, 4})
	if len(newShards) != len(shards) {
		t.Fatalf("wrong %v", newShards)
	}
	for i := 0; i < 7; i += 1 {
		if newShards[i] != shards[i] {
			t.Fatalf("expect %d at %d", shards[i], i)
		}
	}
	count4 := 0
	count3 := 0
	for i := 0; i < len(newShards); i += 1 {
		if newShards[i] == 4 {
			count4 += 1
		}
		if newShards[i] == 3 {
			count3 += 1
		}
	}
	if count4 != 2 {
		t.Fatalf("expect 4 count [%d vs %d]", 2, count4)
	}
	if count3 != 2 {
		t.Fatalf("expect 3 count [%d vs %d]", 2, count3)
	}
}

func TestRemove(t *testing.T) {
	shards := []int{4, 1, 1, 2, 2, 2, 3, 3, 3, 3}
	newShards := remove(shards, []int{1, 3, 4})

	for _, k := range []int{0, 1, 2, 6, 7, 8, 9} {
		if newShards[k] != shards[k] {
			t.Fatalf("expect %d at %d", shards[k], k)
		}
	}
	count4 := 0
	count1 := 0
	count3 := 0
	for i := 0; i < len(newShards); i += 1 {
		if newShards[i] == 4 {
			count4 += 1
		}
		if newShards[i] == 1 {
			count1 += 1
		}
		if newShards[i] == 3 {
			count3 += 1
		}
	}
	if count4 != 3 {
		t.Fatalf("expect 0 count [%d vs %d]", 3, count4)
	}
	if count1 != 3 {
		t.Fatalf("expect 1 count [%d vs %d]", 3, count1)
	}
	if count3 != 4 {
		t.Fatalf("expect 3 count [%d vs %d]", 4, count3)
	}
}

func TestDebug(t *testing.T) {
	shards := []int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2}
	newShards := remove(shards, []int{2})
	for _, v := range newShards {
		if v != 2 {
			t.Fatalf("expect %d vs %d", v, 2)
		}
	}
}
