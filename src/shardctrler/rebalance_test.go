package shardctrler

import "testing"

func TestAdd(t *testing.T) {
	shards := []int{0, 1, 1, 1, 2, 2, 2, 2, 2, 2}
	newShards := add(shards, []int{3})
	if len(newShards) != len(shards) {
		t.Fatalf("wrong %v", newShards)
	}
	for i := 0; i < 7; i += 1 {
		if newShards[i] != shards[i] {
			t.Fatalf("expect %d at %d", shards[i], i)
		}
	}
	count0 := 0
	count3 := 0
	for i := 0; i < len(newShards); i += 1 {
		if newShards[i] == 0 {
			count0 += 1
		}
		if newShards[i] == 3 {
			count3 += 1
		}
	}
	if count0 != 2 {
		t.Fatalf("expect 0 count [%d vs %d]", 2, count0)
	}
	if count3 != 2 {
		t.Fatalf("expect 3 count [%d vs %d]", 2, count3)
	}
}

func TestRemove(t *testing.T) {
	shards := []int{0, 1, 1, 2, 2, 2, 3, 3, 3, 3}
	newShards := remove(shards, []int{2})

	for _, k := range []int{0, 1, 2, 6, 7, 8, 9} {
		if newShards[k] != shards[k] {
			t.Fatalf("expect %d at %d", shards[k], k)
		}
	}
	count0 := 0
	count1 := 0
	count3 := 0
	for i := 0; i < len(newShards); i += 1 {
		if newShards[i] == 0 {
			count0 += 1
		}
		if newShards[i] == 1 {
			count1 += 1
		}
		if newShards[i] == 3 {
			count3 += 1
		}
	}
	if count0 != 3 {
		t.Fatalf("expect 0 count [%d vs %d]", 3, count0)
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
	newShards := remove(shards, []int{1})
	for _, v := range newShards {
		if v != 2 {
			t.Fatalf("expect %d vs %d", v, 2)
		}
	}

	shards = []int{1170, 1170, 1170, 1170, 1160, 1160, 1160, 1170, 1160, 1160}
	newShards = remove(shards, []int{1170})
	t.Fatalf("bbbbbb %v", newShards)
}
