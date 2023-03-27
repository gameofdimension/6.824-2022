package raft

import "fmt"

type VirtualLog struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []LogEntry
}

// virtual index to physical index
func (vl *VirtualLog) v2p(p int) int {
	return p - vl.LastIncludedIndex - 1
}

// physical index to virtual index
func (vl *VirtualLog) p2v(p int) int {
	return p + vl.LastIncludedIndex + 1
}

func (vl *VirtualLog) GetLastIncludedIndex() int {
	return vl.LastIncludedIndex
}

func (vl *VirtualLog) isEmpty() bool {
	return vl.LastIncludedIndex == 0 && len(vl.Data) == 0
}

func (vl *VirtualLog) SnapshotAt(index int) {
	paddr := vl.v2p(index)
	vl.LastIncludedIndex = index
	vl.LastIncludedTerm = vl.Data[paddr].Term
	vl.Data = vl.Data[:paddr+1]
}

func (vl *VirtualLog) GetItem(index int) *LogEntry {
	p := vl.v2p(index)
	if p < 0 {
		panic(fmt.Sprintf("illegal index %d vs %d\n", index, vl.LastIncludedIndex))
	}
	return &vl.Data[p]
}

func (vl *VirtualLog) GetTermAtIndex(index int) int {
	if index > vl.GetLastIncludedIndex() {
		return vl.GetItem(index).Term
	}
	if vl.LastIncludedIndex != index {
		panic(fmt.Sprintf("index %d vs %din snapshot", index, vl.LastIncludedIndex))
	}
	return vl.LastIncludedTerm
}

func (vl *VirtualLog) AddItem(entry LogEntry) {
	vl.Data = append(vl.Data, entry)
}

func (vl *VirtualLog) NextIndex() int {
	return vl.p2v(len(vl.Data))
}

func (vl *VirtualLog) GetLastItem() (int, *LogEntry) {
	p := vl.p2v(len(vl.Data) - 1)
	if len(vl.Data) == 0 {
		panic("data empty")
	}
	return p, &vl.Data[len(vl.Data)-1]
}

func (vl *VirtualLog) GetLastIndexTerm() (int, int) {
	if len(vl.Data) == 0 {
		return vl.LastIncludedIndex, vl.LastIncludedTerm
	}
	i, v := vl.GetLastItem()
	return i, v.Term
}

func (vl *VirtualLog) Slice(from int) []LogEntry {
	p := vl.v2p(from)
	return vl.Data[p:]
}

func (vl *VirtualLog) CopyEntries(start int, entries []LogEntry) {
	index := start
	for i := 0; i < len(entries); i += 1 {
		if index >= vl.NextIndex() {
			left := entries[i:]
			vl.Data = append(vl.Data, left...)
			break
		}
		entryTerm := vl.GetItem(index).Term
		if entryTerm != entries[i].Term {
			paddr := vl.v2p(index)
			vl.Data = vl.Data[:paddr]
			left := entries[i:]
			vl.Data = append(vl.Data, left...)
			break
		}
		index += 1
	}
}
