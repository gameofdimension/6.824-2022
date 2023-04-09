package shardkv

import "log"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if !Debug {
		log.Printf(format, a...)
	}
	return
}

func MergeMap(a map[string]string, b map[string]string) map[string]string {
	for k, v := range b {
		a[k] = v
	}
	return a
}
