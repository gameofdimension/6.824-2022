package shardkv

import "log"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
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
// vRPp1Dq83oJpHk1ivpXlIL3-b2zrbKCogn1RbDrs-U8p_vsqiejq0B48bmrD2LuCBZKdwUistdX8xUdB6NXUl-gqdE2OTzr5AOsDal5qdsqhQ8Z5SIpqRTEtlZkW3qhSQ2tt5IYvG9iv-8YJBU5rGHSrZuDC-dwa2uPcGfQL2hQO1GbssICUGVp8ClqM1S6AttIJ_FRKBDmrSxPDzFUegINWNG48UKxA_s4DGnTAwc9m5aA_caFNIim6_es-flkaaiLJKnQfVxENHY07RGmFYI7BBwenHkoRawFY7C6q5flIg6hkLzMOsJacFhV
// vRPp1Dq83oJpHk1ivpXlIL3-b2zrbKCogn1RbDrs-U8p_vsqiejq0B48bmrD2LuCBZKdwUistdX8xUdB6NXUl-gqdE2OTzr5AOsDal5qdsqhQ8Z5SIpqRTEtlZkW3qhSQ2tt5IYvG9iv-8YJBU5rGHSrZuDC-dwa2uPcGfQL2hQO1GbssICUGVp8ClqM1S6AttIJ_FRKBDmrSxPDzFUegINWNG48UKxA_s4DGnTAwc9m5aA_caFNIim6_es-flkaaiLJKnQfVxENHY07RGmFYI7BBwenHkoRawFY7C6q5flIg6hkLzMOsJacFhVacFhV