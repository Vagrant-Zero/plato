package cache

import "time"

const (
	MaxClientIDKey  = "max_client_id_{%d}_%d" // slot_connID
	LastMsgKey      = "last_msg_{%d}_%d"      // slot_connID
	LoginSlotSetKey = "login_slot_set_{%d}"   // 通过 hash tag保证在cluster模式上 key都在一个shard上 // slot
	TTL7D           = 7 * 24 * time.Hour
)
