package gateway

import (
	"sync"
)

var tables table

type table struct {
	did2conn sync.Map
}

func InitTable() {
	tables = table{
		did2conn: sync.Map{},
	}
}
