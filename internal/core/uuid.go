package core

import (
	crand "crypto/rand"
	"time"
)

func randRead(b []byte) (int, error) { return crand.Read(b) }

func nowUnixNano() int64 { return time.Now().UnixNano() }
