package main

import (
	"crypto/rand"
	"fmt"
	"time"
)

func GetUniqId() (result string) {
	b := make([]byte, 16)
	rand.Read(b)
	result = fmt.Sprintf("%X", int(time.Now().Unix())) + fmt.Sprintf("%X", b)
	return
}
