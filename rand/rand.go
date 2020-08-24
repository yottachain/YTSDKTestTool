package rand

import (
	"math/rand"
	"time"
)

var R = rand.New(rand.NewSource(time.Now().UnixNano()))


func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		rr := R.Intn(3)
		var b int
		if rr  == 0 {
			b = R.Intn(26) + 65
		}else if rr == 1 {
			b = R.Intn(26) + 97
		}else {
			b = R.Intn(10) + 48
		}

		bytes[i] = byte(b)
	}
	return string(bytes)
}