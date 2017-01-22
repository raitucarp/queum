package queum

import (
	"hash/fnv"
	"math/rand"
	"strings"
	"time"
)

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func hashFromString(s string, n int) string {
	h := hash(s)
	rand.Seed(int64(h))

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func randStringRunes(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func buildKey(keys ...string) string {
	// theKey := append([]string{globalKey}, keys...)
	return strings.Join(keys, ":")
}

func generateJobKey(name string) string {
	return hashFromString(name, 10)
}
