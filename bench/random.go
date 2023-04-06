package bench

import (
    "math/rand"
)

// Source: https://stackoverflow.com/a/31832326/12160191
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(prng *rand.Rand, n int32) string {
    b := make([]rune, n)
    for i := range b {
        b[i] = letterRunes[prng.Intn(len(letterRunes))]
    }
    return string(b)
}
