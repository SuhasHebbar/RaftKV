package main

import (
	"fmt"

	"golang.org/x/exp/slog"
)

type Empty struct{}

// Handle irritating unused variable warnings.
func Use(args ...any) {
	for arg := range args {
		_ = arg
	}
}

func min32(a, b int32) int32 {
    if a < b {
        return a
    }
    return b
}	

func Infof(format string, args ...any) {
    slog.Default().Info(fmt.Sprintf(format, args...))
}


func Debugf(format string, args ...any) {
    slog.Default().Debug(fmt.Sprintf(format, args...))
}
