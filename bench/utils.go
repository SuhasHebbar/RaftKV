package bench

import (
	"fmt"

	"golang.org/x/exp/slog"
)

func Infof(format string, args ...any) {
    slog.Default().Info(fmt.Sprintf(format, args...))
}


func Debugf(format string, args ...any) {
    slog.Default().Debug(fmt.Sprintf(format, args...))
}
