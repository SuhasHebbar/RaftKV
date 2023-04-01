package main

import "time"

type Empty struct{}

// Handle irritating unused variable warnings.
func Use(args ...any) {
	for arg := range args {
		_ = arg
	}
}

// Reset timeout of timer t to duration d.
func ResetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		<- t.C
	}

	t.Reset(d)
}

