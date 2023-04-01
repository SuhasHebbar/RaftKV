package main

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
