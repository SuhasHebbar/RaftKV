package main

import (
	"fmt"

	kv "github.com/SuhasHebbar/CS739-P2"
)

var c *kv.TestClient

func main() {
	c = kv.NewTestClient()
	testSingleThreadConsistency()
}

func testSingleThreadConsistency() {
	arguments := "a 2"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)

	arguments = "a 3"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)

	arguments = "a"
	fmt.Println("Sending Get Request: Get ", arguments)
	c.HandleGet(arguments, true, []int{}, false)

	arguments = "a"
	fmt.Println("Sending Get Request: Get ", arguments)
	c.HandleGet(arguments, true, []int{}, false)
}
