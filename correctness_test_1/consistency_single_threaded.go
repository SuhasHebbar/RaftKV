package main

import (
	"fmt"

	kv "github.com/SuhasHebbar/CS739-P2"
)

var c *kv.SimpleClient

func main() {
	c = kv.NewSimpleClient()
	testSingleThreadConsistency()
}

func testSingleThreadConsistency() {
	arguments := "1 2"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)

	arguments = "1 3"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)

	arguments = "1"
	fmt.Println("Sending Get Request: Get ", arguments)
	c.HandleGet(arguments, true, []int{}, false)

	arguments = "1"
	fmt.Println("Sending Get Request: Get ", arguments)
	c.HandleGet(arguments, true, []int{}, false)
}
