package main

import (
	"fmt"
	"sync"

	kv "github.com/SuhasHebbar/CS739-P2"
)

var c *kv.TestClient

func main() {
	c = kv.NewTestClient()
	testMultiThreadConsistency()
}

func testMultiThreadConsistency() {

	wg := new(sync.WaitGroup)

	wg.Add(2)

	go firstThread(wg)

	go secondThread(wg)

	wg.Wait()

	arguments := "a"
	fmt.Println("Sending Get Request: Get ", arguments)
	c.HandleGet(arguments, true, []int{}, false)

	arguments = "a"
	fmt.Println("Sending Get Request: Get ", arguments)
	c.HandleGet(arguments, true, []int{}, false)
}

func firstThread(wg *sync.WaitGroup) {
	defer wg.Done()
	arguments := "a 2"
	fmt.Println("Sending Request from thread 1: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)
}

func secondThread(wg *sync.WaitGroup) {
	defer wg.Done()
	arguments := "a 3"
	fmt.Println("Sending Request: from thread 2: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)

}
