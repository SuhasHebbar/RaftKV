package main

import (
	"fmt"
	"sync"

	kv "github.com/SuhasHebbar/CS739-P2"
)

var c *kv.SimpleClient

func main() {
	c = kv.NewSimpleClient()
	testMultiThreadConsistency()
}

func testMultiThreadConsistency() {

	wg := new(sync.WaitGroup)

	wg.Add(2)

	go firstThread(wg)

	go secondThread(wg)

	wg.Wait()

	arguments := "1"
	fmt.Println("Sending Get Request: Get ", arguments)
	c.HandleGet(arguments, true, []int{}, false)

	arguments = "1"
	fmt.Println("Sending Get Request: Get ", arguments)
	c.HandleGet(arguments, true, []int{}, false)
}

func firstThread(wg *sync.WaitGroup) {
	defer wg.Done()
	arguments := "1 2"
	fmt.Println("Sending Request from thread 1: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)
}

func secondThread(wg *sync.WaitGroup) {
	defer wg.Done()
	arguments := "1 3"
	fmt.Println("Sending Request: from thread 2: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)

}
