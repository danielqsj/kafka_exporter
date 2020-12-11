package main

import "sync"

func main() {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go slowConsumer(wg)
	wg.Add(1)
	go slowProducer(wg)
	wg.Wait()
}
