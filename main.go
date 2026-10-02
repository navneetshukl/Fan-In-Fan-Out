package main

import (
	"log"
	"sync"
)

var (
	workers int = 3
)

func execute(input, output chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	for val := range input {
		output <- val * val
	}
	close(output)
}

func main() {
	input := make(chan int)
	output := make([]chan int, workers)
	global := make(chan int)
	wg := sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		output[i] = make(chan int)
		go execute(input, output[i], &wg)
	}

	wg1 := &sync.WaitGroup{}
	wg1.Add(2)

	go func() {
		defer wg1.Done()

		wg2 := &sync.WaitGroup{}

		for i := range workers {
			ch := output[i]
			wg2.Add(1)
			go func(ch chan int) {
				defer wg2.Done()
				for v := range ch {
					global <- v
				}
			}(ch)
		}
		wg2.Wait()
		close(global)
	}()

	go func() {
		defer wg1.Done()
		for v := range global {
			log.Println(v)
		}
	}()

	for i := 1; i <= 100; i++ {
		input <- i
	}
	close(input)

	wg.Wait()
	wg1.Wait()

}
