// You can edit this code!
// Click here and start typing.
package main

import (
	"fmt"
	"time"
)

func Bg(c chan int) {
	<-time.After(1 * time.Second)
	c <- 1
	close(c)
}

func Consume(c chan int) {
	for {
		<-c
	}
}

func main() {
	ch := make(chan int, 5)
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				fmt.Println("ch closed")
				ch = nil
			}
			fmt.Println(c)
			break
		default:
			fmt.Println("No available chan")
			return
		}
		fmt.Println("hi")
	}
	close(ch)
	fmt.Println("Hello, playground")
	<-time.After(2 * time.Second)
}
