// You can edit this code!
// Click here and start typing.
package main

import (
	"fmt"
	"time"
)

func main() {
	ch := make(chan bool)
	for {
		select {
		case <-ch:
			break
		case <-time.After(2 * time.Second):
			break
		}
		fmt.Println("hi")
	}

	fmt.Println("Hello, playground")
}
