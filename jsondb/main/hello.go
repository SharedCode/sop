package main

import "C"
import "fmt"

//export hello
func hello() {
	fmt.Println("Hello from Go!")
}

//export add
func add(a, b C.long) C.long {
	return a + b
}

func main() {
	// main function is required for building a shared library, but can be empty
}
