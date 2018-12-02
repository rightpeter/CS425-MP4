package main

import (
	"fmt"
	"os"
)

func main() {
	args := os.Args
	if len(args) == 2 {
		fmt.Println("%s count 1", args[1])
	} else {
		fmt.Println("Wrong input")
	}
}
