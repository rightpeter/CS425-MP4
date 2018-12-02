package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	args := os.Args
	if len(args) == 2 {
		words := strings.Fields(args[1])
		if len(words) > 15 {
			fmt.Println(args[1])
		}
	} else {
		fmt.Println("Wrong input")
	}

}
