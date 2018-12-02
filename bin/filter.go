package main

import (
	"fmt"
	"os"
	"strings"
)

const threasHold = 10

func main() {
	args := os.Args
	if len(args) == 2 {
		words := strings.Fields(args[1])
		if len(words) > threasHold {
			fmt.Println(args[1])
		}
	} else {
		fmt.Println("Wrong input")
	}

}
