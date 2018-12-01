package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	args := os.Args
	if len(args) > 1 {
		words := strings.Fields(args[1])
		for _, word := range words {
			fmt.Println(word)
		}
	} else {
		fmt.Println("No input")
	}

}
