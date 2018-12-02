package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	args := os.Args
	if len(args) == 2 {
		fmt.Println(strings.ToUpper(args[1]))
	} else {
		fmt.Println("Wrong input")
	}
}
