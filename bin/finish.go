package main

import (
	"fmt"
	"os"
)

func main() {
	args := os.Args
	if len(args) == 2 {
		f, err := os.OpenFile("/tmp/uppercase.out", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			panic(err)
		}

		defer f.Close()

		if _, err = f.WriteString(args[1] + "\n"); err != nil {
			panic(err)
		}
	} else {
		fmt.Println("Wrong input")
	}

}
