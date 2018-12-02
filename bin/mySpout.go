package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

func main() {
	args := os.Args
	filePath := "/tmp/medium.txt"
	if len(args) == 2 {
		filePath = args[1]
	}

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	for {

	}
}
