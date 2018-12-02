package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"
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
		time.Sleep(50 * time.Millisecond)
		fmt.Println(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
