package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	file, err := os.Open("/tmp/large.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		//time.Sleep(100 * time.Millisecond)
		fmt.Println(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
