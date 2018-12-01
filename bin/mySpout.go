package main

import "fmt"

func main() {
	sentences := []string{"the cow jumped over the moon", "an apple a day keeps the doctor away", "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature"}
	for i := 0; i < 100; i++ {
		for _, sentence := range sentences {
			fmt.Println(sentence)
		}
	}
}
