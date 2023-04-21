package main

import "fmt"

func main() {
	l := map[int]chan int{}
	k := l[0]
	fmt.Println(k)

}
