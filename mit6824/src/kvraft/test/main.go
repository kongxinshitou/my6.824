package main

import "fmt"

func main() {
	var l int
	var ok bool
	k := map[int]int{0: 100}
	if l, ok = k[0]; ok {
		goto PP
	}
PP:
	fmt.Println(l)
}
