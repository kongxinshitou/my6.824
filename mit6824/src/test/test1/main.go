package main

import "fmt"

type A struct {
	Name string
}

func (a A) Change(name string) {
	a.Name = name
}

func main() {
	a := A{
		Name: "John Doe",
	}
	a.Change("foo")
	fmt.Println(a)
}
