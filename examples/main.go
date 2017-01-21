package main

import (
	"fmt"
	"time"

	"github.com/raitucarp/queum"
)

// FooHandler is a handler for test job
func FooHandler(ctx *queum.Context) {
	fmt.Println("----------------------")
	fmt.Println("Details of job", ctx.Job.Name)
	fmt.Println(ctx.Queue.Data)
}

// BarHandler is a handler for bar job
func BarHandler(ctx *queum.Context) {
	fmt.Println("What is this", ctx.Job.Name)
}

func main() {
	data := map[string]string{
		"test": "baba",
	}

	foo := queum.CreateJob("Foo")
	foo.Every(5 * time.Second)
	foo.Queue(data)
	foo.Queue(data)
	foo.Queue(data)

	bar := queum.CreateJob("Bar")
	bar.Every(10 * time.Second)

	// Process all job
	queum.Process("Foo", FooHandler)
	queum.Process("Bar", BarHandler)
	queum.Start()
}
