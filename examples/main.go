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

	inProgressCount := ctx.Job.QueueCount("in-progress")
	fmt.Println(inProgressCount)

	progress := ctx.Queue.GetProgress()
	fmt.Println("Progress", progress)
	ctx.Queue.SetProgress(progress + 20)

	defer ctx.Continue()
}

// BarHandler is a handler for bar job
func BarHandler(ctx *queum.Context) {
	fmt.Println("What is this", ctx.Job.Name)
	defer ctx.Done()
}

func main() {

	foo := queum.CreateJob("Foo").
		Every(5 * time.Second)

	foo.Queue(map[string]string{
		"test": "baba",
	})
	foo.Queue(map[string]string{
		"test": "what the fuck",
	})
	foo.Queue(map[string]string{
		"test": "hell yeah",
	})

	bar := queum.CreateJob("Bar").
		Every(10 * time.Second)

	bar.Queue(map[string]string{
		"hello": "world",
	})

	// Process all job
	queum.Process("Foo", FooHandler)
	queum.Process("Bar", BarHandler)
	queum.Start()
}
