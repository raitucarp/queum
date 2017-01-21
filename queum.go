package queum

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"gopkg.in/redis.v5"
)

// STATUS is a job status
type STATUS int

const (
	// STOP is a state when a job is stopped
	STOP STATUS = iota

	// RUNNING is a state when job is running
	RUNNING

	// RESUME is a state when a job got resumed
	RESUME

	// PAUSE is a state when a job is paused
	PAUSE
)

// Context is a single job context
type Context struct {
	Job   *Job
	Data  interface{}
	Queue *Queue
}

// Queue is a single queue unit
type Queue struct {
	Progress  int
	Data      interface{}
	parentJob *Job
}

// Job is the main structure of job
type Job struct {
	Name     string
	Interval time.Duration
	status   STATUS
	ticker   *time.Ticker
	handler  func(ctx *Context)
}

// Jobs is a collection of a job
type Jobs map[string]*Job

// get
func (jobs *Jobs) get(name string) (j *Job) {
	// range from jobs
	for _, job := range *jobs {
		// match the job
		if job.Name == name {
			// return if match
			return job
		}
	}
	return
}

// applyHandler to a job in a list
func (jobs *Jobs) applyHandler(name string, handler func(ctx *Context)) {
	job := jobs.get(name)
	job.handler = handler
}

var (
	redisOptions *redis.Options
	jobs         Jobs
	client       *redis.Client
	globalKey    string
	jobsKey      string
)

func init() {
	jobs = Jobs{}
	redisOptions = &redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}
	globalKey = "queum"
	jobsKey = buildKey("jobs")
}

// Every apply every time
func (job *Job) Every(duration time.Duration) *Job {
	// apply interval with duration
	job.Interval = duration
	// set job ticker with the duration
	job.ticker = time.NewTicker(duration)

	// return job for chaining
	return job
}

// insert job to job list (jobs)
func (job *Job) insertJob() {
	// generate unique job key with current job's name
	uniqueJobKey := generateJobKey(job.Name)

	// build key for redis
	jobListKey := buildKey(globalKey, "jobs")

	// Hash set with job key
	err := client.HSet(jobListKey, job.Name, uniqueJobKey).Err()
	if err != nil {
		panic(err)
	}
}

// getFirstQueue is get job queue from the current job
func (job *Job) getFirstQueue() *Queue {
	// get unique key of job
	uniqueJobKey := job.getUniqueKey()

	// get full job key
	fullJobKey := buildKey(globalKey, "job", uniqueJobKey)

	fmt.Println(fullJobKey)

	// the queue key
	firstQueueKey, err := client.LIndex(fullJobKey, 0).Result()
	if err != nil {
		panic(err)
	}

	// build queue key
	queueKey := buildKey(fullJobKey, "q", firstQueueKey)

	// get queue data
	data, err := client.HGetAll(queueKey).Result()
	if err != nil {
		panic(err)
	}

	progress, err := strconv.Atoi(data["progress"])
	if err != nil {
		panic(err)
	}

	return &Queue{
		Progress: progress,
		Data:     data["data"],
	}
}

// Run the job
func (job *Job) Run() {
	job.status = RUNNING
	job.insertJob()

	go func() {
		for {
			select {
			case <-job.ticker.C:
				queueCount := job.QueueCount()
				if job.status == RUNNING && queueCount > 0 {
					q := job.getFirstQueue()
					ctx := Context{
						Job:   job,
						Queue: q,
					}

					// do stuff
					job.handler(&ctx)
				}
			}
		}
	}()
}

// Pause the job
func (job *Job) Pause() {
	job.status = PAUSE
}

// Resume the job
func (job *Job) Resume() {
	job.status = RESUME
}

// Stop the job
func (job *Job) Stop() {
	job.status = STOP
}

// GetStatus get the current status of the job
func (job *Job) GetStatus() STATUS {
	return job.status
}

// QueueCount get queue count in the current job.
func (job *Job) QueueCount() int64 {
	uniqueJobKey := job.getUniqueKey()
	fullJobKey := buildKey(globalKey, "job", uniqueJobKey)

	count, err := client.LLen(fullJobKey).Result()
	if err != nil {
		panic(err)
	}
	return count
}

// getKey get the current job unique key in db
func (job *Job) getUniqueKey() (uniqueKey string) {
	key := buildKey(globalKey, jobsKey)

	// get unique key with hash get
	uniqueKey, err := client.HGet(key, job.Name).Result()
	if err != nil {
		panic(err)
	}

	// empty key
	return
}

func (job *Job) insertQueue(data interface{}) {
	uniqueJobKey := job.getUniqueKey()
	queueKey := randStringRunes(10)
	value, errMarshal := json.Marshal(data)
	if errMarshal != nil {
		panic(errMarshal)
	}

	fullJobKey := buildKey(globalKey, "job", uniqueJobKey)
	err := client.RPush(fullJobKey, queueKey).Err()
	if err != nil {
		panic(err)
	}

	fullQueueKey := buildKey(fullJobKey, "q", queueKey)
	err = client.HMSet(fullQueueKey, map[string]string{
		"data":     string(value),
		"progress": "0",
	}).Err()

	if err != nil {
		panic(err)
	}
}

// Queue is a method that send data to the job
func (job *Job) Queue(data interface{}) {
	go func() {
		for {
			if job.status == RUNNING {
				job.insertQueue(data)

				return
			}
		}
	}()

}

// SetStoreOptions provide options for redis
func SetStoreOptions(opts interface{}) {
	redisOptions = opts.(*redis.Options)
}

// SetNameSpace is for reset redis key namespace
func SetNameSpace(name string) {
	globalKey = name
}

// CreateJob is a method that job
func CreateJob(name string) *Job {
	job := Job{
		Name: name,
	}
	jobs[name] = &job
	return &job
}

// Process is a method
func Process(name string, f func(ctx *Context)) {
	jobs.applyHandler(name, f)
}

// Start do start all job
func Start() {
	client = redis.NewClient(redisOptions)
	quit := make(chan struct{})
	for _, job := range jobs {
		go job.Run()
	}
	<-quit
}
