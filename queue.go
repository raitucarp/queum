package queum

import "fmt"

// Queue is a single queue unit
type Queue struct {
	details   map[string]string
	Data      interface{}
	parentJob *Job
}

func (q *Queue) getParentKey() string {
	// get unique key of job
	return q.parentJob.getUniqueKey()
}

func (q *Queue) getParentFullNSKey() string {
	// get unique key of job
	uniqueJobKey := q.getParentKey()

	// get full job key
	return buildKey(globalKey, "job", uniqueJobKey)
}

func (q *Queue) getKey() string {
	// get full job key
	fullParentJobKey := q.getParentFullNSKey()

	// in-progress list key
	inProgressKey := buildKey(fullParentJobKey, "in-progress")

	// the queue key
	firstQueueKey, err := client.LIndex(inProgressKey, -1).Result()
	if err != nil {
		panic(err)
	}

	return firstQueueKey
}

func (q *Queue) getFullNSKey() string {
	fullParentJobKey := q.getParentFullNSKey()
	key := q.getKey()

	// build queue key
	queueKey := buildKey(fullParentJobKey, "q", key)

	return queueKey
}

func (q *Queue) fetchData() {
	queueKey := q.getFullNSKey()

	fmt.Println("queueKey", queueKey)

	// get queue data
	data, err := client.HGetAll(queueKey).Result()
	if err != nil {
		panic(err)
	}

	q.details = data
	q.Data = data["data"]
}

func (q *Queue) done() {
	q.SetProgress(100)

	// get full job key
	fullParentJobKey := q.getParentFullNSKey()

	// in-progress list key
	inProgressKey := buildKey(fullParentJobKey, "in-progress")
	completedKey := buildKey(fullParentJobKey, "completed")

	err := client.RPopLPush(inProgressKey, completedKey).Err()
	if err != nil {
		panic(err)
	}
}

// SetProgress set progress of current queue job
func (q *Queue) SetProgress(n float64) {
	key := q.getFullNSKey()

	err := client.HSet(key, "progress", n).Err()
	if err != nil {
		panic(err)
	}

}

// GetProgress get progress of current queue job
func (q *Queue) GetProgress() (progress float64) {
	key := q.getFullNSKey()

	progress, err := client.HGet(key, "progress").Float64()
	if err != nil {
		panic(err)
	}

	return
}
