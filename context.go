package queum

// Context is a single job context
type Context struct {
	Job   *Job
	Data  interface{}
	Queue *Queue
}

// Done mark current queue to done
func (ctx *Context) Done() {
	ctx.Queue.done()
	ctx.Job.Resume()
}

// Continue mark current queue to continue state
func (ctx *Context) Continue() {
	if ctx.Queue.GetProgress() >= 100 {
		ctx.Queue.done()
	}
	ctx.Job.Resume()
}
