package fqueue

import "time"

type Scheduler struct {
	Interval time.Duration
	Task     func()
}

func NewScheduler(interval time.Duration, task func()) *Scheduler {
	return &Scheduler{
		Interval: interval,
		Task:task,
	}
}
