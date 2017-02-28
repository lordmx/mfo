package main

import (
	"log"
	"time"

	"github.com/nightlyone/lockfile"
)

type OrderJob struct {
	Handler     *OrderHandler
	Gateway     *OrdersGateway
	Scheduler   *TaskScheduler
	LockFile    lockfile.Lockfile
	Queue       chan *Order
	Thread      chan int
	Done        chan bool
	ThreadCount int
	Limit       int
	Interval    int
}

func NewOrderJob(app *Application, handler *OrderHandler) *OrderJob {
	job := &OrderJob{
		Handler:     handler,
		Scheduler:   app.Scheduler,
		Gateway:     handler.Gateway,
		LockFile:    app.LockFile,
		ThreadCount: app.Config.Consumer.ThreadCount,
		Limit:       app.Config.Consumer.Limit,
		Interval:    app.Config.Consumer.Interval,
		Queue:       make(chan *Order, app.Config.Consumer.ThreadCount),
		Thread:      make(chan int),
		Done:        make(chan bool),
	}

	job.Schedule()

	return job
}

func (job *OrderJob) Run() error {
	log.Printf("[%v] OrderJob.Run()", time.Now())

	err := job.LockFile.TryLock()

	if err != nil {
		log.Printf("[%v] (!) %s", time.Now(), err.Error())
		return err
	}

	defer job.LockFile.Unlock()
	limit := job.Limit / job.ThreadCount

	go func() {
		for {
			select {
			case i := <-job.Thread:
				orders := job.Gateway.FindUnprocessed(limit, i*limit)

				for _, order := range orders {
					log.Printf(
						"[%v] Got an order with ID %d and status '%s'",
						time.Now(),
						order.Id,
						order.Status,
					)
					job.Queue <- order
				}
			case order := <-job.Queue:
				action := StatusAction

				if order.Status == StatusCreated {
					action = OrderAction
				}

				if job.Handler.RunCallback(action, order) {
					if order.OrderId == nil || *order.OrderId == 0 {
						order.Status = StatusError
					}

					job.Gateway.Update(order)
				}
			case <-job.Done:
				return
			}
		}
	}()

	for i := 0; i < job.ThreadCount; i++ {
		job.Thread <- i
	}

	defer func() {
		job.Done <- true
	}()

	return nil
}

func (job *OrderJob) Schedule() {
	when := &When{
		Every: Every(job.Interval).Seconds(),
	}

	job.Scheduler.Schedule("event_scheduler", job, when)
}
