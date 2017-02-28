package main

import (
	"errors"
	"fmt"
	"log"
	"math"
	"regexp"
	"strconv"
	"sync"
	"time"
)

const (
	NoDay = iota
	Sun
	Mon
	Tue
	Wed
	Thu
	Fri
	Sat

	tNone = iota
	tMillisecond
	tSecond
	tMinute
	tHour
	tDay
	tWeek
)

type Opts struct {
	When *When

	RetryCount int
	Timeout    time.Duration
}

type When struct {
	LastRun time.Time
	Time    time.Time
	Each    string

	Every *every
	On    int
	At    string
}

type every struct {
	t int
	n int
}

type Job interface {
	Run() error
}

type TaskScheduler struct {
	jobs    map[string]*jobC
	started bool

	wg sync.WaitGroup
	mu sync.Mutex
}

type jobC struct {
	scheduler  *TaskScheduler
	job        Job
	retryCount int
	when       *When
	forever    bool
	timer      *time.Timer
	cancelSig  chan bool
}

func (s *TaskScheduler) Schedule(name string, job Job, when *When) error {
	return s.ScheduleWithOpts(name, job, &Opts{When: when})
}

func (s *TaskScheduler) ScheduleWithOpts(name string, job Job, opts *Opts) (err error) {
	s.mu.Lock()

	defer s.mu.Unlock()

	if _, ok := s.jobs[name]; ok {
		return errors.New("A job already exists with the name provided")
	}

	if opts.When == nil || opts.When.Duration(time.Now()) == 0 {
		return errors.New("Not a valid opts.When is provided")
	}

	if s.jobs == nil {
		s.jobs = make(map[string]*jobC)
	}

	s.jobs[name] = &jobC{
		scheduler:  s,
		job:        job,
		retryCount: opts.RetryCount,
		when:       opts.When,
		forever:    opts.When.Every != nil,
		cancelSig:  make(chan bool),
	}

	if s.started {
		s.wg.Add(1)
		s.jobs[name].schedule()
	}

	return
}

func (s *TaskScheduler) Cancel(name string) {
	s.mu.Lock()

	defer s.mu.Unlock()

	job, ok := s.jobs[name]

	if !ok {
		return
	}

	job.cancel()

	delete(s.jobs, name)
}

func (s *TaskScheduler) Start() {
	log.Printf("[%v] Scheduler was started", time.Now())

	s.started = true

	for _, j := range s.jobs {
		s.wg.Add(1)
		j.schedule()
	}

	s.wg.Wait()
}

func (j *jobC) schedule() {
	select {
	case <-j.cancelSig:
		j.timer.Stop()
		j.done()
		return
	default:
		if j.when.LastRun.IsZero() {
			j.when.LastRun = time.Now()
		}

		dur := j.when.Next(j.when.LastRun)

		j.timer = time.AfterFunc(dur, func() {
			j.run()
			j.when.LastRun = time.Now()

			if j.forever {
				j.schedule()
				return
			}

			j.done()
		})
	}
}

func (j *jobC) run() {
retryLoop:
	for i := 0; i < j.retryCount+1; i++ {
		if err := j.job.Run(); err == nil {
			break retryLoop
		}
	}
}

func (j *jobC) cancel() {
	j.cancelSig <- true

	if j.timer != nil {
		j.timer.Stop()
	}
}

func (j *jobC) done() {
	j.scheduler.wg.Done()
}

func Every(n int) *every {
	if n < 1 {
		n = 1
	}

	return &every{t: tSecond, n: n}
}

func (e *every) Milliseconds() *every {
	e.t = tMillisecond
	return e
}

func (e *every) Seconds() *every {
	e.t = tSecond
	return e
}

func (e *every) Minutes() *every {
	e.t = tMinute
	return e
}

func (e *every) Hours() *every {
	e.t = tHour
	return e
}

func (e *every) Days() *every {
	e.t = tDay
	return e
}

func (e *every) Weeks() *every {
	e.t = tWeek
	return e
}

func (w *When) Next(start time.Time) time.Duration {
	var interval, diff time.Duration
	interval = w.Duration(start)

	for interval > 0 {
		diff = start.Add(interval).Sub(time.Now())

		if diff > 0 {
			break
		}

		interval += w.Duration(start.Add(interval))
	}

	return diff
}

func (w *When) Duration(start time.Time) time.Duration {
	if w.Each != "" {
		dur, _ := time.ParseDuration(w.Each)
		return dur
	}

	if !w.Time.IsZero() {
		return w.Time.Sub(start)
	}

	if w.Every == nil {
		return nextDayAndAtMatch(start, w.On, w.At)
	}

	var dur time.Duration
	n := time.Duration(w.Every.n)

	switch w.Every.t {
	case tMillisecond:
		dur = n * time.Millisecond
	case tSecond:
		dur = n * time.Second
	case tMinute:
		dur = n * time.Minute
	case tHour:
		dur = n * time.Hour
		dur += nextAtMatch(start, w.At)
	case tDay:
		dur = n * 24 * time.Hour
		dur += nextAtMatch(start, w.At)
	case tWeek:
		weekdayDiff := 0

		if w.On != NoDay {
			weekdayDiff = int(math.Mod(float64(7+w.On-1-int(start.Weekday())), 7))
		}

		dur = (n*7 + time.Duration(weekdayDiff)) * 24 * time.Hour
		dur += nextAtMatch(start, w.At)
	default:
		return time.Duration(0)
	}

	return dur
}

func nextDayAndAtMatch(start time.Time, day int, at string) time.Duration {
	dur := nextAtMatch(start, at)

	if day != NoDay {
		nextDay := start.Add(nextDayMatch(start, day))
		dur = nextAtMatch(nextDay, at)
	}

	return dur
}

func nextAtMatch(start time.Time, at string) (d time.Duration) {
	re := regexp.MustCompile("([\\d|\\*]{2}):([\\d|\\*]\\d)")
	matches := re.FindAllStringSubmatch(at, -1)

	if len(matches) < 1 {
		return
	}

	hour, minute := matches[0][1], matches[0][2]
	var diff time.Duration

	if hour != "**" {
		diff += time.Hour * time.Duration(mod(hour, start.Hour(), 24))
	}

	if minute[0] == '*' {
		l := math.Mod(float64(start.Minute()), 10)
		d := mod(fmt.Sprintf("%c", minute[1]), int(l), 10)
		diff += time.Minute * time.Duration(d)
	} else {
		diff += time.Minute * time.Duration(mod(minute, start.Minute(), 60))
	}

	return diff
}

func nextDayMatch(start time.Time, day int) (d time.Duration) {
	diff := math.Mod(float64(7+(day-1)-int(start.Weekday())), 7)

	return time.Duration(diff) * 24 * time.Hour
}

func mod(val string, n, m int) float64 {
	value, _ := strconv.ParseInt(val, 10, 64)

	return math.Mod(float64(m+int(value)-n), float64(m))
}
