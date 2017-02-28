package main

import (
	"fmt"
	"time"

	std "github.com/quipo/statsd"
)

type StatsD struct {
	host     string
	port     int
	prefix   string
	interval int
	provider *std.StatsdBuffer
}

func NewStatsD(host string, port int, prefix string, connected ...bool) (*StatsD, error) {
	statsd := &StatsD{
		host:     host,
		port:     port,
		prefix:   prefix,
		interval: 10,
	}

	if len(connected) > 0 && !connected[0] {
		return statsd, nil
	}

	provider, err := statsd.redial()

	if err != nil {
		return nil, err
	}

	statsd.provider = provider

	return statsd, nil
}

func (statsd *StatsD) Close() error {
	if statsd.provider == nil {
		return nil
	}

	return statsd.provider.Close()
}

func (statsd *StatsD) Timing(key string, delta int) bool {
	if statsd.provider == nil {
		return true
	}

	err := statsd.provider.Timing(key, int64(delta))

	if err != nil {
		provider, err := statsd.redial()

		if err != nil {
			return false
		}

		statsd.provider = provider

		return statsd.Timing(key, delta)
	}

	return true
}

func (statsd *StatsD) Incr(key string, value int) bool {
	if statsd.provider == nil {
		return true
	}

	err := statsd.provider.Incr(key, int64(value))

	if err != nil {
		provider, err := statsd.redial()

		if err != nil {
			return false
		}

		statsd.provider = provider

		return statsd.Incr(key, value)
	}

	return true
}

func (statsd *StatsD) Decr(key string, value int) bool {
	if statsd.provider == nil {
		return true
	}

	err := statsd.provider.Decr(key, int64(value))

	if err != nil {
		provider, err := statsd.redial()

		if err != nil {
			return false
		}

		statsd.provider = provider

		return statsd.Decr(key, value)
	}

	return true
}

func (statsd *StatsD) Gauge(key string, value int) bool {
	if statsd.provider == nil {
		return true
	}

	err := statsd.provider.Gauge(key, int64(value))

	if err != nil {
		provider, err := statsd.redial()

		if err != nil {
			return false
		}

		statsd.provider = provider

		return statsd.Gauge(key, value)
	}

	return true
}

func (statsd *StatsD) SetInternal(interval int) error {
	if interval < 1 {
		interval = 10
	}

	statsd.interval = interval
	provider, err := statsd.redial()

	if err != nil {
		return err
	}

	statsd.provider = provider

	return nil
}

func (statsd *StatsD) redial() (*std.StatsdBuffer, error) {

	host := fmt.Sprintf("%s:%d", statsd.host, statsd.port)
	client := std.NewStatsdClient(host, statsd.prefix)
	err := client.CreateSocket()

	if err != nil {
		return nil, err
	}

	realInterval := time.Second * time.Duration(statsd.interval)

	return std.NewStatsdBuffer(realInterval, client), nil
}
