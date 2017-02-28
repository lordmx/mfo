package main

type API struct {
	URL      string `yaml:"url"`
	Login    string `yaml:"login"`
	Password string `yaml:"password"`
	Hash     string `yaml:"hash"`
}

type Config struct {
	DB struct {
		Host     string `yaml:"host,omitempty"`
		Port     int    `yaml:"port,omitempty"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Database string `yaml:"database"`
		Encoding string `yaml:"names,omitempty"`
	} `yaml:"db"`

	AMQP struct {
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Host     string `yaml:"host"`
		VHost    string `yaml:"vhost,omitempty"`
	} `yaml:"amqp"`

	MessageBus struct {
		Exchange string `yaml:"exchange"`
	} `yaml:"mb"`

	Consumer struct {
		Host        string `yaml:"host"`
		Port        int    `yaml:"port"`
		EventName   string `yaml:"event_name"`
		Interval    int    `yaml:"interval"`
		LockFile    string `yaml:"lock"`
		ThreadCount int    `yaml:"threads"`
		Limit       int    `yaml:"limit"`
	} `yaml:"consumer"`

	API map[int]API `yaml:"api"`

	StatsD struct {
		Enabled  bool   `yaml:"enabled"`
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		Prefix   string `yaml:"prefix"`
		Interval int    `yaml:"interval"`
	} `yaml:"statsd"`

	Timezone string `yaml:"timezone,omitempty"`
}
