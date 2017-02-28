package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/go-fsnotify/fsnotify"
	"github.com/gorilla/websocket"
	"github.com/jmoiron/sqlx"
	"github.com/nightlyone/lockfile"
	"gopkg.in/yaml.v2"
)

type Application struct {
	Config     *Config
	Location   *time.Location
	DB         *sqlx.DB
	AMQP       *AmqpClient
	MessageBus *MessageBus
	StatsD     *StatsD
	Services   map[int]*Service
	Hub        *Hub
	Upgrader   *websocket.Upgrader
	Scheduler  *TaskScheduler
	LockFile   lockfile.Lockfile
	Ticker     *time.Ticker
	OnInit     func(app *Application)

	configPath string
}

func (app *Application) Close() {
	if app.DB != nil {
		app.DB.Close()
	}

	if app.AMQP != nil {
		app.AMQP.Close()
	}

	if app.StatsD != nil {
		app.StatsD.Close()
	}

	if app.Ticker != nil {
		app.Ticker.Stop()
	}
}

func NewApplication(configPath string) (app *Application, err error) {
	if _, err = os.Stat(configPath); os.IsNotExist(err) {
		return nil, errors.New("Could not found the config file " + configPath)
	}

	app = &Application{Services: make(map[int]*Service)}
	app.configPath = configPath

	app.Config, err = app.getConfig()

	if err != nil {
		return nil, err
	}

	err = app.init()

	if err != nil {
		return nil, err
	}

	app.OnInit = func(app *Application) {

	}

	return
}

func NewConfig(path string) (*Config, error) {
	c := &Config{}

	filename, err := filepath.Abs(path)

	if err != nil {
		return nil, err
	}

	raw, err := ioutil.ReadFile(filename)

	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(raw, &c)

	if err != nil {
		return nil, err
	}

	return c, nil
}

func (app *Application) init() error {
	var err error

	app.AMQP, err = app.getAMQP()

	if err != nil {
		return err
	}

	app.DB, err = app.getPgsql()

	if err != nil {
		return err
	}

	app.MessageBus = app.getMessageBus()

	app.Hub = app.getHub()

	app.Upgrader = app.getUpgrader()

	app.StatsD, err = app.getStatsD()

	if err != nil {
		return err
	}

	app.LockFile, err = app.getLockFile()

	if err != nil {
		return err
	}

	app.Services = app.getServices()
	app.Scheduler = &TaskScheduler{}

	timezone := "UTC"

	if app.Config.Timezone != "" {
		timezone = app.Config.Timezone
	}

	location, err := time.LoadLocation(timezone)

	if err != nil {
		return err
	}

	app.Location = location

	go app.watchConfig()
	app.watchConnections()

	log.Printf("[%v] Application was successfuly bootstrapped", time.Now())

	return nil
}

func (app *Application) getConfig() (*Config, error) {
	log.Printf("[%v] Application.getConfig()", time.Now())
	return NewConfig(app.configPath)
}

func (app *Application) getUpgrader() *websocket.Upgrader {
	return &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
}

func (app *Application) getLockFile() (lockfile.Lockfile, error) {
	return lockfile.New(app.Config.Consumer.LockFile)
}

func (app *Application) getHub() *Hub {
	return &Hub{
		register:    make(chan *Client),
		unregister:  make(chan *Client),
		connections: make(map[string]*Connection),
		send:        make(chan *WebsocketMessage),
		Q:           make(chan *Order),
		result:      make(map[string]*Order),
		expires:     make(map[int][]string),
	}
}

func (app *Application) getMessageBus() *MessageBus {
	log.Printf(
		"[%v] Application.getMessageBus('%s')",
		time.Now(),
		app.Config.MessageBus.Exchange,
	)
	return NewMessageBus(app.Config.MessageBus.Exchange, app.AMQP)
}

func (app *Application) getServices() map[int]*Service {
	result := make(map[int]*Service)

	for id, config := range app.Config.API {
		result[id] = NewService(
			config.URL,
			false,
			&BasicAuth{config.Login, config.Password},
			config.Hash,
		)
	}

	return result
}

func (app *Application) getStatsD() (*StatsD, error) {
	log.Printf(
		"[%v] Application.getStatsD('%s:%d/%s')",
		time.Now(),
		app.Config.StatsD.Host,
		app.Config.StatsD.Port,
		app.Config.StatsD.Prefix,
	)

	return NewStatsD(
		app.Config.StatsD.Host,
		app.Config.StatsD.Port,
		app.Config.StatsD.Prefix,
		app.Config.StatsD.Enabled,
	)
}

func (app *Application) getPgsql() (*sqlx.DB, error) {
	config := app.Config

	log.Printf(
		"[%v] Application.getPgsql('%s:%d/%s')",
		time.Now(),
		config.DB.Host,
		config.DB.Port,
		config.DB.Database,
	)

	db, err := NewPgsql(
		config.DB.Host,
		config.DB.Port,
		config.DB.User,
		config.DB.Password,
		config.DB.Database,
		config.DB.Encoding,
		config.Timezone,
	)

	if err != nil {
		return nil, err
	}

	return db, nil
}

func (app *Application) getAMQP() (*AmqpClient, error) {
	config := app.Config

	log.Printf(
		"[%v] Application.getAMQP('%s/%s')",
		time.Now(),
		config.AMQP.Host,
		config.AMQP.VHost,
	)

	amqpHost := fmt.Sprintf(
		"amqp://%s:%s@%s/%s",
		config.AMQP.User,
		config.AMQP.Password,
		config.AMQP.Host,
		config.AMQP.VHost,
	)

	amqpClient, err := NewAmqpClient(amqpHost)

	if err != nil {
		return nil, err
	}

	return amqpClient, err
}

func (app *Application) watchConnections() {
	app.Ticker = time.NewTicker(5 * time.Second)

	go func() {
		isConnected := true

		for {
			select {
			case <-app.Ticker.C:
				_, err := app.DB.Exec("SELECT 1")

				if err != nil {
					if isConnected {
						isConnected = false
					}

					log.Printf("[%v] Lost connection to DB. Reconnecting...", time.Now())
				}

				if err == nil && !isConnected {
					app.init()
					app.OnInit(app)

					isConnected = true
				}
			}
		}
	}()

	go func() {
		for err := range app.AMQP.Connection.NotifyClose(NewAmqpError()) {
			log.Printf("[%v] (!) AMQP: %s", time.Now(), err.Error())

			ticker := time.NewTicker(5 * time.Second)

			go func() {
				for {
					select {
					case <-ticker.C:
						_, e := app.getAMQP()

						if e == nil {
							log.Printf("[%v] AMQP reconnected.", time.Now())
							ticker.Stop()

							app.init()
							app.OnInit(app)

							return
						}
					}
				}
			}()
		}
	}()
}

func (app *Application) watchConfig() {
	watcher, err := fsnotify.NewWatcher()

	if err != nil {
		log.Fatal(err)
	}

	defer watcher.Close()

	done := make(chan bool)

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if event.Op&fsnotify.Write == fsnotify.Write && event.Name == app.configPath {
					log.Println("["+time.Now().Format(time.RFC3339)+"] modified config:", event.Name)
					app.Config, err = app.getConfig()

					if err != nil {
						log.Fatal(err)
					}

					app.init()
					app.OnInit(app)
				}
			case err := <-watcher.Errors:
				log.Fatal(err)
			}
		}
	}()

	err = watcher.Add(app.configPath)

	if err != nil {
		log.Fatal(err)
	}

	<-done
}
