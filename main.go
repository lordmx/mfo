package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/rpc"
	"github.com/gorilla/rpc/json"
)

var (
	configFromFlag = flag.String("config", "config.yml", "Config path")
)

func init() {
	flag.Parse()
}

func main() {
	app, err := NewApplication(*configFromFlag)

	if err != nil {
		log.Printf("[%v] (!) %s", time.Now(), err.Error())
		os.Exit(1)
	}

	defer app.Close()

	handler := NewOrderHandler(app)
	searchCallback := NewSearchCallback(app)

	handler.RegisterCallback(StatusAction, NewStatusCallback(app))
	handler.RegisterCallback(SearchAction, searchCallback)
	handler.RegisterCallback(OrderAction, NewOrderCallback(app, searchCallback))

	NewOrderJob(app, handler)

	go app.Scheduler.Start()
	go app.MessageBus.Run(app.Config.Consumer.EventName, handler)
	go app.Hub.Run()

	router := mux.NewRouter().StrictSlash(true)

	jsonRPC := rpc.NewServer()

	jsonCodec := json.NewCodec()
	jsonRPC.RegisterCodec(jsonCodec, "application/json")
	jsonRPC.RegisterCodec(jsonCodec, "application/json; charset=UTF-8")

	jsonRPC.RegisterService(NewOrderService(app), "")

	router.Handle("/api", jsonRPC)
	router.Handle("/ws/{hash}", NewWebsockerHandler(app))

	host := fmt.Sprintf(
		"%s:%d",
		app.Config.Consumer.Host,
		app.Config.Consumer.Port,
	)

	log.Printf("[%v] Listening on '%s'", time.Now(), host)
	log.Fatal(http.ListenAndServe(host, router))
}
