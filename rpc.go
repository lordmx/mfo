package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type OrderArgs struct {
	Hash string
}

type OrderReply struct {
	Hash string
}

type OrderService struct {
	mb        *MessageBus
	eventName string
}

func NewOrderService(app *Application) *OrderService {
	return &OrderService{
		mb:        app.MessageBus,
		eventName: app.Config.Consumer.EventName,
	}
}

func (service *OrderService) Search(r *http.Request, args *OrderArgs, reply *OrderReply) error {
	log.Printf("[%v] OrderService.Search('%s')", time.Now(), args.Hash)

	return service.process("search", args, reply)
}

func (service *OrderService) Status(r *http.Request, args *OrderArgs, reply *OrderReply) error {
	log.Printf("[%v] OrderService.Status('%s')", time.Now(), args.Hash)

	return service.process("status", args, reply)
}

func (service *OrderService) Order(r *http.Request, args *OrderArgs, reply *OrderReply) error {
	log.Printf("[%v] OrderService.Order('%s')", time.Now(), args.Hash)

	return service.process("order", args, reply)
}

func (service *OrderService) process(action string, args *OrderArgs, reply *OrderReply) error {
	reply.Hash = args.Hash

	event := &OrderEvent{
		Event: service.mb.NewEvent(service.eventName),
		Data: &OrderContext{
			Hash:   args.Hash,
			Action: GetAction(action),
		},
	}

	_, err := json.Marshal(event)

	if err != nil {
		return err
	}

	err = service.mb.Publish(event, service.eventName)

	return err
}
