package main

import (
	"encoding/json"
	"log"
	"time"
)

type MessageBus struct {
	AmqpClient  *AmqpClient
	exchange    *AmqpExchange
	boundEvents map[string][]EventHandler
	consumers   map[string]*AmqpConsumer
}

type Eventizer interface {
	SetEmittedAt(datetime time.Time)
	GetName() string
	GetId() string
	SetDeliveryTag(tag uint64)
	GetDeliveryTag() uint64
}

type HandlerResult struct {
	Id          string
	EmittedAt   time.Time
	ProcessedAt time.Time
	Duration    int
	Action      string
	IsAcked     bool
	Message     string
	Data        map[string]interface{}
}

type EventHandler interface {
	DecodeEvent(message []byte) Eventizer
	Process(event Eventizer) HandlerResult
}

type Event struct {
	Id          string                 `json:"id"`
	Name        string                 `json:"name"`
	Data        map[string]interface{} `json:"data"`
	EmittedAt   time.Time              `json:"emitted_at"`
	DeliveryTag uint64
}

func NewMessageBus(
	exchangeName string,
	amqpClient *AmqpClient,
) *MessageBus {
	exchange := amqpClient.NewAmqpExchange(exchangeName, "direct")

	exchange = exchange.
		Durable(true).
		AutoDelete(false).
		Internal(false).
		NoWait(false)

	err := exchange.Declare()

	if err != nil {
		log.Panic(err)
	}

	return &MessageBus{
		AmqpClient:  amqpClient,
		exchange:    exchange,
		consumers:   make(map[string]*AmqpConsumer),
		boundEvents: make(map[string][]EventHandler),
	}
}

func NewHandlerResult() HandlerResult {
	return HandlerResult{
		Data: make(map[string]interface{}),
	}
}

func (mb *MessageBus) Run(eventName string, handler EventHandler) {
	log.Printf("[%v] Message bus was started", time.Now())

	mb.Subscribe(eventName, handler)

	for eventName := range mb.consumers {
		mb.InitHandler(eventName)
	}
}

func (mb *MessageBus) Subscribe(eventName string, handler EventHandler) {
	if _, ok := mb.consumers[eventName]; !ok {
		tag := "consumer_" + GetUniqId()

		consumer := mb.AmqpClient.NewAmqpConsumer(eventName, tag)
		consumer = consumer.BindToExchange(mb.exchange, eventName)
		consumer.Qos(NewQos(1, 0, false))

		mb.consumers[eventName] = consumer
	}

	mb.boundEvents[eventName] = append(mb.boundEvents[eventName], handler)
}

func (mb *MessageBus) Unbound(eventName string) (handlers []EventHandler) {
	if _, ok := mb.boundEvents[eventName]; ok {
		handlers = mb.boundEvents[eventName]
		mb.AmqpClient.Channel.Cancel(mb.consumers[eventName].Tag, true)
	}

	delete(mb.boundEvents, eventName)
	delete(mb.consumers, eventName)

	return
}

func (mb *MessageBus) GetBounded(eventName string) []EventHandler {
	if handlers, ok := mb.boundEvents[eventName]; ok {
		return handlers
	}

	return []EventHandler{}
}

func (mb *MessageBus) Publish(event Eventizer, eventName string) error {
	event.SetEmittedAt(time.Now())
	message, _ := json.Marshal(event)

	publisher := mb.AmqpClient.NewPublisher(eventName, mb.exchange.Name)

	return publisher.Publish(publisher.NewMessage(message))
}

func (mb *MessageBus) NewEvent(name string) *Event {
	return &Event{
		Id:        GetUniqId(),
		Name:      name,
		Data:      make(map[string]interface{}),
		EmittedAt: time.Now(),
	}
}

func (mb *MessageBus) InitHandler(eventName string) {
	if consumer, ok := mb.consumers[eventName]; ok {
		consumer.Consume(func(deliveries Delivery, done chan error) {
			for d := range deliveries {
				tmp := &Event{}
				err := json.Unmarshal([]byte(d.Body), tmp)

				if err != nil {
					log.Printf("[%v] (!) JSON: %s", time.Now(), err.Error())
					mb.NackMessage(d.DeliveryTag)
					continue
				}

				if _, ok := mb.boundEvents[tmp.GetName()]; !ok {
					continue
				}

				if tmp.GetName() != eventName {
					continue
				}

				log.Printf("[%v] Event '%s': %v", time.Now(), eventName, string(d.Body))

				for _, handler := range mb.boundEvents[tmp.GetName()] {
					event := handler.DecodeEvent([]byte(d.Body))

					if event == nil {
						log.Printf("[%v] (!) Decode event error!", time.Now())
						mb.NackMessage(d.DeliveryTag)
						continue
					}

					result := NewHandlerResult()
					event.SetDeliveryTag(d.DeliveryTag)

					result = handler.Process(event)
					result.Action = tmp.GetName()

					if result.IsAcked {
						mb.AckMessage(d.DeliveryTag)
					} else {
						mb.NackMessage(d.DeliveryTag)
					}
				}
			}

			done <- nil
		})
	}
}

func (mb *MessageBus) AckMessage(tag uint64) {
	mb.AmqpClient.Channel.Ack(tag, false)
}

func (mb *MessageBus) NackMessage(tag uint64) {
	mb.AmqpClient.Channel.Nack(tag, false, false)
}

func (event *Event) SetDeliveryTag(tag uint64) {
	event.DeliveryTag = tag
}

func (event Event) GetDeliveryTag() uint64 {
	return event.DeliveryTag
}

func (event *Event) SetEmittedAt(datetime time.Time) {
	event.EmittedAt = datetime
}

func (event Event) GetName() string {
	return event.Name
}

func (event Event) GetId() string {
	return event.Id
}
