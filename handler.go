package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type HandlerAction string

const (
	StatusAction HandlerAction = "status"
	OrderAction  HandlerAction = "order"
	SearchAction HandlerAction = "search"
)

type OrderContext struct {
	Hash   string        `json:"hash"`
	Action HandlerAction `json:"action"`
}

type OrderEvent struct {
	*Event
	Data *OrderContext `json:"data"`
}

type OrderHandler struct {
	Gateway   *OrdersGateway
	StatsD    *StatsD
	Hub       *Hub
	callbacks map[HandlerAction]HandlerCallback
}

func NewOrderHandler(app *Application) *OrderHandler {
	return &OrderHandler{
		Gateway:   NewOrdersGateway(app.DB),
		StatsD:    app.StatsD,
		Hub:       app.Hub,
		callbacks: make(map[HandlerAction]HandlerCallback),
	}
}

func GetAction(str string) HandlerAction {
	switch str {
	case "status":
		return StatusAction
	case "order":
		return OrderAction
	case "search":
		return SearchAction
	}

	panic("Wrong action: '" + str + "'")
}

func (handler *OrderHandler) RegisterCallback(action HandlerAction, callback HandlerCallback) {
	handler.callbacks[action] = callback
}

func (handler OrderHandler) DecodeEvent(message []byte) Eventizer {
	event := &OrderEvent{}
	err := json.Unmarshal(message, event)

	if err != nil {
		return nil
	}

	return event
}

func (handler OrderHandler) Process(event Eventizer) HandlerResult {
	myEvent := event.(*OrderEvent)
	result := NewHandlerResult()
	result.IsAcked = true

	order := handler.Gateway.FindByHash(myEvent.Data.Hash)
	success := false
	handler.StatsD.Incr("orders.count", 1)
	byOperant := ""

	if order == nil {
		log.Printf("[%v] (!) Could not found order with hash '%s'", time.Now(), myEvent.Data.Hash)
		result.IsAcked = false
	} else {
		message := order.Data
		handler.StatsD.Incr(byOperant+".count", 1)

		success = handler.RunCallback(myEvent.Data.Action, order)
		byOperant = "offsers.byOperant." + fmt.Sprintf("%d", message.Operant)
	}

	result.ProcessedAt = time.Now()
	st := result.ProcessedAt.UnixNano() / int64(time.Millisecond)
	result.Duration = int(result.EmittedAt.UnixNano()/int64(time.Millisecond) - st)

	if success {
		handler.StatsD.Incr(byOperant+".success", 1)

		if byOperant != "" {
			handler.StatsD.Timing(byOperant, result.Duration)
			handler.Gateway.Update(order)
		}
	} else {
		if byOperant != "" {
			handler.StatsD.Incr(byOperant+".failure", 1)
		}
	}

	if order != nil {
		handler.Hub.Q <- order
	}

	return result
}

func (handler *OrderHandler) RunCallback(action HandlerAction, order *Order) bool {
	if callback, ok := handler.callbacks[action]; ok {
		return callback.Run(order)
	}

	return false
}
