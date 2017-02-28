package main

import (
	"log"
	"strconv"
	"time"
)

const (
	StatusError         = "error"
	StatusCreated       = "created"
	StatusPending       = "pending"
	StatusApproved      = "approved"
	StatusDisapproved   = "disapproved"
	StatusClientRefused = "client_refused"
	StatusForbidden     = "forbidden"
)

type handlerCallback struct {
	Services map[int]*Service
}

type HandlerCallback interface {
	Run(order *Order) bool
}

type StatusCallback struct {
	*handlerCallback
}

type OrderCallback struct {
	*handlerCallback
	search *SearchCallback
}

type SearchCallback struct {
	*handlerCallback
}

func newHandlerCallback(app *Application) *handlerCallback {
	return &handlerCallback{
		Services: app.Services,
	}
}

func DecodeStatus(status string) string {
	switch status {
	case "З":
		return StatusForbidden
	case "О":
		return StatusApproved
	case "Т":
		return StatusPending
	case "Н":
		return StatusDisapproved
	case "К":
		return StatusClientRefused

	}

	return StatusPending
}

func NewStatusCallback(app *Application) *StatusCallback {
	return &StatusCallback{
		handlerCallback: newHandlerCallback(app),
	}
}

func NewOrderCallback(app *Application, search *SearchCallback) *OrderCallback {
	return &OrderCallback{
		handlerCallback: newHandlerCallback(app),
		search:          search,
	}
}

func NewSearchCallback(app *Application) *SearchCallback {
	return &SearchCallback{
		handlerCallback: newHandlerCallback(app),
	}
}

func (callback *StatusCallback) Run(order *Order) bool {
	order.Data.Operant = 3

	if order.OrderId != nil {
		order.Data.Idorder = int32(*order.OrderId)
	}

	if order.OrderNum != nil {
		order.Data.OrderNum = *order.OrderNum
	}

	return callback.process(order)
}

func (callback *OrderCallback) Run(order *Order) bool {
	callback.search.Run(order)
	order.Data.Operant = 1

	if order.UserId != nil && *order.UserId > 0 {
		order.Data.Idusers = int32(*order.UserId)
		order.Data.Operant = 2
	}

	if order.OrderId == nil || *order.OrderId == 0 {
		result := callback.process(order)

		if result {
			order.Status = StatusPending
		}

		return result
	} else {
		if order.Status != "" {
			order.Status = DecodeStatus(order.Status)
		}
	}

	result := order.OrderId != nil && *order.OrderId > 0

	if !result {
		order.Status = StatusError
	}

	return result
}

func (callback *SearchCallback) Run(order *Order) bool {
	order.Data.Operant = 4

	return callback.process(order)
}

func (callback *handlerCallback) process(order *Order) bool {
	if _, ok := callback.Services[order.ClientId]; !ok {
		return false
	}

	request := &Request{Message: order.Data}
	response, err := callback.Services[order.ClientId].Process(request)

	if response.Errors != "" {
		order.ExternalNotice = &response.Errors
	}

	if err != nil {
		callback.processError(order, err)
		return false
	}

	callback.processResponse(order, response)

	return true
}

func (callback *handlerCallback) processError(order *Order, err error) {
	message := err.Error()
	order.ExternalNotice = &message
	order.Status = StatusError

	log.Printf("[%v] (!) %s", time.Now(), message)
}

func (callback *handlerCallback) processResponse(order *Order, response *Response) {
	if response.Idorder != "" && response.Idorder != "0" {
		orderId, _ := strconv.Atoi(response.Idorder)

		if orderId > 0 {
			order.OrderId = &orderId
		}
	}

	if response.Idusers != "" && response.Idusers != "0" {
		userId, _ := strconv.Atoi(response.Idusers)

		if userId > 0 {
			order.UserId = &userId
		}
	}

	switch order.Data.Operant {
	case 1, 2, 4:
		order.OrderNum = interfaceToStringPtr(response.OrderNumRaw)
		order.Percent = interfaceToFloatPtr(response.PercentRaw)
		order.MaxAmount = interfaceToFloatPtr(response.MaxAmountRaw)
		order.ProductName = interfaceToStringPtr(response.ProductNameRaw)
	case 3:
		order.Status = DecodeStatus(interfaceToString(response.StatusRaw))
		order.PayStatus = interfaceToString(response.PayStatusRaw)
		order.GivenAt = interfaceToTime(response.GivenAtRaw)
		order.NextPayAt = interfaceToTime(response.NextPayAt)
		order.FineDays = interfaceToIntPtr(response.FineDays)
		order.TotalDebt = interfaceToFloatPtr(response.TotalDebtRaw)
		order.PaidTotal = interfaceToFloatPtr(response.PaidTotalRaw)
		order.BodyDebt = interfaceToFloatPtr(response.BodyDebtRaw)
		order.PercentsDebt = interfaceToFloatPtr(response.PercentsDebtRaw)
		order.FineDebt = interfaceToFloatPtr(response.FineDebtRaw)

		if order.PayStatus == "" {
			order.PayStatus = "It is required to consider"
		}

		if order.Status == "" {
			order.Status = StatusPending
		}
	}
}
