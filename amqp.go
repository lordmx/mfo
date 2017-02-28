package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

const (
	Direct  string = "direct"
	Fanout  string = "fanout"
	Topic   string = "topic"
	Headers string = "headers"
)

type Delivery <-chan amqp.Delivery
type Callback func(deliveries Delivery, done chan error)

type AmqpClient struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
}

type AmqpExchange struct {
	Client     *AmqpClient
	Name       string
	Type       string
	noWait     bool
	internal   bool
	autoDelete bool
	durable    bool
}

type AmqpConsumer struct {
	QueueName      string
	Client         *AmqpClient
	Tag            string
	exchangeToBind *AmqpExchange
	bindingKey     string
	done           chan error
	noAck          bool
	exclusive      bool
	noLocal        bool
	noWait         bool
	qos            *Qos
}

type AmqpQueue struct {
	Client     *AmqpClient
	Name       string
	durable    bool
	noWait     bool
	exclusive  bool
	autoDelete bool
}

type AmqpMessage struct {
	Body         []byte
	ContentType  string
	Timestamp    time.Time
	IsPersistent bool
	Mandatory    bool
	Immediate    bool
}

type AmqpPublisher struct {
	Client       *AmqpClient
	RoutingKey   string
	exchangeName string
}

type Qos struct {
	count  int
	size   int
	global bool
}

func NewQos(count, size int, global bool) *Qos {
	return &Qos{
		count:  count,
		size:   size,
		global: global,
	}
}

func NewAmqpError() chan *amqp.Error {
	return make(chan *amqp.Error)
}

func NewAmqpClient(dsn string) (*AmqpClient, error) {
	connection, err := amqp.Dial(dsn)

	if err != nil {
		return nil, err
	}

	channel, err := connection.Channel()

	if err != nil {
		return nil, err
	}

	client := &AmqpClient{
		Connection: connection,
		Channel:    channel,
	}

	return client, nil
}

func (client *AmqpClient) Close() {
	client.Connection.Close()
	client.Channel.Close()
}

func (client *AmqpClient) NewAmqpConsumer(queueName string, tagOptional ...string) *AmqpConsumer {
	tag := ""

	if len(tagOptional) > 0 {
		tag = tagOptional[0]
	}

	return &AmqpConsumer{
		Client:    client,
		QueueName: queueName,
		Tag:       tag,
		done:      make(chan error),
		qos:       &Qos{},
		noWait:    false,
		noLocal:   false,
		exclusive: false,
		noAck:     false,
	}
}

func (client *AmqpClient) NewAmqpExchange(exchangeName, exchangeType string) *AmqpExchange {
	return &AmqpExchange{
		Client:     client,
		Name:       exchangeName,
		Type:       exchangeType,
		durable:    false,
		noWait:     false,
		internal:   false,
		autoDelete: false,
	}
}

func (client *AmqpClient) NewAmqpQueue(queueName string) *AmqpQueue {
	return &AmqpQueue{
		Client:     client,
		Name:       queueName,
		noWait:     false,
		exclusive:  false,
		autoDelete: false,
		durable:    false,
	}
}

func (client *AmqpClient) NewPublisher(routingKey string, exchangeOptional ...string) *AmqpPublisher {
	exchangeName := ""

	if len(exchangeOptional) > 0 {
		exchangeName = exchangeOptional[0]
	}

	return &AmqpPublisher{
		Client:       client,
		RoutingKey:   routingKey,
		exchangeName: exchangeName,
	}
}

func (client *AmqpClient) Qos(qos Qos) error {
	return client.Channel.Qos(qos.count, qos.size, qos.global)
}

func (consumer *AmqpConsumer) NoAck(flag bool) *AmqpConsumer {
	consumer.noAck = flag
	return consumer
}

func (consumer *AmqpConsumer) Exclusive(flag bool) *AmqpConsumer {
	consumer.exclusive = flag
	return consumer
}

func (consumer *AmqpConsumer) NoLocal(flag bool) *AmqpConsumer {
	consumer.noLocal = flag
	return consumer
}

func (consumer *AmqpConsumer) NoWait(flag bool) *AmqpConsumer {
	consumer.noWait = flag
	return consumer
}

func (consumer *AmqpConsumer) Qos(qos *Qos) *AmqpConsumer {
	consumer.qos = qos
	return consumer
}

func (consumer *AmqpConsumer) BindToExchange(exchange *AmqpExchange, bindingKey ...string) *AmqpConsumer {
	consumer.exchangeToBind = exchange

	if len(bindingKey) > 0 {
		consumer.bindingKey = bindingKey[0]
	}

	return consumer
}

func (consumer *AmqpConsumer) Consume(callback Callback) error {
	var err error

	queue := consumer.Client.NewAmqpQueue(consumer.QueueName)
	queue = queue.
		AutoDelete(false).
		NoWait(false)

	if consumer.QueueName == "" {
		queue = queue.
			Durable(false).
			Exclusive(true)
	} else {
		queue = queue.
			Durable(true).
			Exclusive(false)
	}

	err = queue.Declare()

	if err != nil {
		return err
	}

	if consumer.QueueName == "" {
		consumer.QueueName = queue.Name
	}

	if consumer.exchangeToBind != nil {
		consumer.exchangeToBind.BindQueue(queue.Name, consumer.bindingKey)
	}

	if consumer.qos != nil {
		err = consumer.Client.Qos(*consumer.qos)
		if err != nil {
			return err
		}
	}

	deliveries, err := consumer.Client.Channel.Consume(
		consumer.QueueName,
		"",
		consumer.noAck,     // noAck
		consumer.exclusive, // exclusive
		consumer.noLocal,   // noLocal
		consumer.noWait,    // noWait
		nil,                // arguments
	)

	if err != nil {
		return err
	}

	forever := make(chan bool)

	go callback(deliveries, consumer.done)

	<-forever

	return nil
}

func (consumer *AmqpConsumer) Shutdown() error {
	if err := consumer.Client.Channel.Cancel(consumer.Tag, true); err != nil {
		log.Printf("[%v] (!) Consumer cancel failed: %s", time.Now(), err.Error())
		return err
	}

	if err := consumer.Client.Connection.Close(); err != nil {
		log.Printf("[%v] (!) AMQP connection close error: %s", time.Now(), err.Error())
		return err
	}

	defer log.Printf("[%v] AMQP shutdown OK", time.Now())

	return <-consumer.done
}

func (exchange *AmqpExchange) NoWait(flag bool) *AmqpExchange {
	exchange.noWait = flag
	return exchange
}

func (exchange *AmqpExchange) Internal(flag bool) *AmqpExchange {
	exchange.internal = flag
	return exchange
}

func (exchange *AmqpExchange) AutoDelete(flag bool) *AmqpExchange {
	exchange.autoDelete = flag
	return exchange
}

func (exchange *AmqpExchange) Durable(flag bool) *AmqpExchange {
	exchange.durable = flag
	return exchange
}

func (exchange *AmqpExchange) Declare() error {
	return exchange.Client.Channel.ExchangeDeclare(
		exchange.Name,
		exchange.Type,
		exchange.durable,    // durable
		exchange.autoDelete, // delete when complete
		exchange.internal,   // internal
		exchange.noWait,     // noWait
		nil,                 // arguments
	)
}

func (exchange *AmqpExchange) BindQueue(queueName string, bindingKey ...string) error {
	key := ""

	if len(bindingKey) > 0 {
		key = bindingKey[0]
	}

	return exchange.Client.Channel.QueueBind(
		queueName,
		key,
		exchange.Name,
		false,
		nil,
	)
}

func (queue *AmqpQueue) Durable(flag bool) *AmqpQueue {
	queue.durable = flag
	return queue
}

func (queue *AmqpQueue) NoWait(flag bool) *AmqpQueue {
	queue.noWait = flag
	return queue
}

func (queue *AmqpQueue) Exclusive(flag bool) *AmqpQueue {
	queue.exclusive = flag
	return queue
}

func (queue *AmqpQueue) AutoDelete(flag bool) *AmqpQueue {
	queue.autoDelete = flag
	return queue
}

func (queue *AmqpQueue) Declare() (err error) {
	q, err := queue.Client.Channel.QueueDeclare(
		queue.Name,
		queue.durable,    // durable
		queue.autoDelete, // delete when usused
		queue.exclusive,  // exclusive
		queue.noWait,     // noWait
		nil,              // arguments
	)

	if err != nil {
		return
	}

	if queue.Name == "" {
		queue.Name = q.Name
	}

	return
}

func (message *AmqpMessage) Persistent(flag bool) *AmqpMessage {
	message.IsPersistent = flag
	return message
}

func (message *AmqpMessage) SetContentType(typeName string) *AmqpMessage {
	message.ContentType = typeName
	return message
}

func (message *AmqpMessage) SetMandatory(flag bool) *AmqpMessage {
	message.Mandatory = flag
	return message
}

func (message *AmqpMessage) SetImmediate(flag bool) *AmqpMessage {
	message.Immediate = flag
	return message
}

func (publisher *AmqpPublisher) NewMessage(body []byte) *AmqpMessage {
	return &AmqpMessage{
		Body:         body,
		ContentType:  "text/plain",
		IsPersistent: false,
		Timestamp:    time.Now(),
		Mandatory:    false,
		Immediate:    false,
	}
}

func (publisher *AmqpPublisher) Publish(message *AmqpMessage) error {
	deliveryMode := amqp.Transient
	if message.IsPersistent {
		deliveryMode = amqp.Persistent
	}

	msg := amqp.Publishing{
		DeliveryMode: deliveryMode,
		Timestamp:    message.Timestamp,
		ContentType:  message.ContentType,
		Body:         message.Body,
	}

	return publisher.Client.Channel.Publish(
		publisher.exchangeName,
		publisher.RoutingKey,
		message.Mandatory, // mandatory
		message.Immediate, // immediate
		msg,               // message
	)
}
