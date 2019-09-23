package rpc_server

import (
	"github.com/getmorebrasil/amqp-connection/connection"
	"github.com/streadway/amqp"
	"log"
)

type Connection *amqp.Connection
type Channel *amqp.Channel

var conn *amqp.Connection
var ch *amqp.Channel

func StartServer(connString string, fn func(Connection, Channel)) {
	conn, ch = amqp_connection.Connect(connString)
	fn(conn, ch)
	forever := make(chan bool)
	defer conn.Close()
	defer ch.Close()
	<-forever
}

func CreateQueue(name string, durable bool, del_when_unused bool, exclusive bool, noWait bool, arguments amqp.Table) (q amqp.Queue) {
	q = amqp_connection.CreateQueue(name, durable, del_when_unused, exclusive, noWait, arguments, ch)
	return q
}

func PublishQueue(exchange string, routingKey string, mandatory bool, immediate bool, options amqp.Publishing, ch *amqp.Channel) {
	amqp_connection.PublishQueue(exchange, routingKey, mandatory, immediate, options, ch)
}
func ConsumeQueue(fn func([]byte) []byte, q amqp.Queue) {
	msgs := amqp_connection.ConsumeQueue(
		q.Name, //queue
		// "",     //consumer
		false, //auto-ack
		false, //exclusive
		false, //no-local
		false, //no-wait
		nil,
		ch)

	go func() {
		for d := range msgs {
			response := fn(d.Body)
			amqp_connection.PublishQueue(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          response,
				},
				ch)
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting RPC requests from %s", q.Name)
	// <-forever
}
