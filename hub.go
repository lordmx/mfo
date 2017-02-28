package main

import (
	"log"
	"time"
)

const (
	writeWait  = 3 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	ttl        = 300
)

type Hub struct {
	connections map[string]*Connection
	register    chan *Client
	unregister  chan *Client
	send        chan *WebsocketMessage
	Q           chan *Order
	result      map[string]*Order
	expires     map[int][]string
}

func (h *Hub) Run() {
	ticker := time.NewTicker(ttl * time.Second)

	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case c := <-h.register:
			log.Printf("[%v] Registered: %s\n", time.Now(), c.hash)
			h.connections[c.hash] = c.connection

			if _, ok := h.result[c.hash]; ok {
				h.send <- &WebsocketMessage{c.hash, h.result[c.hash].Bytes()}
			}

		case c := <-h.unregister:
			log.Printf("[%v] Unregistered: %s\n", time.Now(), c.hash)

			if _, ok := h.connections[c.hash]; ok {
				delete(h.connections, c.hash)
				close(c.connection.send)
			}

		case order := <-h.Q:
			ts := int(time.Now().Unix())
			hash := order.Hash
			h.result[hash] = order
			h.expires[ts] = append(h.expires[ts], hash)

			if _, ok := h.connections[hash]; ok {
				h.send <- &WebsocketMessage{hash, order.Bytes()}
			}

		case <-ticker.C:
			ts := int(time.Now().Unix())

			for i := ts - 2*ttl; i < ts-ttl; i++ {
				if _, ok := h.expires[i]; !ok {
					continue
				}

				for _, hash := range h.expires[i] {
					delete(h.result, hash)
				}

				delete(h.expires, i)
			}

		case m := <-h.send:
			if _, ok := h.connections[m.hash]; ok {
				select {
				case h.connections[m.hash].send <- m.payload:
					if _, ok := h.result[m.hash]; ok {
						delete(h.result, m.hash)
					}

				default:
					close(h.connections[m.hash].send)
					delete(h.connections, m.hash)
				}
			}
		}
	}
}
