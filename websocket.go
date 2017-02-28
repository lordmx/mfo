package main

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

const (
	maxMessageSize = 512
)

type Connection struct {
	ws   *websocket.Conn
	send chan []byte
}

type Client struct {
	hash       string
	connection *Connection
}

type WebsocketMessage struct {
	hash    string
	payload []byte
}

type WebSocketHandler struct {
	hub      *Hub
	upgrader *websocket.Upgrader
}

func NewWebsockerHandler(app *Application) *WebSocketHandler {
	return &WebSocketHandler{
		hub:      app.Hub,
		upgrader: app.Upgrader,
	}
}

func (connection *Connection) write(mt int, payload []byte) error {
	connection.ws.SetWriteDeadline(time.Now().Add(writeWait))
	return connection.ws.WriteMessage(mt, payload)
}

func (connection *Connection) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		connection.ws.Close()
	}()
	for {
		select {
		case message, ok := <-connection.send:
			if !ok {
				connection.write(websocket.CloseMessage, []byte{})
				return
			}
			if err := connection.write(websocket.TextMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			if err := connection.write(websocket.PingMessage, []byte{}); err != nil {
				return
			}
		}
	}
}

func (handler *WebSocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	if hash, ok := vars["hash"]; ok {
		ws, err := handler.upgrader.Upgrade(w, r, nil)

		if err != nil {
			log.Printf("[%v] (!) %s", time.Now(), err.Error())
			return
		}

		connection := &Connection{ws, make(chan []byte, maxMessageSize)}
		handler.hub.register <- &Client{hash, connection}

		go connection.writePump()

		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}
