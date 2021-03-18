package main

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

type Event struct {
	ClientTime  LocalDateTime  `json:"client_time"`
	DeviceId    string         `json:"device_id"`
	DeviceOs    string         `json:"device_os"`
	Session     string         `json:"session"`
	Sequence    int            `json:"sequence"`
	Event       string         `json:"event"`
	ParamInt    *int           `json:"param_int"`
	ParamString *string        `json:"param_str"`
	Ip          *string        `json:"ip"`
	ServerTime  *LocalDateTime `json:"server_time"`
}

func (e Event) GetIp() net.IP {
	var ip net.IP
	if e.Ip == nil {
		ip = nil
	} else {
		ip = net.ParseIP(*e.Ip)
	}
	return ip
}

/*

{
    "client_time":"2020-12-01 23:59:00",
    "device_id":"0287D9AA-4ADF-4B37-A60F-3E9E645C821E",
    "device_os":"iOS 13.5.1",
    "session":"ybuRi8mAUypxjbxQ",
    "sequence":1,
    "event":"app_start",
    "param_int":0,
    "param_str":"some text"
}



*/

func main() {
	connect := connect()
	_ = connect

	var eventChan = make(chan Event, 100)

	http.HandleFunc("/api/event", func(w http.ResponseWriter, r *http.Request) {
		eventHandler(w, r, eventChan)
	})

	go eventProcessor(connect, eventChan)

	log.Fatal(http.ListenAndServe(":81", nil))
}

func connect() *sql.DB {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?database=sayGames")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		log.Fatal(err)
	}

	_, err = connect.Exec(`
create table sayGames.events
(
    client_time DateTime,
    device_id   UUID,
    device_os   String,
    session     String,
    sequence    Int32,
    event       String,
    param_int   Nullable(Int32),
    param_str   Nullable(String),
    ip          IPv4,
    server_time DateTime
)
    engine = MergeTree;`)
	return connect
}

func eventProcessor(connect *sql.DB, eventChan <-chan Event) {
	timer := time.NewTicker(1 * time.Second)
	const bufLen = 10
	var events = make([]Event, 0, bufLen)
	for {

		select {
		case event := <-eventChan:
			events = append(events, event)
		case <-timer.C:
			log.Printf("Sending %d events", len(events))
			if len(events) > 0 {
				go sendEvents(events, connect)
				events = make([]Event, 0, bufLen)
			}
		}
	}

}

func sendEvents(events []Event, connect *sql.DB) {
	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare(`INSERT INTO events (client_time, device_id, device_os, session, sequence, event, param_int, param_str, ip, server_time) 
                                                 VALUES (?,?,?,?,?,?,?,?,?,?)`)
	)
	defer stmt.Close()

	for _, e := range events {

		if _, err := stmt.Exec(
			e.ClientTime.Time,
			e.DeviceId,
			e.DeviceOs,
			e.Session,
			e.Sequence,
			e.Event,
			e.ParamInt,
			e.ParamString,
			e.GetIp(),
			e.ServerTime.Time,
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}

func eventHandler(w http.ResponseWriter, r *http.Request, eventChan chan Event) {
	defer r.Body.Close()
	e, err := decodeEvent(r.Body)
	if err != nil {
		log.Printf(err.Error())

		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	enrich(e, r)

	eventChan <- *e
}

func decodeEvent(body io.Reader) (*Event, error) {
	var e Event
	err := json.NewDecoder(body).Decode(&e)
	return &e, err
}

func enrich(e *Event, r *http.Request) {
	e.ServerTime = &LocalDateTime{time.Now()}
	e.Ip = &strings.SplitN(r.RemoteAddr, ":", 2)[0]
}
