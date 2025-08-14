// main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

const (
	influxURL    = "http://localhost:8086"
	influxToken  = "my-super-secret-token"
	influxOrg    = "my_org"
	influxBucket = "camera_events"
)

// Event is the event payload stored and broadcasted.
type Event struct {
	CameraID   string  `json:"camera_id" binding:"required"`
	AIEngineID string  `json:"ai_engine_id" binding:"required"`
	EventType  string  `json:"event_type" binding:"required"`
	Confidence float64 `json:"confidence" binding:"required"`
	Details    string  `json:"details"`
	Timestamp  int64   `json:"timestamp"`
}

// EventMessage wraps an Event with a source tag for the WebSocket clients.
type EventMessage struct {
	Source string `json:"source"` // "post" or "watcher"
	Event  Event  `json:"event"`
}

/* ----------------------------- WebSocket hub ----------------------------- */

type client struct {
	hub  *hub
	conn *websocket.Conn
	send chan []byte
	subs map[string]struct{} // subscribed camera IDs; empty = all
}

type broadcast struct {
	cameraID string
	payload  []byte
	key      string // dedup key
}

type hub struct {
	clients    map[*client]bool
	register   chan *client
	unregister chan *client
	broadcast  chan broadcast

	mu   sync.Mutex
	seen map[string]time.Time // dedup cache: key -> first seen at
}

func newHub() *hub {
	return &hub{
		clients:    make(map[*client]bool),
		register:   make(chan *client),
		unregister: make(chan *client),
		// Large buffer to avoid blocking producers (POST handlers) under burst
		broadcast: make(chan broadcast, 10000),
		seen:      make(map[string]time.Time),
	}
}

func (h *hub) run() {
	// Periodically GC dedup cache (does not delay broadcasts)
	go func() {
		t := time.NewTicker(1 * time.Minute)
		defer t.Stop()
		for range t.C {
			now := time.Now()
			h.mu.Lock()
			for k, ts := range h.seen {
				if now.Sub(ts) > 2*time.Minute {
					delete(h.seen, k)
				}
			}
			h.mu.Unlock()
		}
	}()

	for {
		select {
		case c := <-h.register:
			h.clients[c] = true

		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
				_ = c.conn.Close()
			}

		case b := <-h.broadcast:
			// Deduplicate very recent duplicates across paths
			if b.key != "" {
				h.mu.Lock()
				if ts, ok := h.seen[b.key]; ok && time.Since(ts) < 30*time.Second {
					h.mu.Unlock()
					continue
				}
				h.seen[b.key] = time.Now()
				h.mu.Unlock()
			}

			// Fan-out to matching subscribers (non-blocking; drop slow clients)
			for c := range h.clients {
				if len(c.subs) == 0 {
					select {
					case c.send <- b.payload:
					default:
						close(c.send)
						delete(h.clients, c)
						_ = c.conn.Close()
					}
					continue
				}
				if _, ok := c.subs[b.cameraID]; ok {
					select {
					case c.send <- b.payload:
					default:
						close(c.send)
						delete(h.clients, c)
						_ = c.conn.Close()
					}
				}
			}
		}
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10 // ~54s
)

func serveWS(h *hub, c *gin.Context) {
	// Optional subscriptions: ?camera_id=camA,camB
	raw := c.Query("camera_id")
	subs := map[string]struct{}{}
	if strings.TrimSpace(raw) != "" {
		for _, id := range strings.Split(raw, ",") {
			id = strings.TrimSpace(id)
			if id != "" {
				subs[id] = struct{}{}
			}
		}
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("websocket upgrade error: %v", err)
		return
	}
	cl := &client{
		hub:  h,
		conn: conn,
		send: make(chan []byte, 1024),
		subs: subs,
	}
	h.register <- cl

	// Reader: just to keep connection alive
	go func() {
		defer func() { h.unregister <- cl }()
		_ = cl.conn.SetReadDeadline(time.Now().Add(pongWait))
		cl.conn.SetPongHandler(func(string) error {
			_ = cl.conn.SetReadDeadline(time.Now().Add(pongWait))
			return nil
		})
		for {
			if _, _, err := cl.conn.ReadMessage(); err != nil {
				break
			}
		}
	}()

	// Writer: writes immediately when messages arrive; ping keeps connection healthy
	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer func() {
			ticker.Stop()
			_ = cl.conn.Close()
		}()
		for {
			select {
			case msg, ok := <-cl.send:
				_ = cl.conn.SetWriteDeadline(time.Now().Add(writeWait))
				if !ok {
					_ = cl.conn.WriteMessage(websocket.CloseMessage, []byte{})
					return
				}
				if err := cl.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					return
				}
			case <-ticker.C:
				_ = cl.conn.SetWriteDeadline(time.Now().Add(writeWait))
				if err := cl.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}()
}

/* ------------------------------ Influx watcher ------------------------------ */

func startInfluxWatcher(ctx context.Context, queryAPI api.QueryAPI, h *hub) {
	// Start slightly in the past to catch recent events on startup.
	last := time.Now().Add(-5 * time.Second).UTC()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			startRFC3339 := last.Add(1 * time.Nanosecond).Format(time.RFC3339Nano)
			query := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s)
  |> filter(fn: (r) => r._measurement == "events")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: false)
  |> keep(columns: ["_time","camera_id","ai_engine_id","event_type","confidence","details"])
`, influxBucket, startRFC3339)

			ctxQ, cancel := context.WithTimeout(ctx, 8*time.Second)
			result, err := queryAPI.Query(ctxQ, query)
			cancel()
			if err != nil {
				log.Printf("watcher query error: %v", err)
				continue
			}

			var newest time.Time
			seenAny := false
			for result.Next() {
				rec := result.Record()
				ts := rec.Time()
				if !seenAny || ts.After(newest) {
					newest = ts
				}

				evt := Event{
					CameraID:   fmt.Sprint(rec.ValueByKey("camera_id")),
					AIEngineID: fmt.Sprint(rec.ValueByKey("ai_engine_id")),
					EventType:  fmt.Sprint(rec.ValueByKey("event_type")),
					Confidence: asFloat(rec.ValueByKey("confidence")),
					Details:    fmt.Sprint(rec.ValueByKey("details")),
					Timestamp:  ts.Unix(),
				}
				msg := EventMessage{Source: "watcher", Event: evt}
				payload, _ := json.Marshal(msg)
				h.broadcast <- broadcast{
					cameraID: evt.CameraID,
					payload:  payload,
					key:      buildEventKey(evt),
				}
			}
			if result.Err() != nil {
				log.Printf("watcher result error: %v", result.Err())
			}
			if seenAny {
				last = newest
			}
		}
	}
}

func asFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int64:
		return float64(t)
	case int:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	default:
		return 0
	}
}

func buildEventKey(e Event) string {
	// Simple dedup key across POST and watcher within a short window.
	return fmt.Sprintf("%s|%s|%s|%d|%.5f|%s",
		e.CameraID, e.AIEngineID, e.EventType, e.Timestamp, e.Confidence, e.Details)
}

/* ---------------------------------- main ---------------------------------- */

func main() {
	// Initialize InfluxDB client
	client := influxdb2.NewClient(influxURL, influxToken)
	defer client.Close()

	// Get write and query APIs
	writeAPI := client.WriteAPIBlocking(influxOrg, influxBucket)
	queryAPI := client.QueryAPI(influxOrg)

	// Initialize WebSocket hub
	h := newHub()
	go h.run()

	// Start watcher (approach 1)
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	// go startInfluxWatcher(ctx, queryAPI, h)

	// Initialize Gin router
	r := gin.Default()

	// WebSocket endpoint: clients can optionally filter by camera_id: /ws?camera_id=cam1,cam2
	r.GET("/ws", func(c *gin.Context) {
		serveWS(h, c)
	})

	// GET /events/window/:camera_id?stop=<unix_seconds>
	// Returns events in the 10-minute window ending at 'stop' (or now if not provided),
	// sorted from latest to oldest.
	r.GET("/events/window/:camera_id", func(c *gin.Context) {

		bucket := influxBucket
		cameraID := c.Param("camera_id")
		stopParam := c.Query("stop") // unix seconds; optional

		// Determine stop time (default: now)
		stopTime := time.Now().UTC()
		if stopParam != "" {
			sec, err := strconv.ParseInt(stopParam, 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid stop (unix seconds)"})
				return
			}
			// Interpret as seconds
			stopTime = time.Unix(sec, 0).UTC()
		}
		startTime := stopTime.Add(-10 * time.Minute)

		// Flux expects RFC3339 timestamps
		startRFC3339 := startTime.Format(time.RFC3339Nano)
		stopRFC3339 := stopTime.Format(time.RFC3339Nano)

		// Escape quotes in camera_id
		safeCameraID := strings.ReplaceAll(cameraID, `"`, `\"`)

		query := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "events" and r.camera_id == "%s")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: true)
  |> keep(columns: ["_time","camera_id","ai_engine_id","event_type","confidence","details"])
`, bucket, startRFC3339, stopRFC3339, safeCameraID)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		result, err := queryAPI.Query(ctx, query)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		events := []gin.H{}
		for result.Next() {
			rec := result.Record()
			events = append(events, gin.H{
				"camera_id":    rec.ValueByKey("camera_id"),
				"ai_engine_id": rec.ValueByKey("ai_engine_id"),
				"event_type":   rec.ValueByKey("event_type"),
				"confidence":   rec.ValueByKey("confidence"),
				"details":      rec.ValueByKey("details"),
				"timestamp":    rec.Time().Unix(),
			})
		}
		if result.Err() != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": result.Err().Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"start":  startTime.Unix(),
			"stop":   stopTime.Unix(),
			"count":  len(events),
			"events": events,
			"order":  "desc", // latest -> oldest
			"window": "10m",
			"camera": cameraID,
		})
	})
	// GET /events/window/?stop=<unix_seconds>
	// Returns events from all cameras in the 10-minute window ending at 'stop' (or now if not provided),
	// sorted from latest to oldest.
	r.GET("/events/window", func(c *gin.Context) {

		bucket := influxBucket
		stopParam := c.Query("stop") // unix seconds; optional

		// Determine stop time (default: now)
		stopTime := time.Now().UTC()
		if stopParam != "" {
			sec, err := strconv.ParseInt(stopParam, 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid stop (unix seconds)"})
				return
			}
			stopTime = time.Unix(sec, 0).UTC()
		}
		startTime := stopTime.Add(-10 * time.Minute)

		startRFC3339 := startTime.Format(time.RFC3339Nano)
		stopRFC3339 := stopTime.Format(time.RFC3339Nano)

		query := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "events")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: true)
  |> keep(columns: ["_time","camera_id","ai_engine_id","event_type","confidence","details"])
`, bucket, startRFC3339, stopRFC3339)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		result, err := queryAPI.Query(ctx, query)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		events := []gin.H{}
		for result.Next() {
			rec := result.Record()
			events = append(events, gin.H{
				"camera_id":    rec.ValueByKey("camera_id"),
				"ai_engine_id": rec.ValueByKey("ai_engine_id"),
				"event_type":   rec.ValueByKey("event_type"),
				"confidence":   rec.ValueByKey("confidence"),
				"details":      rec.ValueByKey("details"),
				"timestamp":    rec.Time().Unix(),
			})
		}
		if result.Err() != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": result.Err().Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"start":  startTime.Unix(),
			"stop":   stopTime.Unix(),
			"count":  len(events),
			"events": events,
			"order":  "desc", // latest -> oldest
			"window": "10m",
			"camera": "all",
		})
	})

	// GET /events/:camera_id - Get all events for a camera (oldest to newest), with optional pagination
	r.GET("/events/:camera_id", func(c *gin.Context) {
		cameraID := c.Param("camera_id")

		// Pagination parameters (optional)
		limit := 100 // default limit
		offset := 0
		if l := c.Query("limit"); l != "" {
			fmt.Sscanf(l, "%d", &limit)
		}
		if o := c.Query("offset"); o != "" {
			fmt.Sscanf(o, "%d", &offset)
		}

		// Flux query to get all events for the camera, ordered by time ascending
		query := fmt.Sprintf(`
from(bucket: "%s")
|> range(start: 0)
|> filter(fn: (r) => r._measurement == "events" and r.camera_id == "%s")
|> sort(columns: ["_time"], desc: false)
|> drop(columns: ["_start", "_stop"])
|> limit(n: %d, offset: %d)
`, influxBucket, cameraID, limit, offset)

		result, err := queryAPI.Query(context.Background(), query)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Collect events
		events := []map[string]interface{}{}
		for result.Next() {
			record := result.Record()
			// Only add event if this is the first field for this timestamp (avoid duplicates)
			if record.Field() == "event_type" {
				event := map[string]interface{}{
					"camera_id":    record.ValueByKey("camera_id"),
					"ai_engine_id": record.ValueByKey("ai_engine_id"),
					"event_type":   record.ValueByKey("event_type"),
					"confidence":   record.ValueByKey("confidence"),
					"details":      record.ValueByKey("details"),
					"timestamp":    record.Time().Unix(),
				}
				events = append(events, event)
			}
		}

		if len(events) == 0 {
			c.JSON(http.StatusNotFound, gin.H{"error": "No events found for camera"})
			return
		}

		c.JSON(http.StatusOK, events)
	})

	// POST /events - Write event to InfluxDB (and broadcast via WebSocket) - approach 2
	r.POST("/events", func(c *gin.Context) {
		var event Event
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Use provided timestamp or current time
		timestamp := time.Now().UTC()
		if event.Timestamp != 0 {
			timestamp = time.Unix(event.Timestamp, 0).UTC()
		}

		// Create InfluxDB point
		p := influxdb2.NewPoint(
			"events",
			map[string]string{
				"camera_id":    event.CameraID,
				"ai_engine_id": event.AIEngineID,
			},
			map[string]interface{}{
				"event_type": event.EventType,
				"confidence": event.Confidence,
				"details":    event.Details,
			},
			timestamp,
		)

		// Write to InfluxDB
		if err := writeAPI.WritePoint(context.Background(), p); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Broadcast to WebSocket clients immediately (approach 2)
		out := Event{
			CameraID:   event.CameraID,
			AIEngineID: event.AIEngineID,
			EventType:  event.EventType,
			Confidence: event.Confidence,
			Details:    event.Details,
			Timestamp:  timestamp.Unix(),
		}
		msg := EventMessage{Source: "post", Event: out}
		payload, _ := json.Marshal(msg)
		h.broadcast <- broadcast{
			cameraID: out.CameraID,
			payload:  payload,
			key:      buildEventKey(out),
		}

		c.JSON(http.StatusCreated, gin.H{"message": "Event written successfully"})
	})

	// GET /events/latest/:camera_id - Get latest event for a camera
	r.GET("/events/latest/:camera_id", func(c *gin.Context) {
		cameraID := c.Param("camera_id")

		// Flux query to get the latest event
		query := fmt.Sprintf(`
from(bucket: "%s")
|> range(start: -1y)
|> filter(fn: (r) => r._measurement == "events" and r.camera_id == "%s")
|> last()
`, influxBucket, cameraID)

		result, err := queryAPI.Query(context.Background(), query)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		var event map[string]interface{}
		for result.Next() {
			record := result.Record()
			event = map[string]interface{}{
				"camera_id":    record.ValueByKey("camera_id"),
				"ai_engine_id": record.ValueByKey("ai_engine_id"),
				"event_type":   record.ValueByKey("event_type"),
				"confidence":   record.ValueByKey("confidence"),
				"details":      record.ValueByKey("details"),
				"timestamp":    record.Time().Unix(),
			}
		}

		if event == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "No events found for camera"})
			return
		}

		c.JSON(http.StatusOK, event)
	})

	// Start server
	if err := r.Run(":8080"); err != nil {
		fmt.Printf("Server failed: %v\n", err)
	}
}
