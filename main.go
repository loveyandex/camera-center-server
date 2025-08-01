// ...existing code...
package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

type Event struct {
	CameraID   string  `json:"camera_id" binding:"required"`
	AIEngineID string  `json:"ai_engine_id" binding:"required"`
	EventType  string  `json:"event_type" binding:"required"`
	Confidence float64 `json:"confidence" binding:"required"`
	Details    string  `json:"details"`
	Timestamp  int64   `json:"timestamp"`
}

func main() {
	// Initialize InfluxDB client
	client := influxdb2.NewClient("http://localhost:8086", "my-super-secret-token")
	defer client.Close()

	// Get write and query APIs
	writeAPI := client.WriteAPIBlocking("my_org", "camera_events")
	queryAPI := client.QueryAPI("my_org")

	// Initialize Gin router
	r := gin.Default()

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
			from(bucket: "camera_events")
			|> range(start: 0)
			|> filter(fn: (r) => r._measurement == "events" and r.camera_id == "%s")
			|> sort(columns: ["_time"], desc: false)
			|> drop(columns: ["_start", "_stop"])
			|> limit(n: %d, offset: %d)
		`, cameraID, limit, offset)

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

	// POST /events - Write event to InfluxDB
	r.POST("/events", func(c *gin.Context) {
		var event Event
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Use provided timestamp or current time
		timestamp := time.Now()
		if event.Timestamp != 0 {
			timestamp = time.Unix(event.Timestamp, 0)
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

		c.JSON(http.StatusCreated, gin.H{"message": "Event written successfully"})
	})

	// GET /events/latest/:camera_id - Get latest event for a camera
	r.GET("/events/latest/:camera_id", func(c *gin.Context) {
		cameraID := c.Param("camera_id")

		// Flux query to get the latest event
		query := fmt.Sprintf(`
			from(bucket: "camera_events")
			|> range(start: -1y)
			|> filter(fn: (r) => r._measurement == "events" and r.camera_id == "%s")
			|> last()
		`, cameraID)

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
