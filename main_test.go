package main

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

func TestNonStopWriteRandomEvents(t *testing.T) {
	// Initialize InfluxDB client
	client := influxdb2.NewClient("http://localhost:8086", "my-super-secret-token")
	defer client.Close()

	// Get write API
	writeAPI := client.WriteAPIBlocking("my_org", "camera_events")

	// Define camera IDs
	cameraIDs := []string{"cam_001", "cam_002", "cam_003", "cam_004", "cam_005"}

	// Define possible event types
	eventTypes := []string{"motion", "object_detected", "face_recognized", "intrusion"}

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Counter for events
	eventCount := 0

	// Run for a limited number of iterations to avoid infinite loop in test
	// Adjust maxEvents for longer/shorter tests (e.g., 100 events ~ 10 seconds)
	const maxEvents = 100

	for i := 0; i < maxEvents; i++ {
		// Generate random event
		event := Event{
			CameraID:   cameraIDs[rand.Intn(len(cameraIDs))], // Random camera ID
			AIEngineID: fmt.Sprintf("engine_%03d", rand.Intn(3)+1),
			EventType:  eventTypes[rand.Intn(len(eventTypes))], // Random event type
			Confidence: 0.5 + rand.Float64()*0.5,               // Random confidence between 0.5 and 1.0
			Details:    fmt.Sprintf("Event %d: %s", eventCount+1, eventTypes[rand.Intn(len(eventTypes))]),
			Timestamp:  time.Now().Unix(),
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
			time.Unix(event.Timestamp, 0),
		)

		// Write to InfluxDB
		err := writeAPI.WritePoint(context.Background(), p)
		if err != nil {
			t.Fatalf("Failed to write event %d to InfluxDB: %v", eventCount+1, err)
		}

		eventCount++
		t.Logf("Wrote event %d: camera_id=%s, event_type=%s", eventCount, event.CameraID, event.EventType)

		// Sleep for 0.1 seconds
			time.Sleep(100 * time.Microsecond)
	}

	t.Logf("Successfully wrote %d random events to InfluxDB", eventCount)
}