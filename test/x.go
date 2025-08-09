package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

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

func writeEventsConcurrently(totalEvents, workers int) {
	// Initialize InfluxDB client
	token := "H6M3GMl-nWpWbPIFOoV3CRxG6m_sxGIHnjghJOhTigX_WmBWhPP9ufAuMpCm4hoMhu2J7EesTXQwrEhHfYfDmQ=="
	client := influxdb2.NewClient("http://localhost:8086", token)
	defer client.Close()

	// Get write API
	writeAPI := client.WriteAPIBlocking("my_org", "camera_events")

	// Define camera IDs
	cameraIDs := []string{"cam_001", "cam_002", "cam_003", "cam_004", "cam_005"}

	// Define possible event types
	eventTypes := []string{"motion", "object_detected", "face_recognized", "intrusion"}

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Channel for errors
	errChan := make(chan error, totalEvents)
	var wg sync.WaitGroup

	// Calculate events per worker
	eventsPerWorker := totalEvents / workers
	if eventsPerWorker == 0 {
		eventsPerWorker = 1
	}

	// Counter for events
	eventCount := 0
	countMutex := sync.Mutex{}

	// Start workers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < eventsPerWorker; i++ {
				// Generate random event
				countMutex.Lock()
				eventCount++
				currentCount := eventCount
				countMutex.Unlock()

				event := Event{
					CameraID:   cameraIDs[rand.Intn(len(cameraIDs))], // Random camera ID
					AIEngineID: fmt.Sprintf("engine_%03d", rand.Intn(3)+1),
					EventType:  eventTypes[rand.Intn(len(eventTypes))], // Random event type
					Confidence: 0.5 + rand.Float64()*0.5,               // Random confidence between 0.5 and 1.0
					Details:    fmt.Sprintf("Event %d: %s", currentCount, eventTypes[rand.Intn(len(eventTypes))]),
					Timestamp:  time.Now().Unix(),
				}
				fmt.Printf("event: %v\n", event)

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
					errChan <- fmt.Errorf("worker %d failed to write event %d: %v", workerID, currentCount, err)
					return
				}

				log.Printf("Worker %d wrote event %d: camera_id=%s, event_type=%s", workerID, currentCount, event.CameraID, event.EventType)
			}
		}(w)
		// Sleep 0.1s between starting workers to stagger load
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for all workers to finish
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		log.Printf("Error: %v", err)
	}

	log.Printf("Successfully wrote %d random events to InfluxDB", eventCount)
}

func main() {
	const totalEvents = 1
	const workers = 1 // Adjust number of workers as needed
	writeEventsConcurrently(totalEvents, workers)
}