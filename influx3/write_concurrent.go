package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb3-go/influxdb3"

	
)

func main() {
	// InfluxDB 3.x connection settings
	url := "http://localhost:8086"
	database := "example_db"
	table := "events"
	token := "supersecret" // Use admin password as token for local dev

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Database: database,
		Token:    token,
	})
	if err != nil {
		log.Fatalf("Failed to create InfluxDB 3 client: %v", err)
	}
	defer client.Close()

	nWorkers := 10
	eventsPerWorker := 1000
	var wg sync.WaitGroup

	cameraIDs := []string{"cam_001", "cam_002", "cam_003", "cam_004", "cam_005"}
	eventTypes := []string{"motion", "object_detected", "face_recognized", "intrusion"}
	rand.Seed(time.Now().UnixNano())

	for w := 0; w < nWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var lines []string
			for i := 0; i < eventsPerWorker; i++ {
				eventType := eventTypes[rand.Intn(len(eventTypes))]
				cameraID := cameraIDs[rand.Intn(len(cameraIDs))]
				ts := time.Now().UnixNano()
				confidence := 0.5 + rand.Float64()*0.5
				details := fmt.Sprintf("Event %d: %s", i+1, eventType)
				line := fmt.Sprintf("%s,camera_id=%s event_type=\"%s\",confidence=%f,details=\"%s\" %d", table, cameraID, eventType, confidence, details, ts)
				lines = append(lines, line)
			}
			// Write batch
			body := strings.Join(lines, "\n")
			_, err := client.Write(context.Background(), influxdb3.WriteParams{
				Database:  database,
				Table:     table,
				Precision: "ns",
				Body:      strings.NewReader(body),
			})
			if err != nil {
				log.Printf("Worker %d failed to write: %v", workerID, err)
			} else {
				log.Printf("Worker %d wrote %d events", workerID, eventsPerWorker)
			}
		}(w)
		// Optional: stagger workers
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()
	log.Println("All workers finished.")
}
