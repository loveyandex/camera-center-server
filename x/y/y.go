package main

import (
    "context"
    "fmt"
    influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

func main() {
    client := influxdb2.NewClient("http://localhost:8086", "my-super-secret-token")
    defer client.Close()
    
    // Call the function to query all events
    queryAllEvents(client)
}

func queryAllEvents(client influxdb2.Client) {
    queryAPI := client.QueryAPI("my_org")

    // Query to get all individual points from events measurement
    query := `
        from(bucket: "camera_events")
        |> range(start: 0)
        |> filter(fn: (r) => r._measurement == "events")
        |> sort(columns: ["_time"])
    `
    
    result, err := queryAPI.Query(context.Background(), query)
    if err != nil {
        panic(err)
    }

    fmt.Println("All Events Data:")
    fmt.Println("================")
    
    pointCount := 0
    for result.Next() {
        record := result.Record()
        pointCount++
        
        fmt.Printf("Point #%d:\n", pointCount)
        fmt.Printf("  Time: %v\n", record.Time())
        fmt.Printf("  Measurement: %s\n", record.Measurement())
        fmt.Printf("  Field: %s\n", record.Field())
        fmt.Printf("  Value: %v (Type: %T)\n", record.Value(), record.Value())
        
        // Print all tags
        fmt.Printf("  Tags:\n")
        for tagKey, tagValue := range record.Values() {
            // Skip system fields that start with underscore
            if tagKey[0] != '_' && tagKey != "result" && tagKey != "table" {
                fmt.Printf("    %s = %v\n", tagKey, tagValue)
            }
        }
        
        // Print all system fields for reference
        fmt.Printf("  System Info:\n")
        fmt.Printf("    _start: %v\n", record.ValueByKey("_start"))
        fmt.Printf("    _stop: %v\n", record.ValueByKey("_stop"))
        fmt.Printf("    table: %v\n", record.ValueByKey("table"))
        
        fmt.Println("  ---")
    }
    
    if result.Err() != nil {
        fmt.Printf("Query error: %v\n", result.Err())
    }
    
    fmt.Printf("\nTotal points retrieved: %d\n", pointCount)
}

// Alternative function to get events with specific time range
func queryEventsWithTimeRange(client influxdb2.Client, startTime, stopTime string) {
    queryAPI := client.QueryAPI("my_org")

    query := fmt.Sprintf(`
        from(bucket: "camera_events")
        |> range(start: %s, stop: %s)
        |> filter(fn: (r) => r._measurement == "events")
        |> sort(columns: ["_time"])
    `, startTime, stopTime)
    
    result, err := queryAPI.Query(context.Background(), query)
    if err != nil {
        panic(err)
    }

    fmt.Printf("Events from %s to %s:\n", startTime, stopTime)
    fmt.Println("============================")
    
    for result.Next() {
        record := result.Record()
        
        fmt.Printf("Time: %v | Field: %s | Value: %v", 
                   record.Time(), record.Field(), record.Value())
        
        // Print tags in a compact format
        tags := make([]string, 0)
        for tagKey, tagValue := range record.Values() {
            if tagKey[0] != '_' && tagKey != "result" && tagKey != "table" {
                tags = append(tags, fmt.Sprintf("%s=%v", tagKey, tagValue))
            }
        }
        if len(tags) > 0 {
            fmt.Printf(" | Tags: %v", tags)
        }
        fmt.Println()
    }
    
    if result.Err() != nil {
        fmt.Printf("Query error: %v\n", result.Err())
    }
}

// Function to get events with filtering by specific tag
func queryEventsByTag(client influxdb2.Client, tagKey, tagValue string) {
    queryAPI := client.QueryAPI("my_org")

    query := fmt.Sprintf(`
        from(bucket: "camera_events")
        |> range(start: 0)
        |> filter(fn: (r) => r._measurement == "events")
        |> filter(fn: (r) => r.%s == "%s")
        |> sort(columns: ["_time"])
    `, tagKey, tagValue)
    
    result, err := queryAPI.Query(context.Background(), query)
    if err != nil {
        panic(err)
    }

    fmt.Printf("Events filtered by %s = %s:\n", tagKey, tagValue)
    fmt.Println("===============================")
    
    for result.Next() {
        record := result.Record()
        fmt.Printf("Time: %v | Field: %s | Value: %v\n", 
                   record.Time(), record.Field(), record.Value())
    }
    
    if result.Err() != nil {
        fmt.Printf("Query error: %v\n", result.Err())
    }
}