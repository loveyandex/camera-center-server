package main

import (
    "context"
    "fmt"
    influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

func main() {
    client := influxdb2.NewClient("http://localhost:8086", "my-super-secret-token")
    defer client.Close()
    queryAPI := client.QueryAPI("my_org")

    query := `
        from(bucket: "camera_events")
        |> range(start: 0)
        |> filter(fn: (r) => r._measurement == "events")
        |> group()
        |> count()
    `
    result, err := queryAPI.Query(context.Background(), query)
    if err != nil {
        panic(err)
    }
    for result.Next() {
        fmt.Printf("Field: %s, Count: %v\n", result.Record().Field(), result.Record().Value())
    }
    if result.Err() != nil {
        fmt.Printf("Query error: %v\n", result.Err())
    }
}