from(bucket:"kafka_events") |> range(start: -1h) |> group(columns: ["_measurement", "_field", "resource_name", "device_name"]) |> limit(n: 1)
