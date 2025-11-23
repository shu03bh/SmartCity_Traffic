--Vehicle Count By hour
SELECT date_trunc('hour', window_start) AS hour_bucket,
       SUM(vehicle_count_total) AS vehicles
FROM traffic_alerts
GROUP BY date_trunc('hour', window_start)
ORDER BY hour_bucket;