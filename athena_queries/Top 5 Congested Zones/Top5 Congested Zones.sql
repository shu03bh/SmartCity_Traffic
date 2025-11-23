SELECT hour_of_day,
       zone,
       total_congestion
FROM (
    SELECT
        hour(window_start) AS hour_of_day,
        zone,
        SUM(rolling_congestion_index) AS total_congestion,
        ROW_NUMBER() OVER (
            PARTITION BY hour(window_start)
            ORDER BY SUM(rolling_congestion_index) DESC
        ) AS rnk
    FROM traffic_alerts
    GROUP BY hour(window_start), zone
) ranked
WHERE rnk <= 5
ORDER BY hour_of_day, total_congestion DESC;
