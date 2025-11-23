--Avg Speed per zone
SELECT zone,
       AVG(avg_speed) AS avg_speed_zone
FROM traffic_data
GROUP BY zone
ORDER BY avg_speed_zone ASC;