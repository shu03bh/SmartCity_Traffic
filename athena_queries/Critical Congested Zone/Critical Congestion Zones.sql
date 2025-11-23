--Critical Zones

SELECT zone,
       COUNT(*) AS critical_events
FROM traffic_alerts
WHERE critical_flag = 'Y'
GROUP BY zone
ORDER BY critical_events DESC
LIMIT 10;