SELECT month, year
FROM weather_data
WHERE station_id = {}
ORDER BY year DESC, month DESC
LIMIT 1