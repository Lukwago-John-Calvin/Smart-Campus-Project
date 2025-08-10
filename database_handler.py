import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import os # To load credentials safely

# --- Load Credentials (replace with your actual details) ---
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "INFLUXDB_API=CM73hsJUqjIOOiVaqlYbPPW5_GzNhgENJvQP_kqnQ8Lgnq3zEBvB8aIdPex1Xu-EIHQb-ZvsJIEpWd6ZsEYQJA==" # Your InfluxDB API Token
INFLUX_ORG = "Netlabs!UG"          # Your InfluxDB Organization
INFLUX_BUCKET = "sensor_data"

# --- Initialize InfluxDB Client ---
client = influxdb_client.InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)
write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

def write_sensor_data(data):
    """Writes a dictionary of sensor data to InfluxDB."""
    try:
        node_id = data.pop("node_id", "unknown_node") 
        
        point = influxdb_client.Point("campus_metrics") \
            .tag("node_id", node_id)

        # --- Common Fields ---
        if "temperature" in data:
            point.field("temperature", float(data["temperature"]))
        if "humidity" in data:
            point.field("humidity", float(data["humidity"]))
        if "air_quality" in data:
            point.field("air_quality", float(data["air_quality"]))
        if "light_level" in data:
            point.field("light_level", int(data["light_level"]))
        if "presence" in data:
            point.field("presence", bool(data["presence"]))
        if "intrusion" in data:
            point.field("intrusion", bool(data["intrusion"]))

        # --- Security Specific (optional) ---
        if "distance_cm" in data:
            point.field("distance_cm", float(data["distance_cm"]))
        if "rfid_uid" in data:
            point.field("rfid_uid", str(data["rfid_uid"]))
        if "access_status" in data:
            point.field("access_status", str(data["access_status"]))

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        print(f"✅ Data written for node {node_id}")

    except Exception as e:
        print(f"❌ Error writing to InfluxDB: {e}")


def query_historical_data(range_start="-1h"):
    """Queries data from the last hour (or specified range)."""
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {range_start})
      |> filter(fn: (r) => r._measurement == "campus_metrics")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    try:
        result = query_api.query(query, org=INFLUX_ORG)
        return result
    except Exception as e:
        print(f"Error querying InfluxDB: {e}")
        return None
