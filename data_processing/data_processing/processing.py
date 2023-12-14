from datetime import timedelta, datetime, timezone
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaInput, KafkaOutput
from bytewax.connectors.stdio import StdOutput  # For testing purposes
from bytewax.window import (
    EventClockConfig,
    SlidingWindow,
    SystemClockConfig,
    TumblingWindow,
)
from bytewax.tracing import setup_tracing
import json, time

# Delay execution for 40 seconds, typically to ensure system readiness or dependencies
time.sleep(40)

# Set up tracing for debugging purposes, with a log level of DEBUG
tracer = setup_tracing(log_level="DEBUG")

# Function to map key-value pairs from Kafka; deserializes JSON payload
def map1(key_bytes__payload_bytes):
    key, payload_bytes = key_bytes__payload_bytes
    # If payload is present, load it as JSON, else return None
    event_data = json.loads(payload_bytes) if payload_bytes else None

    # If key is None, return a default key and a tuple of zeros and one
    if key == None:
        return "key", (0, 0, 0, 0, 0, 0, 1)
    else:
        # Decode key from bytes to ASCII and return it with event data values as tuple
        return key.decode("ascii"), (
            event_data["timestamp"],
            event_data["lat"],
            event_data["long"],
            event_data["temp"],
            event_data["wind_v"],
            event_data["humidity"],
            event_data["count"],
        )

# Function to map aggregated data to JSON, preparing it for Kafka output
def map2(aggreagted_data):
    # Aggregate data and calculate averages for temperature, wind velocity, and humidity
    data = {
        "timestamp": aggreagted_data[1][0],
        "lat": aggreagted_data[1][1],
        "long": aggreagted_data[1][2],
        "temp": aggreagted_data[1][3] / aggreagted_data[1][6],
        "wind_v": aggreagted_data[1][4] / aggreagted_data[1][6],
        "humidity": aggreagted_data[1][5] / aggreagted_data[1][6],
    }
    # Return the key and the JSON serialized aggregated data
    return aggreagted_data[0], json.dumps(data)

# Clock configuration for windowing; currently using system time
# Uncomment the EventClockConfig for event time-based processing
clock_config = SystemClockConfig()
# clock_config = EventClockConfig(
#     lambda e: e["time"], wait_for_system_duration=timedelta(seconds=10)
# )

# Configuration for a sliding window of 1 minute with a 30-second offset
# Adjust the alignment time as needed
window_config = SlidingWindow(
    length=timedelta(minutes=1),
    offset=timedelta(seconds=30),
    align_to=datetime(2023, 9, 4, tzinfo=timezone.utc)
    + timedelta(seconds=10),  # Update alignment datetime as needed
)

# Function to aggregate data by summing up the counts
def add(count1, count2):
    return (
        count1[0],
        count1[1],
        count1[2],
        count1[3] + count2[3],
        count1[4] + count2[4],
        count1[5] + count2[5],
        count1[6] + count2[6],
    )

# Define the dataflow for processing
flow = Dataflow()
# Input from Kafka topic "weather_data"
flow.input(
    "input",
    KafkaInput(brokers=["kafka:29092"], topics=["weather_data"], tail=True),
)

# Apply the mapping and reduction functions
flow.map(map1)
flow.reduce_window("sum", clock_config, window_config, add)
flow.map(map2)

# Output the processed data to a Kafka topic "weather_data_output"
flow.output(
    "k_output", KafkaOutput(brokers=["kafka:29092"], topic="weather_data_output")
)
