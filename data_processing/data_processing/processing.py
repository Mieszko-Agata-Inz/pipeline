from datetime import timedelta, datetime, timezone
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaInput, KafkaOutput
from bytewax.connectors.stdio import StdOutput  # for testing purposes
from bytewax.window import (
    EventClockConfig,
    SlidingWindow,
    SystemClockConfig,
    TumblingWindow,
)
from bytewax.tracing import setup_tracing
import json, time

time.sleep(40)

tracer = setup_tracing(log_level="DEBUG")


# maping for key as hash and temperature as value
def map1(key_bytes__payload_bytes):
    key, payload_bytes = key_bytes__payload_bytes
    event_data = json.loads(payload_bytes) if payload_bytes else None

    if key == None:
        return "key", (0, 0, 0, 0, 0, 0, 1)
    else:
        # return 'key', json.dumps(event_data).encode()
        return key.decode("ascii"), (
            event_data["timestamp"],
            event_data["lat"],
            event_data["long"],
            event_data["temp"],
            event_data["wind_v"],
            event_data["humidity"],
            event_data["count"],
        )


# maping to add to the kafka topic as key and json
def map2(aggreagted_data):
    data = {
        "timestamp": aggreagted_data[1][0],
        "lat": aggreagted_data[1][1],
        "long": aggreagted_data[1][2],
        "temp": aggreagted_data[1][3] / aggreagted_data[1][6],
        "wind_v": aggreagted_data[1][4] / aggreagted_data[1][6],
        "humidity": aggreagted_data[1][5] / aggreagted_data[1][6],
    }
    # return 'key', json
    return aggreagted_data[0], json.dumps(data)


# alignment =  datetime.now(tz=timezone.utc)

# to change!
clock_config = SystemClockConfig()
# clock_config = EventClockConfig(
#     lambda e: e["time"], wait_for_system_duration=timedelta(seconds=10)
# )

# so far one minute only and every 30 seconds new window
window_config = SlidingWindow(
    length=timedelta(minutes=1),
    offset=timedelta(seconds=30),
    align_to=datetime(2023, 9, 4, tzinfo=timezone.utc)
    + timedelta(seconds=10),  # datatime to change?
)


# averages for each hash
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


# data flow
flow = Dataflow()
flow.input(
    "input",
    KafkaInput(brokers=["kafka:29092"], topics=["weather_data"], tail=True),
)

flow.map(map1)
flow.reduce_window("sum", clock_config, window_config, add)
flow.map(map2)

flow.output(
    "k_output", KafkaOutput(brokers=["kafka:29092"], topic="weather_data_output")
)
