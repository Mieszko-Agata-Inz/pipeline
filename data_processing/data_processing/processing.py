from datetime import timedelta, datetime, timezone
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaInput, KafkaOutput
from bytewax.connectors.stdio import StdOutput #for testing purposes
from bytewax.window import EventClockConfig, SlidingWindow, SystemClockConfig, TumblingWindow
from bytewax.tracing import setup_tracing
import json, time

time.sleep(40)

tracer = setup_tracing(log_level='DEBUG')

# def deserialize(key_bytes__payload_bytes):
#     _, payload_bytes = key_bytes__payload_bytes
#     event_data = json.loads(payload_bytes) if payload_bytes else None

#     if(event_data == None):
#         return 'key', json.dumps(event_data).encode()
#     else:
#         # return 'key', json.dumps(event_data).encode()
#         return event_data["hash"], json.dumps(event_data)

#maping for key as hash and temperature as value
def map1(key_bytes__payload_bytes):
    _, payload_bytes = key_bytes__payload_bytes
    event_data = json.loads(payload_bytes) if payload_bytes else None

    if(event_data == None):
        return 'key', 0
    else:
        # return 'key', json.dumps(event_data).encode()
        return event_data["hash"], event_data["temp"]


#maping to add to the kafka topic as key and json
def map2(aggreagted_data):
        # return 'key', json.dumps(event_data).encode()
        return aggreagted_data[0], json.dumps(aggreagted_data[1]).encode()
        # return key, json.dumps(String(value)).encode()


# clock_config = EventClockConfig(
#     lambda e: e["time"], wait_for_system_duration=timedelta(seconds=10)
# )

# alignment =  datetime.now(tz=timezone.utc)

#so far one minute only and every 10 seconds new window
window_config = SlidingWindow(
    length = timedelta(minutes=1),
    offset  = timedelta(seconds=1),
    align_to  = datetime(2023, 8, 28, tzinfo=timezone.utc),
)

#to change!
clock_config = SystemClockConfig()
# window_config = TumblingWindow(
#     length=timedelta(minutes=2), align_to=datetime(2023, 8, 28, tzinfo=timezone.utc)
# )

#so far only sum the data - returns the sum of temepratures - change to averages for each hash
def add(count1, count2):
    return count1 + count2


flow = Dataflow()

# flow.fold_window("sum", clock_config, window_config, list, add)

flow.input( 
    "input",
    KafkaInput(brokers=["kafka:29092"], topics=["weather_data"], tail=True),
)

flow.map(map1)
flow.reduce_window("sum", clock_config, window_config, add)
flow.map(map2)

flow.output("k_output", KafkaOutput(brokers=["kafka:29092"], topic="weather_data_output"))