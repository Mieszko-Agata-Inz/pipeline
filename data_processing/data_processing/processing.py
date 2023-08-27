from datetime import timedelta, datetime, timezone
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaInput, KafkaOutput
from bytewax.connectors.stdio import StdOutput #for testing purposes
from bytewax.window import EventClockConfig, SlidingWindow
from bytewax.tracing import setup_tracing
import json, time

time.sleep(40)

tracer = setup_tracing(log_level='DEBUG')

def deserialize(key_bytes__payload_bytes):
    _, payload_bytes = key_bytes__payload_bytes
    event_data = json.loads(payload_bytes) if payload_bytes else None

    if(event_data == None):
        return 'key', json.dumps(event_data).encode()
    else:
        # return 'key', json.dumps(event_data).encode()
        return event_data["hash"], json.dumps(event_data)



clock_config = EventClockConfig(
    lambda e: e.time, wait_for_system_duration=timedelta(seconds=10)
)

alignment = datetime.now(tz=timezone.utc)
#so far one minute only and every 10 seconds new window
window_config = SlidingWindow(
    length = timedelta(minutes=1),
    offset  = timedelta(seconds=1),
    align_to  = alignment,
)

def add(acc, x):
    # acc.append(x)
    return acc


flow = Dataflow()

flow.fold_window("sum", clock_config, window_config, list, add)

flow.input( 
    "input",
    KafkaInput(brokers=["kafka:29092"], topics=["weather_data"], tail=True),
)

flow.map(deserialize)

flow.output("k_output", KafkaOutput(brokers=["kafka:29092"], topic="weather_data_output"))
#flow.output("output", StdOutput())