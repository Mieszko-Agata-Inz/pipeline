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
    #return 'key', event_data
    return 'key', json.dumps(event_data).encode()




flow = Dataflow()

flow.input( 
    "input",
    KafkaInput(brokers=["kafka:29092"], topics=["weather_data"], tail=True),
)

flow.map(deserialize)

flow.output("k_output", KafkaOutput(brokers=["kafka:29092"], topic="weather_data_output"))
#flow.output("output", StdOutput())