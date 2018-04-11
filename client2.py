import websocket
from threading import Thread
import avro.schema
import io
import avro.io
from datetime import datetime
import json


output_file = 'Data{}.txt'.format(datetime.now().date())
test_schema = '''
   {
   "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number",  "type": ["int", "null"]},
        {"name": "favorite_color", "type": ["string", "null"]}
    ]
   }
   '''
schema = avro.schema.Parse(test_schema)


def on_message(ws, message):
    print(message)

    def run(*args):
        raw_bytes = args[0]
        bytes_reader = io.BytesIO(raw_bytes)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        decoded_data = reader.read(decoder)
        with open(output_file, 'a') as text_file:
            text_file.write('\n Received at {:%Y-%m-%d %H:%M} :\n'.format(datetime.now()))
            text_file.write(json.dumps(decoded_data))
    t = Thread(target=run, args=(message,))
    t.start()


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### closed ###")


def on_open(ws):
    print("### Initiating new websocket connection ###")


def initiate():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("ws://localhost:5670/",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()


if __name__ == "__main__":
    initiate()