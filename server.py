import io
import avro.schema
import avro.io
import asyncio
import random
import websockets

async def send_avro(websocket, path):
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
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write({"name": "Alyssa", "favorite_number": 256}, encoder)

    raw_bytes = bytes_writer.getvalue()

    while True:
        print(raw_bytes)
        await websocket.send(raw_bytes)
        await asyncio.sleep(random.random() * 3)


start_server = websockets.serve(send_avro, 'localhost', 5670)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()