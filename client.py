import websockets
import asyncio
import avro.schema
import io
import avro.io


async def recv_avro():
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

    async with websockets.connect('ws://localhost:5670') as websocket:
        while True:
            raw_bytes = await websocket.recv()
            bytes_reader = io.BytesIO(raw_bytes)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(schema)
            user = reader.read(decoder)
            print("Received...")
            print(user)


asyncio.get_event_loop().run_until_complete(recv_avro())
