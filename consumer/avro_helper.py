from fastavro import schemaless_reader, schemaless_writer, parse_schema
import io
import json
import os

def load_schema():
    """Load and parse Avro schema"""
    schema_path = os.path.join(os.path.dirname(__file__), '..', 'schemas', 'order.avsc')
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    return parse_schema(schema)

def deserialize_avro(avro_bytes, schema):
    """Deserialize Avro binary to Python dict"""
    bytes_reader = io.BytesIO(avro_bytes)
    return schemaless_reader(bytes_reader, schema)

def serialize_avro(message, schema):
    """Serialize Python dict to Avro binary"""
    bytes_writer = io.BytesIO()
    schemaless_writer(bytes_writer, schema, message)
    return bytes_writer.getvalue()


