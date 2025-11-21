from fastavro import parse_schema, schemaless_writer
import io
import json
import os

def load_schema():
    """Load and parse Avro schema"""
    schema_path = os.path.join(os.path.dirname(__file__), '..', 'schemas', 'order.avsc')
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    return parse_schema(schema)

def serialize_avro(message, schema):
    """Serialize Python dict to Avro binary"""
    bytes_writer = io.BytesIO()
    schemaless_writer(bytes_writer, schema, message)
    return bytes_writer.getvalue()


