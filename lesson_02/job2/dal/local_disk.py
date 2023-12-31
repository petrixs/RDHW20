import json
from typing import List, Dict, Any
import fastavro


def infer_avro_schema_from_json(json_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    fields = []
    sample_record = json_data[0]

    for field_name, field_value in sample_record.items():
        field_schema = {'name': field_name}

        if isinstance(field_value, str):
            field_schema['type'] = 'string'
        elif isinstance(field_value, bool):
            field_schema['type'] = 'boolean'
        elif isinstance(field_value, int):
            field_schema['type'] = 'int'
        elif isinstance(field_value, float):
            field_schema['type'] = 'float'
        elif field_value is None:
            field_schema['type'] = ['null', 'string']
        else:
            raise ValueError(f"Unsupported type for field {field_name}")

        fields.append(field_schema)

    return {
        'type': 'record',
        'name': 'AutoGeneratedSchema',
        'namespace': 'com.example',
        'fields': fields
    }


def transform_to_avro_record(json_record: Dict[str, Any], avro_schema) -> Dict[str, Any]:
    avro_record = {}
    for field in avro_schema['fields']:
        field_name = field['name']
        avro_record[field_name] = json_record.get(field_name, field.get('default'))
    return avro_record


def save_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:
    avro_schema = infer_avro_schema_from_json(json_content)
    avro_records = [transform_to_avro_record(record, avro_schema) for record in json_content]

    with open(path, 'wb') as file:
        fastavro.writer(file, avro_schema, avro_records)
