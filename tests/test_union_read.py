import fastavro

schema = {
    "name": "Message",
    "type": "record",
    "namespace": "test",
    "fields": [
        {"name": "id",
         "type": "long"},
        {"name": "payload",
         "type": [
             {
                 "name": "ApplicationCreated",
                 "type": "record",
                 "fields": [
                     {"name": "applicationId", "type": "string"},
                     {"name": "data", "type": "string"},
                     {"name": "origin", "type": "string", "default": "ApplicationCreated"}
                 ]
             },
             {
                 "name": "ApplicationSubmitted",
                 "type": "record",
                 "fields": [
                     {"name": "applicationId", "type": "string"},
                     {"name": "data", "type": "string"},
                     {"name": "origin", "type": "string", "default": "ApplicationSubmitted"}
                 ]
             },
         ]}
    ]
}



parsed_schema = fastavro.parse_schema(schema)

input_record = {"id": 123, "payload": ("test.ApplicationSubmitted", {"applicationId": "123456789UT", "data": "..."})}

with open('test_output', 'wb') as f:
    fastavro.schemaless_writer(f, parsed_schema, input_record)

with open('test_output', 'rb') as f:
    output_record = fastavro.schemaless_reader(f, parsed_schema)
print(output_record)
