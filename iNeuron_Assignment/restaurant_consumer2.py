import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


API_KEY = 'W4M6PQHMPLWONFFK'
ENDPOINT_SCHEMA_URL  = 'https://psrc-68gz8.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'kQw6sqIJ5v7IpxkZzsdwovsyThvxwM94py5QoNtggIiwwn+r1ZrlWwPaXzUcKMFG'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = '46ON5XAUFDDAEEIO'
SCHEMA_REGISTRY_API_SECRET = 'krjL62JIsY5tkTKQ8+EfsKARPvGHKVK5WRfdrEVy0axFfxpFcqNFyUoeyVCx6gHN'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Order:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_order(data:dict,ctx):
        return Order(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

    schema_str = """
    {
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "Item_Name": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "Order_Date": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "Order_Number": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "Product_Price": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "Total_products": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "Quantity": {
      "description": "The type(v) type is used.",
      "type": "number"
    }    
  },
  "title": "SampleRecord",
  "type": "object"
}
    """
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Order.dict_to_order)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
 
    count=0
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                print('No.of records consumed by consumer2: '+str(count))
                continue

            order = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if order is not None:
                print("User record {}: order: {}\n"
                      .format(msg.key(), order))
                count+=1
        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurent-take-away-data")