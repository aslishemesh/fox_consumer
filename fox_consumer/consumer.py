# coding=utf-8
import pika
from fox_helper import FoxItem
import json
import jsonschema
from consumer_db_support import PostgresWrapper

class Consumer(object):

    """
    1. Consumer overview:
        The Consumer class will be used for receive input from rabbitmq,
        convert from json to FoxItem and send it to postgress server.
    2. The consumer will receive catalog from rabbitmq, it will verify the input and save it to the DB.
        1. The connection to rabbitmq server will be continuous (initialization)
        2. The connection to postgress DB - The main connection session will be continuous (initialization)
    3. The consumer will receive the data from rabbitmq (“pika” package)
    4. The consumer will send and save the data to the postgresql (“psycopg2” package)
        1. Get json
        2. Convert to FoxItem
        3. Save to DB
    5. Consumer structure:
        1. The consumer input - FoxItems from rabbitmq
        2. The consumer will use the follow main functions:
            1. initialize
                1. Create a connection to rabbitmq
                2. Create a connection to postgress
            2. Receive input
                1. Verify input
                2. verify duplicate data
                3. Save input to DB
    6. Problems:
        1. Connection to rabbitmq
            1. Server is down
                1. Assumption - no problems in rabbitmq (same as scraper…)
            2. Lost connection (msec/sec)
                1. Assumption - no problems in rabbitmq (same as scraper…)
            3. Exchange not available
                1. Assumption - no problems in rabbitmq (same as scraper…)
                2. Change to not exist - the consumer will create it
        2. Input problems
            1. Data from not authorize source
                1. How to verify it? - need to google
                2. Since we have a dedicated exchange I think we should use an assumption it won’t happen
                (same as we talk about the unnecessary routing key)
            2. Data corrupted
                1. verify json data
                    1. We can use json.loads() which will throw “ValueError” (in try-catch)
                    2. verify the json data using schema ("jsonschema" package)
        3. Connection to postgres
            1. Server is down
                1. Assumption - no problems in postgres (same as rabbitmq…)
            2. Lost connection
                1. Verify every query using try-catch (if necessary - reestablish the connection)
            3. Cannot update the table (lost connection before executing - before updating table)
                1. Verify in the initialization that the table exist and create it if needed
                2. Verify every query using try-catch (if necessary - reestablish the connection)
        4. Postgres problems
            1. DB not available (not exist)
                1. Assumption - the DB should be ready before running the service
            2. create table - if it already exist
                1. check if table exist - if not create
                2. The table won’t be deleted after the consumer has initialized
            3. Duplicate data
                1. Look for the primary key (img-id) in the DB before saving item

    """
    def __init__(self):
        """
        creation of Consumer (using the 'with' statement)
        """
        self.schema = {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "title": "FoxItem",
                "description": "An item for Fox catalog",
                "type": "object",
                "properties": {
                    "item_img_id": {"type": "string"},
                    "item_main_category": {"type": "string"},
                    "item_type": {"type": "string"},
                    "item_name": {"type": "string"},
                    "item_price": {"type": "number"},
                },
                "required": ["item_img_id", "item_main_category", "item_type", "item_name", "item_price"]
            }

        try:
            self.consumer_db = PostgresWrapper()
        except:
            self.close_connections("Cannot connect to postgres server")
        try:
            self.initialize_rabbitmq_connection()
            self.declare_queue()
            self.start_consumer()
        except:
            self.close_connections("Cannot connect to rabbitmq server")

    def close_connections(self, close_reason):
        print "closing connections...."
        print "reason -", close_reason
        try:
            self.rabbitmq_connection.close()
        except:
            print " cannot close rabbitmq connection"
        try:
            self.consumer_db.close_connections()
        except:
            print " cannot close postgres connection"

    def initialize_rabbitmq_connection(self):
        """
        This function will initialize rabbitmq connection.
        The Consumer will connect to a pre-define exchange server (fox_scrap) and start to wait for input
        """
        self.rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.rabbitmq_channel = self.rabbitmq_connection.channel()
        self.rabbitmq_channel.exchange_declare(exchange='fox_scrap', type='fanout')

    def declare_queue(self):
        result = self.rabbitmq_channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.rabbitmq_channel.queue_bind(exchange='fox_scrap', queue=queue_name, routing_key='')
        self.rabbitmq_channel.basic_consume(self.callback, queue=queue_name, no_ack=True)

    def start_consumer(self):
        """
        start consumer (listening to rabbitmq)
        """
        try:
            self.rabbitmq_channel.start_consuming()
        except KeyboardInterrupt:
            self.close_connections("User request")
        except:
            self.close_connections("Unknown error")

    def callback(self, method, properties, asd, body):
        """
        This is the "main" function for the Consumer.
        every time the consumer recieve input it will verify it and add to the DB.
        :param body: input from rabbitmq server
        """
        if self.verify_json(body):
            current_item = self.convert_json_to_fox_item(body)
            self.consumer_db.save_item(current_item)
        else:
            print "the input is corrupted..."

    def verify_json(self, data):
        """
        Verify json input according to the schema defined for FoxItem class
        :param data: json input from rabbitmq server
        :return: True/False (verified/not)
        """
        try:
            jsonschema.validate(json.loads(data), self.schema)
            return True
        except jsonschema.ValidationError as e:
            print e.message
        except jsonschema.SchemaError as e:
            print e
        return False

    def convert_json_to_fox_item(self, obj):
        """
        This function will convert the json to FoxItem class
        :param obj: json object
        :return: FoxItem object
        """
        item = json.loads(obj)
        item = FoxItem(**item)
        return item
