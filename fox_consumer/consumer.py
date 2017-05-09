# coding=utf-8
import pika
from fox_helper import FoxItem
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
        creation of Consumer
        """
        self.rabbitmq_connection = None
        self.consumer_db = PostgresWrapper()

    def runner(self):
        """
        this will be the main function the consumer will run
        """
        try:
            self.initialize_rabbitmq_connection()
            self.consumer_db.runner()
        except Exception as e:
            print "Starting consumer failed"
            raise e
        self.start_consumer()

    def close_connection(self):
        """
        close connections (postgres + rabbitmq servers)
        :return: 
        """
        print "closing connections...."
        self.consumer_db.close_connection()
        if self.rabbitmq_connection and not self.rabbitmq_connection.is_closed:
            self.rabbitmq_connection.close()

    def initialize_rabbitmq_connection(self):
        """
        This function will initialize rabbitmq connection.
        The Consumer will connect to a pre-define exchange server (fox_scrap) and start to wait for input
        """
        self.rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.rabbitmq_channel = self.rabbitmq_connection.channel()
        self.rabbitmq_channel.exchange_declare(exchange='fox_scrap', type='fanout')
        self.rabbitmq_channel.queue_declare(queue='fox_scrap_queue', exclusive=True)
        self.rabbitmq_channel.queue_bind(exchange='fox_scrap', queue='fox_scrap_queue', routing_key='')

    def start_consumer(self):
        """
        start consumer (listening to rabbitmq)
        """
        try:
            self.rabbitmq_channel.basic_consume(self.handle_message, queue='fox_scrap_queue', no_ack=True)
            self.rabbitmq_channel.start_consuming()
        except Exception as e:
            raise

    def handle_message(self, method, properties, asd, body):
        """
        This is the "main" function for the Consumer.
        every time the consumer recieve input it will verify it and add to the DB.
        :param body: input from rabbitmq server
        """
        try:
            current_item = FoxItem.from_json(body)
            self.consumer_db.save_item(current_item)
        except Exception as e:
            print "the input is corrupted...\n", e

