# coding=utf-8
import psycopg2
import pika
from fox_helper import FoxItem
import json
import jsonschema


class FoxConsumer(object):

    """
    1. FoxConsumer overview:
        The FoxConsumer class will be used for receive input from rabbitmq
        and send convert from json to FoxItem and send it to postgress server.
    2. The consumer will receive catalog from rabbitmq, it will verify the input and save it to the DB.
        1. The connection to rabbitmq server will be continuous (initialization)
        2. The connection to postgress DB - The main connection session will be continuous (initialization)
    3. The consumer will receive the data from rabbitmq (“pika” package)
    4. The consumer will send and save the data to the postgresql (“psycopg2” package)
        1. Get json
        2. Convert to FoxItem
        3. Save to DB
    5. FoxConsumer structure:
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
        creation of FoxConsumer (using the 'with' statement)
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

        self.initialize_postgress_connection()
        self.initialize_rabbitmq_connection()

    def close_connections(self):
        # TODO - Caduri - since it should run continuously, do we need this method?
        self.rabbitmq_connection.close()
        self.db_curser.close()
        self.postgress_connection.close()

    def initialize_rabbitmq_connection(self):
        """
        This function will initialize rabbitmq connection.
        The FoxConsumer will connect to a pre-define exchange server (fox_scrap) and start to wait for input
        """
        self.rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.rabbitmq_channel = self.rabbitmq_connection.channel()
        self.rabbitmq_channel.exchange_declare(exchange='fox_scrap', type='fanout')
        result = self.rabbitmq_channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.rabbitmq_channel.queue_bind(exchange='fox_scrap', queue=queue_name, routing_key='')
        self.rabbitmq_channel.basic_consume(self.callback, queue=queue_name, no_ack=True)
        self.rabbitmq_channel.start_consuming()

    def initialize_postgress_connection(self):
        """
        This function will initialize postgress database connection.
        it will connect to "mtdb" database and check if the "fox_item" table exist, id not it will create it.
        """
        # Connect to an existing database
        self.postgress_connection = psycopg2.connect("dbname=mydb user=aslishemesh")
        # Open a cursor to perform database operations
        self.db_curser = self.postgress_connection.cursor()

        # verify if the table already exist
        sql_query_table = "select exists(select * from information_schema.tables where table_name='fox_items');"
        self.db_curser.execute(sql_query_table)
        table_exist = self.db_curser.fetchone()[0]
        if table_exist == False:
            self.db_curser.execute("CREATE TABLE fox_items ("
                                    "item_img_id varchar, "
                                    "item_main_category varchar, "
                                    "item_type varchar, "
                                    "item_name varchar, "
                                    "item_price real);")
            # Make the changes to the database persistent (allows us to see the DB update live in Postico)
            self.postgress_connection.commit()

    def callback(self, method, properties, asd, body):
        """
        This is the "main" function for the FoxConsumer.
        every time the consumer recieve input it will verify it and add to the DB.
        :param body: input from rabbitmq server
        """
        print(" [x] Received %r" % body)    # TODO - will be deleted at the end...

        if self.verify_json(body):
            current_item = self.convert_json_to_fox_item(body)
            self.save_item(current_item)
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
        item = FoxItem(item['item_img_id'], item['item_main_category'], item['item_type'], item['item_name'], item['item_price'])
        return item

    def save_item(self, item):
        """
        This function will save the data to the DB.
        (if the data already exist it will update it)
        :param item: FoxItem object
        """
        if self.is_item_exist(item):
            sql_update = "UPDATE fox_items SET item_main_category = %s, item_type = %s, item_name = %s, item_price = %s WHERE item_img_id = %s"
            self.db_curser.execute(sql_update, (item.item_main_category, item.item_type, item.item_name, item.item_price, item.item_img_id))
            self.postgress_connection.commit()
        else:
            sql_insert = "INSERT INTO fox_items (item_img_id, item_main_category, item_type, item_name, item_price) VALUES (%s, %s, %s, %s, %s)"
            self.db_curser.execute(sql_insert, (item.item_img_id, item.item_main_category, item.item_type, item.item_name, item.item_price))
            self.postgress_connection.commit()

    def is_item_exist(self, item):
        """
        This function will verify if the item exist in DB
        (using the primary-key - image url (item_image_id)
        :param item: FoxItem
        :return: True/False (exist/not)
        """
        self.db_curser.execute("SELECT * FROM fox_items WHERE item_img_id = (%s);", (item.item_img_id,))
        return False if self.db_curser.fetchone() == None else True
