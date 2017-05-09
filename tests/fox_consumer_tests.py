from fox_consumer import Consumer
from fox_consumer.consumer_db_support import PostgresWrapper
from mock import patch
from StringIO import StringIO
from fox_consumer import FoxItem
import unittest
import json
import sys

class TestFoxConsumer(unittest.TestCase):
    @patch('fox_consumer.PostgresWrapper')          # 'override' the entire object
    def setUp(self, mock_postgres):
        self.consumer = Consumer()

    @patch.object(Consumer, 'initialize_rabbitmq_connection')    # 'override' specific method in Consumer object
    @patch.object(Consumer, 'start_consumer')
    @patch.object(PostgresWrapper, 'runner')     # 'override' specific method in PostgresWrapper object
    def test_runner(self, mock_runner, mock_start_consumer, mock_initialize_rabbitmq):
        self.consumer.runner()
        self.assertEquals(mock_initialize_rabbitmq.called, True)
        self.assertEquals(mock_runner.called, True)
        self.assertEquals(mock_start_consumer.called, True)

    @patch.object(PostgresWrapper, 'save_item')
    def test_handle_message_with_valid_json(self, mock_save_item):
        body = {"item_img_id": "test_img", "item_main_category": "test_main_cat", "item_type": "test_type", "item_name": "test_name", "item_price": 10}
        self.consumer.handle_message(None, None, None, json.dumps(body))
        self.assertEquals(mock_save_item.called, True)

    @patch.object(PostgresWrapper, 'close_connection')
    def test_close_connection_postgres(self, mock_db_close_connection):
        self.consumer.close_connection()
        self.assertEquals(mock_db_close_connection.called, True)

    def test_handle_message_with_invalid_json(self):
        save_for_restore_stdout = sys.stdout
        sys.stdout = StringIO()   # prepare to get input from output prints
        self.consumer.handle_message(None, None, None, "temp")  # send bad json
        output = sys.stdout.getvalue().strip()
        output_all = output.splitlines()
        output = output_all.pop()
        self.assertEquals(output, 'Could not convert from json')
        sys.stdout = save_for_restore_stdout

if __name__ == "__main__":
    unittest.main()
