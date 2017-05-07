from fox_consumer import FoxConsumer
from fox_consumer.consumer_db_support import FoxConsumerPostgres
from mock import patch
from StringIO import StringIO
from fox_consumer import FoxItem
import unittest
import json
import sys

class TestFoxConsumer(unittest.TestCase):
    @patch.object(FoxConsumer, 'initialize_rabbitmq_connection')    # 'override' specific method in FoxConsumer object
    @patch('fox_consumer.FoxConsumerPostgres')          # 'override' the entire object
    def setUp(self, mock_postgres, mock_connection):
        self.consumer = FoxConsumer()

    @patch.object(FoxConsumerPostgres, 'save_item')     # 'override' specific method in FoxConsumerPostgres object
    def test_handle_message_with_valid_json(self, mock_save_item):
        body = {"item_img_id": "test_img", "item_main_category": "test_main_cat", "item_type": "test_type", "item_name": "test_name", "item_price": 10}
        self.consumer.handle_message(None, None, None, json.dumps(body))
        self.assertEquals(mock_save_item.called, True)

    def test_handle_message_with_invalid_json(self):
        save_for_restore_stdout = sys.stdout
        sys.stdout = StringIO()   # prepare to get input from output prints
        self.consumer.handle_message(None, None, None, json.dumps("temp"))  # send bad json
        output = sys.stdout.getvalue().strip()
        self.assertEquals(output, 'the input is corrupted...')
        sys.stdout = save_for_restore_stdout

    def test_verify_invalid_json(self):
        self.assertEquals(self.consumer.verify_json(json.dumps("temp")), False)

    def test_verify_valid_json(self):
        data = {"item_img_id": "test_img", "item_main_category": "test_main_cat", "item_type": "test_type", "item_name": "test_name", "item_price": 10}
        self.assertEquals(self.consumer.verify_json(json.dumps(data)), True)

    def test_convert_json_to_fox_item(self):
        json_data = {"item_img_id": "test_img", "item_main_category": "test_main_cat", "item_type": "test_type", "item_name": "test_name", "item_price": 10}
        item = self.consumer.convert_json_to_fox_item(json.dumps(json_data))
        self.assertEquals(item, FoxItem("test_img", "test_main_cat", "test_type", "test_name", 10))

if __name__ == "__main__":
    unittest.main()