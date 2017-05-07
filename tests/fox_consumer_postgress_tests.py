from fox_consumer.consumer_db_support import FoxConsumerPostgres
from mock import patch
from StringIO import StringIO
from fox_consumer import FoxItem
import unittest

class TestFoxConsumerPostgres(unittest.TestCase):

    # TODO - ASLI - learn and use "side affect"

    @patch('fox_consumer.consumer_db_support.psycopg2.connect')          # 'override' the connection
    @patch.object(FoxConsumerPostgres, 'check_and_create_fox_table')    # 'override' specific method
    def setUp(self, mock_init_postgres, mock_connection):
        self.fox_db = FoxConsumerPostgres()

    @patch('fox_consumer.consumer_db_support.FoxConsumerPostgres.is_item_exist', return_value=True)
    def test_save_item_with_exist_item(self,  mock_is_item_exist):
        item = FoxItem("test_img", "test_main_cat", "test_type", "test_name", 10)
        sql_update = "UPDATE fox_items SET item_main_category = 'test_main_cat', item_type = 'test_type', " \
                     "item_name = 'test_name', item_price = '10' WHERE item_img_id = 'test_img'"
        self.fox_db.save_item(item)
        self.fox_db.db_curser.execute.assert_called_with(sql_update)
        self.assertEquals(self.fox_db.postgress_connection.commit.called, True)

    @patch('fox_consumer.consumer_db_support.FoxConsumerPostgres.is_item_exist', return_value=False)
    def test_save_item_with_non_exist_item(self, mock_is_item_exist):
        item = FoxItem("test_img", "test_main_cat", "test_type", "test_name", 10)
        sql_insert = "INSERT INTO fox_items (item_img_id, item_main_category, item_type, item_name, item_price) " \
                     "VALUES ('%(item_img_id)s', '%(item_main_category)s', '%(item_type)s', '%(item_name)s', '%(item_price)s')" \
                     %item.to_json()
        self.fox_db.save_item(item)
        self.fox_db.db_curser.execute.assert_called_with(sql_insert)
        self.assertEquals(self.fox_db.postgress_connection.commit.called, True)

    def test_is_item_exist_with_exist(self):
        self.fox_db.db_curser.return_value = True
        item = FoxItem("test_img", "test_main_cat", "test_type", "test_name", 10)
        sql_select = "SELECT * FROM fox_items WHERE item_img_id = 'test_img';"
        self.fox_db.is_item_exist(item)
        self.fox_db.db_curser.execute.assert_called_with(sql_select)

    def test_is_item_exist_with_non_exist(self):
        self.fox_db.db_curser.return_value = False
        item = FoxItem("test_img", "test_main_cat", "test_type", "test_name", 10)
        sql_select = "SELECT * FROM fox_items WHERE item_img_id = 'test_img';"
        self.fox_db.is_item_exist(item)
        self.fox_db.db_curser.execute.assert_called_with(sql_select)

if __name__ == "__main__":
    unittest.main()