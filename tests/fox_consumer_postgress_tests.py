from fox_consumer import PostgresWrapper
from fox_consumer import FoxItem
from mock import patch, MagicMock
import unittest
import psycopg2

class TestCursor(object):
    def cursor(self):
        return True

class TestPostgresWrapper(unittest.TestCase):
    @patch('fox_consumer.consumer_db_support.psycopg2.connect')          # 'override' the connection
    @patch.object(PostgresWrapper, 'check_and_create_fox_table')    # 'override' specific method
    def setUp(self, mock_init_postgres, mock_connection):
        self.fox_db = PostgresWrapper()
        self.fox_db.connect_to_postgres()

    @patch('fox_consumer.consumer_db_support.psycopg2.connect')          # 'override' the connection
    def test_connect_to_postgres_fail_to_connect(self,mock_connect):
        e = psycopg2.Error('fail to connect')
        mock_connect.side_effect = e
        with self.assertRaises(psycopg2.Error):
            self.fox_db.connect_to_postgres()

    @patch('fox_consumer.consumer_db_support.psycopg2.connect', return_value=TestCursor())
    def test_connect_to_postgres_succeed_to_connect(self, mock_connect):
        self.fox_db.connect_to_postgres()
        self.assertTrue(mock_connect.called)
        self.assertTrue(self.fox_db.db_cursor)

    @patch('fox_consumer.consumer_db_support.PostgresWrapper.is_item_exist', return_value=True)
    def test_save_item_with_existing_item(self,  mock_is_item_exist):
        item = FoxItem("test_img", "test_main_cat", "test_type", "test_name", 10)
        sql_update = "UPDATE fox_items SET item_main_category = 'test_main_cat', item_type = 'test_type', " \
                     "item_name = 'test_name', item_price = '10' WHERE item_img_id = 'test_img'"
        self.fox_db.save_item(item)
        self.fox_db.db_cursor.execute.assert_called_with(sql_update)
        self.assertEquals(self.fox_db.postgress_connection.commit.called, True)

    @patch('fox_consumer.consumer_db_support.PostgresWrapper.is_item_exist', return_value=False)
    def test_save_item_with_not_existing_item(self, mock_is_item_exist):
        item = FoxItem("test_img", "test_main_cat", "test_type", "test_name", 10)
        sql_insert = "INSERT INTO fox_items (item_img_id, item_main_category, item_type, item_name, item_price) " \
                     "VALUES ('%(item_img_id)s', '%(item_main_category)s', '%(item_type)s', '%(item_name)s', '%(item_price)s')" \
                     %item.to_json()
        self.fox_db.save_item(item)
        self.fox_db.db_cursor.execute.assert_called_with(sql_insert)
        self.assertEquals(self.fox_db.postgress_connection.commit.called, True)

    def test_is_item_exist_with_existing_item(self):
        self.fox_db.db_cursor.fetchone.return_value = (True,1)
        item = FoxItem("test_img", "test_main_cat", "test_type", "test_name", 10)
        sql_select = "SELECT * FROM fox_items WHERE item_img_id = 'test_img';"
        is_item_exist = self.fox_db.is_item_exist(item)
        self.assertTrue(is_item_exist)
        self.fox_db.db_cursor.execute.assert_called_with(sql_select)

    def test_is_item_exist_with_not_existing_item(self):
        self.fox_db.db_cursor.fetchone.return_value = False
        item = FoxItem("test_img", "test_main_cat", "test_type", "test_name", 10)
        sql_select = "SELECT * FROM fox_items WHERE item_img_id = 'test_img';"
        is_item_exist = self.fox_db.is_item_exist(item)
        self.assertFalse(is_item_exist)
        self.fox_db.db_cursor.execute.assert_called_with(sql_select)

if __name__ == "__main__":
    unittest.main()
