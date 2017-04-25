import psycopg2

class FoxConsumerPostgres(object):
    def __init__(self):
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

    def close_connections(self):
        # TODO - Caduri - since it should run continuously, do we need this method?
        self.db_curser.close()
        self.postgress_connection.close()

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
