import psycopg2

class PostgresWrapper(object):
    def __init__(self):
        """
        This function will initialize postgress database connection.
        it will connect to "mtdb" database and check if the "fox_item" table exist, id not it will create it.
        """
        try:
            # Connect to an existing database
            self.postgress_connection = psycopg2.connect("dbname=mydb user=aslishemesh")
            # Open a cursor to perform database operations
            self.db_cursor = self.postgress_connection.cursor()
        except:
            #self.close_connections("Cannot connect to postgres DB")
            raise
        # verify if the table already exist
        sql_query_table = "select exists(select * from information_schema.tables where table_name='fox_items');"
        table_exist = self.execute_and_fetch_sql_query(sql_query_table)[0]
        if not table_exist:
            sql_query = "CREATE TABLE fox_items (" \
                                    "item_img_id varchar, "\
                                    "item_main_category varchar, "\
                                    "item_type varchar, "\
                                    "item_name varchar, "\
                                    "item_price real);"
            # Make the changes to the database persistent (allows us to see the DB update live in Postico)
            self.execute_and_commit(sql_query)

    def close_connections(self):
        try:
            self.postgress_connection.close()
        except:
            print " cannot close postgres connection"

    def save_item(self, item):
        """
        This function will save the data to the DB.
        (if the data already exist it will update it)
        :param item: FoxItem object
        """
        if self.is_item_exist(item):
            sql_query = "UPDATE fox_items " \
                        "SET item_main_category = '%s', item_type = '%s', item_name = '%s', item_price = '%s' " \
                        "WHERE item_img_id = '%s'" \
                        % (item.item_main_category, item.item_type, item.item_name, item.item_price, item.item_img_id)
        else:
            sql_query = "INSERT INTO fox_items (item_img_id, item_main_category, item_type, item_name, item_price) " \
                         "VALUES ('%s', '%s', '%s', '%s', '%s')" \
                        % (item.item_img_id, item.item_main_category, item.item_type, item.item_name, item.item_price)
        self.execute_and_commit(sql_query)

    def execute_and_commit(self, sql_query):
        """
        this function will receive an sql query execute it and commit the DB  
        :param sql_query: string contain sql query
        """
        try:
            self.db_cursor.execute(sql_query)
            self.postgress_connection.commit()
        except:
            self.close_connections("postgres exception")
    def execute_and_fetch_sql_query(self, sql_query):
        """
        this function will receive an sql query, execute it and return its answer
        :param sql_query: string contain sql query
        :return: sql response
        """
        try:
            self.db_cursor.execute(sql_query)
            return self.db_cursor.fetchone()
        except:
            self.close_connections("postgres exception")

    def is_item_exist(self, item):
        """
        This function will verify if the item exist in DB
        (using the primary-key - image url (item_image_id)
        :param item: FoxItem
        :return: True/False (exist/not)
        """
        sql_query = "SELECT * FROM fox_items WHERE item_img_id = '%s';" % item.item_img_id
        item_exist = self.execute_and_fetch_sql_query(sql_query)
        return True if item_exist else False


