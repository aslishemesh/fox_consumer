import psycopg2

class PostgresWrapper(object):
    def __init__(self):
        self.postgress_connection = None

    def runner(self):
        """
        this will be the main function for the PostgresWrapper"
        """
        self.connect_to_postgres()
        self.check_and_create_fox_table()

    def connect_to_postgres(self):
        try:
            # Connect to an existing database
            self.postgress_connection = psycopg2.connect("dbname=mydb user=aslishemesh")
            # Open a cursor to perform database operations
            self.db_cursor = self.postgress_connection.cursor()
        except psycopg2.Error as e:
            print "PostgresWrapper - connect_to_postgres - exception:"
            raise

    def check_and_create_fox_table(self):
        # verify if the table already exist
        sql_query_table = "select exists(select * from information_schema.tables where table_name='fox_items');"
        table_exist = self.execute_and_fetch_sql_query(sql_query_table)[0]
        if not table_exist:
            self.db_cursor.execute("CREATE TABLE fox_items ("
                                   "item_img_id varchar, "
                                   "item_main_category varchar, "
                                   "item_type varchar, "
                                   "item_name varchar, "
                                   "item_price real);")
            # Make the changes to the database persistent (allows us to see the DB update live in Postico)
            self.postgress_connection.commit()

    def close_connections(self):
        if self.postgress_connection and not self.postgress_connection.closed:
            self.postgress_connection.close()

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
        except psycopg2.OperationalError as e:
            # do not raise - fix it locally
            print "PostgresWrapper - execute_and_commit - OperationalError exception:"
            print e
            self.restart_connection()
            print "Error - last query did not applied !"
        except psycopg2.Error as e:
            self.handle_postgres_general_exception(e, "execute_and_commit")

    def execute_and_fetch_sql_query(self, sql_query):
        """
        this function will receive an sql query, execute it and return its answer
        :param sql_query: string contain sql query
        :return: sql response
        """
        try:
            self.db_cursor.execute(sql_query)
            return self.db_cursor.fetchone()
        except psycopg2.OperationalError as e:
            # do not raise - just print "specifc problem and continue
            print "PostgresWrapper - execute_and_fetch_sql_query - OperationalError exception:"
            print e
            self.restart_connection()
            print "Error - last query did not applied !"
        except psycopg2.Error as e:
            self.handle_postgres_general_exception(e, "execute_and_fetch_sql_query")

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

    def restart_connection(self):
        """
        restart connection (while the program is running)
        """
        self.close_connections()
        self.connect_to_postgres()

    def handle_postgres_general_exception(self, e, location):
        """
        handle general exception for postgres
        :param e: current exception
        :param location: source method the exception has thrown
        """
        if e.pgcode == '42P01':

            
            print "PostgresWrapper - execute_and_commit - exception - 'undefined_table'"
            print "Reconnecting and creating table..."
            self.restart_connection()
            self.check_and_create_fox_table()
            print "Info - last query did not applied !"
        else:
            print "PostgresWrapper - execute_and_commit - exception:"
            print e
            raise
