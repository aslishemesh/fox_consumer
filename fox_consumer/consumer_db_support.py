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
        """
        connect to postgress using "mydb" DB and "aslishemesh" user
        """
        try:
            self.postgress_connection = psycopg2.connect("dbname=mydb user=aslishemesh")
            self.db_cursor = self.postgress_connection.cursor()
        except psycopg2.Error as e:
            # ---Caduri--- - its not part of handle_exception method since
            # I want it to be clear that the problem was with the postgres connection
            # and yes I know - after your approve (or not) I will delete it :)
            print "PostgresWrapper - connect_to_postgres - exception:"
            raise

    def check_and_create_fox_table(self):
        """
        check if "fox_table" table exist and if not create it
        """
        # verify if the table already exist
        sql_query_table = "select exists(select * from information_schema.tables where table_name='fox_items');"
        table_exist = self.is_exist_execute_and_fetch_query(sql_query_table)
        if not table_exist:
            sql_query_table = "CREATE TABLE fox_items (item_img_id varchar, item_main_category varchar, " \
                              "item_type varchar, item_name varchar, item_price real);"
            self.execute_and_commit(sql_query_table)

    def close_connection(self):
        """
        close connection to postgres
        """
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
        except Exception as e:
            self.handle_postgres_general_exception(e)

    def is_exist_execute_and_fetch_query(self, sql_query):
        """
        this function will receive an sql query, execute it and return its answer
        (use case examples - if item exist, it table exist etc.)
        :param sql_query: string contain sql query
        :return: True/False - was the query was successful
        """
        try:
            self.db_cursor.execute(sql_query)
            is_exist = self.db_cursor.fetchone()
            if is_exist:    # check for table existence
                is_exist = is_exist[0]
            return True if is_exist else False
        except Exception as e:
            self.handle_postgres_general_exception(e)

    def is_item_exist(self, item):
        """
        This function will verify if the item exist in DB
        (using the primary-key - image url (item_image_id)
        :param item: FoxItem
        :return: True/False (exist/not)
        """
        sql_query = "SELECT * FROM fox_items WHERE item_img_id = '%s';" % item.item_img_id
        item_exist = self.is_exist_execute_and_fetch_query(sql_query)
        return item_exist

    def restart_connection(self):
        """
        restart connection (while the program is running)
        """
        self.close_connection()
        self.connect_to_postgres()

    def handle_postgres_general_exception(self, e):
        """
        handle general exception for postgres
        :param e: current exception
        :param location: source method the exception has thrown
        """

        if isinstance(e, psycopg2.OperationalError):
            # do not raise - fix it locally
            print "PostgresWrapper - OperationalError exception:"
            self.restart_connection()
            print "Error - last query did not applied !"
        elif isinstance(e, psycopg2.ProgrammingError):
            if e.pgcode == '42P01':
                # do not raise - fix it locally - create a new table (need to restart the connection!)
                print "PostgresWrapper - exception - 'unidentified_table'"
                print "Reconnecting and creating table..."
                self.restart_connection()
                self.check_and_create_fox_table()
                print "Error - last query did not applied !"
            else:
                print "PostgresWrapper - ProgrammingError - exception:"
                raise
        elif isinstance(e, psycopg2.Error):
            print "PostgresWrapper - Error - exception:"
            raise
