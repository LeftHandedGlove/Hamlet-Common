import time
import atexit
import mysql.connector

from python_utils import print_msg

class MySQLDatabaseConnection:
    def __init__(self, address, username, password, database):
        self.__connection = None
        self.__address = address
        self.__username = username
        self.__password = password
        self.__database = database
        self.cursor = None

    def open_connection(self, timeout=60):
        print_msg("Opening connection to database...")
        print_msg("  Database: {0}".format(self.__database))
        print_msg("  Address:  {0}".format(self.__address))
        print_msg("  User:     {0}".format(self.__username))
        print_msg("  Password: {0}".format(self.__password))
        start_time = time.time()
        while True:
            if time.time() - start_time > timeout:
                print_msg("Timed out connecting to database")
                raise TimeoutError
            try:
                self.__connection = mysql.connector.connect(
                    host=self.__address,
                    user=self.__username,
                    password=self.__password,
                    database=self.__database
                )
                break
            except:
                print_msg("Failed to connect to database. Trying again...")
                time.sleep(1)
        print_msg("Connected to database")
        self.cursor = self.__connection.cursor()
        atexit.register(self.close_connection)

    def close_connection(self):
        print_msg("Closing connection to database")
        self.__connection.close()
        self.__connection = None
        self.cursor = None
        atexit.unregister(self.close_connection)

    def command(self, sql_query):
        self.cursor.execute(sql_query)
        try:
            result = self.cursor.fetchall()
        except mysql.connector.errors.InterfaceError:
            # The command didn't return any data
            result = None
        return result

    def create_table(self, table_name, column_config_dict):
        create_table_sql = "CREATE TABLE {0}(".format(table_name)
        for col_name, col_sql_str in column_config_dict.items():
            create_table_sql += "{0} {1},".format(col_name, col_sql_str)
        create_table_sql = create_table_sql[:-1] + ")"
        self.cursor.execute(create_table_sql)

    def drop_table(self, table_name):
        self.cursor.execute("DROP TABLE {0}".format(table_name))