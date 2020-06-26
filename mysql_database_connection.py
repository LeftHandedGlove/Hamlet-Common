import time
import atexit
import mysql.connector
import yaml
import os

from hamlet_common.python_utils import print_msg

class MySQLDatabaseConnection:
    def __init__(self):
        self.__connection = None
        self.__db_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "database_outline.yaml"))
        with open(self.__db_config_path) as yaml_file:
            db_config_data = yaml.load(yaml_file, Loader=yaml.FullLoader)
        self.__database = db_config_data['name']
        self.__address = db_config_data['address']
        self.__username = db_config_data['username']
        self.__password = db_config_data['password']
        self.__known_tables = db_config_data['tables']
        self.__cursor = None

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
        self.__cursor = self.__connection.cursor()
        atexit.register(self.close_connection)

    def close_connection(self):
        print_msg("Closing connection to database")
        self.__connection.close()
        self.__connection = None
        self.__cursor = None
        atexit.unregister(self.close_connection)

    def command(self, sql_query):
        self.__cursor.execute(sql_query)
        try:
            result = self.__cursor.fetchall()
        except mysql.connector.errors.InterfaceError:
            # The command didn't return any data
            result = None
        self.__connection.commit()
        return result

    def create_table(self, table):
        # Check for table in known tables
        if table in self.__known_tables.keys():
            # Generate the sql query to create the table
            table_column_dict = self.__known_tables[table]['columns']
            create_table_sql = "CREATE TABLE {0}(".format(table)
            for col_name, col_sql_str in table_column_dict.items():
                create_table_sql += "{0} {1},".format(col_name, col_sql_str)
            create_table_sql = create_table_sql[:-1] + ")"
            self.command(create_table_sql)
        else:
            print_msg("Table '{0}' not in dict of known tables".format(table))
            raise Exception

    def drop_table(self, table):
        try:
            self.__cursor.execute("DROP TABLE {0}".format(table))
        except mysql.connector.errors.ProgrammingError:
            print_msg("Unable to drop table '{0}', it might not exist.".format(table))
        except Exception as e:
            print_msg("Something went wrong when dropping table '{0}': {1}".format(table, e))
