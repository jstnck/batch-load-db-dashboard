"""Defines the Database class, which interacts with Postgres"""
import time
import logging
import psycopg2
from psycopg2 import OperationalError

class Database:
    """Class for interacting with Postgres"""
    def __init__(self, db_name: str, db_host: str, db_port: str, db_user: str, db_password: str):
        """initializes Database object"""
        self._db_name = db_name
        self._db_host = db_host
        self._db_port = db_port
        self._db_user = db_user
        self._db_password = db_password
        self.create_connection(db_name, db_host, db_port, db_user, db_password)

        if self.conn is None:
            raise Exception("Connection error")
        logging.info("connection created")


    def create_connection(self, db_name, db_host, db_port, db_user, db_password) -> None:
        """Returns postgres connection object"""
        self.conn = None
        try:
            self.conn = psycopg2.connect(
                database=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port,
            )
            logging.info(f"Connection to '{db_name}' successful")
        except OperationalError as error:
            print(f"error! = {error}")
            logging.info(f"The error '{error}' occurred")

    def close_connection(self) -> None:
        """Closes database connection"""
        try:
            self.conn.close()
        except:
            time.sleep(1)
            self.conn.close()
        finally:
            raise Exception("There was an error closing the connection")


    def create_database(self, name: str) -> None:
        """Checks to see if database exists; if not creates the database"""
        self.conn.autocommit = True
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{name}'")
        exists = cursor.fetchone()
        if not exists:
            try:
                cursor.execute(f"CREATE DATABASE {name}")
                logging.info(f"Database {name} created")
            except OperationalError as error:
                logging.info(f"The error '{error}' occurred")
        else:
            logging.info(f"Database '{name}' already exists")

    def execute_query(self, query: str) -> None:
        """Executes SQL query against the database"""
        self.conn.autocommit = True
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            logging.info("Query executed successfully")
        except OperationalError as error:
            logging.info(f"The error '{error}' occurred")

    def execute_query_return(self, query: str) -> list[tuple]:
        """Executes a SQL query agains the database and
        returns the result as a list of tuples"""
        self.conn.autocommit = True
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            logging.info("Query executed successfully")
        except OperationalError as error:
            logging.info(f"The error '{error}' occurred")

        return cursor.fetchall()

    def truncate_table(self, db_schema, db_table) -> None:
        """Truncates the table db_name.db_schema.db_table"""
        sql_truncate_table = f"TRUNCATE TABLE {db_schema}.{db_table}"
        self.execute_query(sql_truncate_table)
