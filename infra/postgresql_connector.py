import psycopg2
from psycopg2 import pool
import time

class PostgresConnector:
    def __init__(self, dbname, user, password, host, port, max_retries=3, retry_delay=5):
        self.connection_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection_pool = None
        self._initialize_pool()

    def _initialize_pool(self):
        for attempt in range(self.max_retries):
            try:
                self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                    1, 20,
                    **self.connection_params
                )
                # Test the connection
                conn = self.connection_pool.getconn()
                conn.cursor().execute('SELECT 1')
                self.connection_pool.putconn(conn)
                return
            except psycopg2.Error as e:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise ConnectionError(f"Failed to connect to PostgreSQL after {self.max_retries} attempts: {str(e)}")

    def get_connection(self):
        if not self.connection_pool:
            self._initialize_pool()
        return self.connection_pool.getconn()

    def return_connection(self, conn):
        if self.connection_pool:
            self.connection_pool.putconn(conn)

    def close_all(self):
        if self.connection_pool:
            self.connection_pool.closeall()
