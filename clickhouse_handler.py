from clickhouse_connect import get_client

class ClickHouseHandler:
    def __init__(self, host, port, username, password, database):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.client = None

    def connect(self):
        """Establish a connection to the ClickHouse server."""
        try:
            self.client = get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database
            )
            print("Connected to ClickHouse")
        except Exception as e:
            print("Failed to connect to ClickHouse:", e)

    def disconnect(self):
        """Close the ClickHouse connection."""
        if self.client:
            self.client.close()
            print("Disconnected from ClickHouse")

    def execute_query(self, query):
        """Execute a query on the ClickHouse database."""
        if self.client:
            try:
                result = self.client.query(query).result_rows
                return result
            except Exception as e:
                print("Query execution failed:", e)
                return None
        else:
            print("Not connected to ClickHouse")
            return None