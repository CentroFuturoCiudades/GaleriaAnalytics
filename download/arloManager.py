import psycopg2
from psycopg2 import sql
import logging

class ArloManager:
    def __init__(self, postgres_config: dict):
        """
        Initialize the ArloManager.

        Args:
            postgres_config (dict): Configuration for the PostgreSQL connection.
                {
                    'dbname': 'galeria',
                    'user': 'postgres',
                    'password': 'new_password',
                    'host': 'localhost',
                    'port': '5433'
                }
        """
        self.postgres_config = postgres_config
        self.postgres_conn = None
        logging.basicConfig(level=logging.INFO)

    def connect_to_databases(self):
        """Establish connections to PostgreSQL."""
        try:
            # Connect to PostgreSQL
            logging.info("Connecting to PostgreSQL database...")
            self.postgres_conn = psycopg2.connect(**self.postgres_config)
            logging.info("Connected to PostgreSQL database.")
        except Exception as e:
            logging.error(f"Error connecting to databases: {e}")
            raise

    def close_connections(self):
        """Close connection to PostgreSQL."""
        if self.postgres_conn:
            self.postgres_conn.close()
            logging.info("Closed PostgreSQL connection.")

    def transfer_data(self, table_name: str):
        """
        Transfer data from one PostgreSQL table to another PostgreSQL table.

        Args:
            table_name (str): Name of the table to transfer.
        """
        try:
            # Ensure connection is open
            if not self.postgres_conn:
                raise ConnectionError("Database is not connected. Call connect_to_databases() first.")

            # Fetch data from the source PostgreSQL table
            postgres_cursor = self.postgres_conn.cursor()
            postgres_cursor.execute(f"SELECT * FROM {table_name}")
            rows = postgres_cursor.fetchall()

            # Get column names from the table
            column_names = [desc[0] for desc in postgres_cursor.description]

            # Create the table in PostgreSQL if it doesn't exist
            create_table_query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {columns}
                )
            """).format(
                table_name=sql.Identifier(table_name),
                columns=sql.SQL(", ").join(
                    sql.SQL("{} {}").format(
                        sql.Identifier(column),
                        sql.SQL("TEXT")  # Adjust type if necessary
                    ) for column in column_names
                )
            )
            postgres_cursor.execute(create_table_query)

            # Insert data into PostgreSQL (same table or another if necessary)
            insert_query = sql.SQL("""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders})
                ON CONFLICT (id) DO NOTHING
            """).format(
                table_name=sql.Identifier(table_name),
                columns=sql.SQL(", ").join(map(sql.Identifier, column_names)),
                placeholders=sql.SQL(", ").join(sql.Placeholder() * len(column_names))
            )
            for row in rows:
                postgres_cursor.execute(insert_query, row)

            # Commit the transaction
            self.postgres_conn.commit()
            logging.info(f"Transferred {len(rows)} rows in PostgreSQL table '{table_name}'.")

        except Exception as e:
            if self.postgres_conn:
                self.postgres_conn.rollback()
            logging.error(f"Error transferring data: {e}")
            raise

# Example Usage
if __name__ == "__main__":
    # PostgreSQL configuration
    postgres_config = {
        'dbname': 'galeria',  # Your PostgreSQL database name
        'user': 'postgres',   # Your PostgreSQL username
        'password': 'new_password',  # Your PostgreSQL password
        'host': 'localhost',  # PostgreSQL host (localhost if local)
        'port': '5433'        # PostgreSQL port
    }

    # Initialize the ArloManager
    arlo_manager = ArloManager(postgres_config)

    try:
        # Connect to the PostgreSQL database
        arlo_manager.connect_to_databases()

        # Transfer data from the PostgreSQL tables 'tracks' and 'video_recorded'
        arlo_manager.transfer_data(table_name="tracks")
        # O también:
        # arlo_manager.transfer_data(table_name="video_recorded")

    finally:
        # Close database connection
        arlo_manager.close_connections()

