from typing import Any, Generator

import psycopg2
from decouple import config


class Dataloader:
    def __init__(
        self,
        dbname: str = config("DB_NAME"),
        user: str = config("POSTGRES_USER"),
        password: str = config("POSTGRES_PASSWORD"),
        host: str = config("DB_HOST"),
        port: int = config("DB_PORT"),
    ) -> None:
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self) -> bool:
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            return True
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return False

    def disconnect(self) -> None:
        if hasattr(self, "conn"):
            self.conn.close()

    def fetch_data_in_batches(
        self, table_name: str, batch_size: int = 1000
    ) -> Generator[list[tuple[Any, ...]], None, None]:
        try:
            if not self.connect():
                raise Exception("Could not connect to the database.")
            cursor = self.conn.cursor()
            offset = 0

            while True:
                query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s"
                cursor.execute(query, (batch_size, offset))

                rows = cursor.fetchall()
                if not rows:
                    break

                yield rows
                offset += len(rows)

            cursor.close()
        except Exception as e:
            print(f"An error occurred while fetching data: {e}")
        finally:
            self.disconnect()


def process_batch(batch: list[tuple]) -> None:
    for row in batch:
        print(row)


def main() -> None:
    loader = Dataloader()

    try:
        for batch in loader.fetch_data_in_batches("balanced", batch_size=1000):
            process_batch(batch)
    except Exception as e:
        print(f"A general error occurred: {e}")


if __name__ == "__main__":
    main()
