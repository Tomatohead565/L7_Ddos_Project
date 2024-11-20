from typing import Any, Generator

import psycopg2


class Dataloader:
    def __init__(
        self,
        dbname: str = "postgres",
        user: str = "erelis",
        password: str = "4955",
        host: str = "127.0.0.1",
        port: int = 5432,
    ):
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


def main() -> None:
    # Создание экземпляра класса Dataloader
    loader = Dataloader()

    # Подключение к базе данных
    if loader.connect():
        # Чтение данных из таблицы 'users' порциями по 50 строк
        for batch in loader.fetch_data_in_batches("balanced", batch_size=1000):
            # Обработка данных в каждом батче
            process_batch(batch)

        # Отключаемся от базы данных
        loader.disconnect()
    else:
        print("Не удалось подключиться к базе данных.")


def process_batch(batch: list[tuple]) -> None:
    # Здесь вы можете обработать данные из каждого батча
    for row in batch:
        print(row)


if __name__ == "__main__":
    main()
