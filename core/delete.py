from db_connect import engine
from sqlalchemy import text, inspect
from tqdm import tqdm


def batch_delete():
    # Разделяем схему и таблицу
    schema, table = TABLE_NAME.split('.') if '.' in TABLE_NAME else (
    None, TABLE_NAME)

    inspector = inspect(engine)

    # Получаем список таблиц с учетом схемы
    tables = inspector.get_table_names(
        schema=schema) if schema else inspector.get_table_names()

    if table not in tables:
        raise ValueError(f"Таблица {TABLE_NAME} не существует")

    # Получаем колонки с указанием схемы
    columns = [col['name'] for col in
               inspector.get_columns(table, schema=schema)]
    if COLUMN_NAME not in columns:
        raise ValueError(
            f"Колонка {COLUMN_NAME} не существует в таблице {TABLE_NAME}")

    with engine.begin() as conn:
        # Получаем общее количество строк для удаления
        total_count = conn.execute(
            text(
                f'SELECT COUNT(*) FROM {TABLE_NAME} WHERE "{COLUMN_NAME}" > 80')
        ).scalar()

        with tqdm(total=total_count, desc="Удаление строк") as pbar:
            while True:
                # Для PostgreSQL важно экранировать имена
                result = conn.execute(
                    text(f"""
                        WITH batch AS (
                            SELECT id_ 
                            FROM {TABLE_NAME} 
                            WHERE "{COLUMN_NAME}" > 80
                            LIMIT {BATCH_SIZE}
                            FOR UPDATE SKIP LOCKED
                        )
                        DELETE FROM {TABLE_NAME} 
                        WHERE id_ IN (SELECT id_ FROM batch)
                        RETURNING id_
                    """)
                )

                deleted = result.rowcount
                if deleted == 0:
                    break

                pbar.update(deleted)


if __name__ == "__main__":
    TABLE_NAME = "moscow.noise_lines"  # Формат: schema.table_name
    COLUMN_NAME = "level"  # Имя колонки для условия
    BATCH_SIZE = 100000  # Размер пакета

    batch_delete()
