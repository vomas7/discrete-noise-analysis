from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Table, MetaData, update, text
from config import (
    schema,
    db_name,
    geometry_column,
    street_table_name,
    building_level_column,
    barrier_noise_table_name,
    barrier_noise_level_column
)
engine = create_engine(
    f'postgresql://ilya:UiojK(*)1@192.168.1.99:5432/{db_name}'
)


def mark_a_street_as_processed(street_id: int):
    Session = sessionmaker(bind=engine)
    session = Session()
    metadata = MetaData()
    street = Table(
        street_table_name, metadata, autoload_with=engine, schema=schema
    )

    update_stmt = (
        update(street).where(
            street.c.id == street_id).values(finished=True)
    )
    session.execute(update_stmt)
    session.commit()
    session.close()


def delete_duplicates_barriers():
    print('удаляю дубли')
    with engine.begin() as connection:
        connection.execute(text(f"""
        WITH duplicates AS (
            SELECT 
                {geometry_column},
                {building_level_column},
                MAX({barrier_noise_level_column}) AS max_value
            FROM 
                {schema}.{barrier_noise_table_name}
            GROUP BY 
                {geometry_column}, {building_level_column}
            HAVING 
                COUNT(*) > 1
        ),
        
        records_to_keep AS (
            SELECT 
                DISTINCT ON ({geometry_column}, {building_level_column}) 
                id
            FROM 
                {schema}.{barrier_noise_table_name}
            WHERE 
                ({geometry_column}, {building_level_column}) IN (
                    SELECT {geometry_column}, {building_level_column}
                     FROM duplicates
                )
            ORDER BY 
                {geometry_column}, 
                {building_level_column}, 
                {barrier_noise_level_column} DESC
        )
        
        DELETE FROM 
            {schema}.{barrier_noise_table_name}
        WHERE 
            ({geometry_column}, {building_level_column}) IN (
                SELECT {geometry_column}, {building_level_column} 
                FROM duplicates
            )
            AND id NOT IN (
                SELECT id FROM records_to_keep
            );
        """))
