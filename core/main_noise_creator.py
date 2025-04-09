import time
import geopandas as gpd
import pandas as pd
from core.geom_transform import (
    polygons_to_segments,
    segmentation_of_barrier_by_floors
)
from core.stars_maker import make_noise_stars
from core.reflection import make_noise_reflection
from core.db_connect import (
    engine,
    mark_a_street_as_processed,
    delete_duplicates_barriers
)
from config import (
    schema,
    base_crs,
    noise_limit,
    point_interval,
    stars_line_step,
    geometry_column,
    street_table_name,
    noise_level_column,
    building_table_name,
    building_level_column,
    noise_lines_table_name,
    barrier_noise_table_name
)


def create_noise(streets: gpd.GeoDataFrame, buildings: gpd.GeoDataFrame):
    start_time = time.time()

    noise_stars = make_noise_stars(
        street_layer=streets,
        stars_line_step=stars_line_step,
        noise_limit=noise_limit,
        point_interval=point_interval
    )

    building_max_level = buildings[building_level_column].max()

    noise_stars = noise_stars[noise_stars[noise_level_column] /
                              3 <= building_max_level]

    start = time.time()
    print('начинаю искать пересечения', len(noise_stars), len(buildings))

    intersect_noise_lines = gpd.sjoin(
        noise_stars,
        buildings,
        how="inner",
        predicate='intersects'
    )

    intersect_buildings = gpd.sjoin(
        buildings,
        noise_stars,
        how="inner",
        predicate='intersects'
    ).drop_duplicates(subset=geometry_column)

    end = time.time()
    print('закончил за ', end - start)

    intersect_noise_lines = intersect_noise_lines[
        (intersect_noise_lines[noise_level_column] / 3).astype(int) <=
        intersect_noise_lines[building_level_column]
        ].drop_duplicates(subset=geometry_column)

    non_intersect = noise_stars[
        ~noise_stars.index.isin(intersect_noise_lines.index)
    ]

    building_segments = polygons_to_segments(intersect_buildings)
    building_segments = segmentation_of_barrier_by_floors(building_segments)

    noise_lines, noise_barriers = make_noise_reflection(
        noize=intersect_noise_lines,
        barriers=building_segments
    )
    noise_lines = gpd.GeoDataFrame(
        pd.concat([non_intersect, noise_lines], ignore_index=True),
        crs=noise_stars.crs
    )

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Время выполнения: {execution_time} секунд")
    return noise_lines, noise_barriers


def save_to_postgis(gdf: gpd.GeoDataFrame, name):
    gdf.to_postgis(
        name=name,
        schema=schema,
        con=engine,
        if_exists='append',
        index=True,
        dtype={geometry_column: f'GEOMETRY(LINESTRING, {base_crs})'}
    )


def noise_maker(count_streets_update: int):
    i = 0
    while i != count_streets_update:
        streets = gpd.read_postgis(
            con=engine,
            crs=base_crs,
            geom_col=geometry_column,
            sql=f'''SELECT * FROM {schema}.{street_table_name} WHERE "highway" 
            IN ('living_street', 'trunk', 'trunk_link', 'primary', 
            'primary_link', 'secondary', 'secondary_link', 'tertiary', 
            'tertiary_link', 'unclassified', 'residential') 
            AND finished is not True ORDER BY id ASC LIMIT 1'''
            )
        street = streets.iloc[[0]].squeeze()
        buildings = gpd.read_postgis(
            con=engine,
            crs=base_crs,
            geom_col=geometry_column,
            sql=f'SELECT * FROM {schema}.{building_table_name}'
            )
        print('-----------------------------------')
        street_id = int(street['id'])
        print(street['name'], street_id)
        street = gpd.GeoDataFrame(
            [street.to_dict()],
            crs=streets.crs,
            geometry=geometry_column)

        noise_lines, noise_barrier = create_noise(street, buildings)
        save_to_postgis(noise_lines, noise_lines_table_name)
        save_to_postgis(noise_barrier, barrier_noise_table_name)
        mark_a_street_as_processed(street_id)
        delete_duplicates_barriers()
        print('-----------------------------------')
        i += 1
        print(f'готово {i} из {count_streets_update}')
    print('готово')


if __name__ == '__main__':
    noise_maker(100)
