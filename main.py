import time
import geopandas as gpd
import pandas as pd
from core.geom_transform import (
    polygons_to_segments,
    segmentation_of_barrier_by_floors
)
from core.stars_maker import make_noise_stars
from core.reflection import make_noise_reflection
from config import (
    noise_limit,
    point_interval,
    stars_line_step
)

if __name__ == '__main__':
    start_time = time.time()

    streets = gpd.read_file('core/street_3857.gpkg')
    crs = streets.crs

    noise_stars = make_noise_stars(
        street_layer=streets,
        stars_line_step=stars_line_step,
        noise_limit=noise_limit,
        point_interval=point_interval
    )

    buildings = gpd.read_file('core/Здания_3857.gpkg')
    building_segments = polygons_to_segments(buildings)
    building_segments = segmentation_of_barrier_by_floors(building_segments)

    intersect_noise_lines = gpd.sjoin(
        noise_stars,
        building_segments,
        how="inner",
        predicate='intersects'
    )

    intersect_noise_lines = intersect_noise_lines[
        (intersect_noise_lines['level'] / 3).astype(int) ==
        intersect_noise_lines['et']
        ].drop_duplicates(subset='geometry')

    non_intersect = noise_stars[
        ~noise_stars.index.isin(intersect_noise_lines.index)
    ]

    noise_lines, barriers = make_noise_reflection(
        noize=intersect_noise_lines,
        barriers=building_segments
    )
    final_result = gpd.GeoDataFrame(
        pd.concat([non_intersect, noise_lines], ignore_index=True),
        crs=noise_stars.crs
    )

    # final_result.to_file('noise_lines.gpkg', driver='GPKG')
    # barriers.to_file('barrier_noise.gpkg', driver='GPKG')
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Время выполнения: {execution_time} секунд")
