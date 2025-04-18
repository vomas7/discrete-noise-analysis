import math
import numpy as np
from tqdm import tqdm
import geopandas as gpd
from typing import List
from multiprocessing import Pool, cpu_count
from shapely.geometry import LineString, Point
from config import noise_level_column, street_column_noise, geometry_column


def make_noise_stars(
        street_layer: gpd.GeoDataFrame,
        noise_limit: int,
        point_interval: int,
        stars_line_step: int
) -> gpd.GeoDataFrame:
    crs = street_layer.crs

    noise_points = []
    for _, street in tqdm(street_layer.iterrows(),
                          total=len(street_layer),
                          desc="Creating noise points"):
        line = street.geometry
        noise = int(street[street_column_noise])
        noise_distance = 10 ** ((noise - noise_limit) / 10)
        noise_points += make_points_on_line_with_attr(
            linestring=line,
            interval=point_interval,
            noise=noise,
            noise_distance=noise_distance
        )

    with Pool(cpu_count()) as pool:
        star_args = [(point[geometry_column],
                      point['noise_distance'],
                      stars_line_step,
                      point['noise'])
                     for point in noise_points]

        noise_stars = list(tqdm(pool.imap(make_noise_star_wrapper, star_args),
                                total=len(star_args),
                                desc="Generating noise stars"))

    noise_stars = [item for sublist in noise_stars for item in sublist]
    return gpd.GeoDataFrame(noise_stars).set_crs(crs)


def make_noise_star_wrapper(args):
    point, distance, step, noise = args
    return make_noise_star(
        point=point,
        distance_normal=distance,
        step=step,
        start_noise=noise
    )


def make_points_on_line_with_attr(
        linestring: LineString, interval: int, **params
) -> List[dict]:
    length = linestring.length
    distances = np.arange(3, length, interval)
    return [{
        geometry_column: linestring.interpolate(distance),
        **params
    } for distance in distances]


def create_point_from_angle_distance(
        start_point: Point,
        distance: float,
        angle_degrees: float
) -> Point:
    angle_radians = math.radians(angle_degrees)
    new_x = start_point.x + distance * math.cos(angle_radians)
    new_y = start_point.y + distance * math.sin(angle_radians)
    return Point(new_x, new_y)


def make_noise_star(
        point: Point,
        distance_normal: float,
        step: int,
        **params
) -> List[dict]:
    star_lines = []
    for level in range(0, int(distance_normal), 3):
        distance = ((distance_normal ** 2) - (level ** 2)) ** 0.5
        for angle in range(20, 380, step):
            new_point = create_point_from_angle_distance(
                start_point=point,
                distance=distance,
                angle_degrees=angle
            )
            star_lines.append({
                geometry_column: LineString([point, new_point]),
                noise_level_column: level,
                'angle': angle,
                **params
            })
    return star_lines
