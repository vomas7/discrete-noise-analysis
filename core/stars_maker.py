import math
import time
import numpy as np
from typing import Any
import geopandas as gpd
from shapely.geometry import LineString, Point


def make_points_on_line_with_attr(
        linestring: LineString, interval: int, **params
) -> [Point]:
    length = linestring.length
    distances = np.arange(0, length, interval)
    points = [
        {
            'geometry': linestring.interpolate(distance),
            **params
        } for distance in distances
    ]
    return points


def create_point_from_angle_distance(
        start_point: Point,
        distance: float,
        angle_degrees: float
) -> Point:
    angle_radians = math.radians(angle_degrees)
    new_x = start_point.x + distance * math.cos(angle_radians)
    new_y = start_point.y + distance * math.sin(angle_radians)
    return Point(new_x, new_y)


def make_noise_star(point: Point, distance_normal: float, step: int, **params) -> [{LineString, Any}]:
    star_lines = []
    for level in range(0, int(distance_normal), 3):
        distance = ((distance_normal ** 2) - (level ** 2)) ** 0.5
        for angle in range(20, 380, step):
            new_point = create_point_from_angle_distance(point, distance, angle)
            star_lines.append(
                {
                    'geometry': LineString([point, new_point]),
                    **params
                }
            )
    return star_lines


if __name__ == '__main__':
    start_time = time.time()
    streets = gpd.read_file('street_3857.gpkg')
    crs = streets.crs
    noise_limit = 50
    stars_line_step = 5
    noise_points = []

    for _, street in streets.iterrows():
        line = street.geometry
        noise = int(street['noize_day_cars'])
        noise_distance = 10 ** ((noise - noise_limit) / 10)
        noise_points += make_points_on_line_with_attr(
            linestring=line,
            interval=5,
            noise=noise,
            noise_distance=noise_distance
        )
    gdf_noise_points = gpd.GeoDataFrame(noise_points).set_crs(crs)
    noise_stars = []
    for _, noise_point in gdf_noise_points.iterrows():
        noise_stars += make_noise_star(
            point=noise_point.geometry,
            distance_normal=noise_point['noise_distance'],
            step=stars_line_step,
            start_noise=noise_point['noise']
        )
    gdf_noise_stars = gpd.GeoDataFrame(noise_stars).set_crs(crs)
    gdf_noise_stars.to_file('stars.gpkg', driver='GPKG')
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Время выполнения: {execution_time} секунд")

