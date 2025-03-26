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


def make_noize_star(point: Point, distance_normal: float, step: int, **params) -> [{LineString, Any}]:
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
    noize_limit = 50
    stars_line_step = 5
    noize_points = []

    for _, street in streets.iterrows():
        line = street.geometry
        noize = int(street['noize_day_cars'])
        noize_distance = 10 ** ((noize - noize_limit) / 10)
        noize_points += make_points_on_line_with_attr(
            linestring=line,
            interval=5,
            noize=noize,
            noize_distance=noize_distance
        )
    gdf_noize_points = gpd.GeoDataFrame(noize_points).set_crs(crs)
    noize_stars = []
    for _, noize_point in gdf_noize_points.iterrows():
        noize_stars += make_noize_star(
            point=noize_point.geometry,
            distance_normal=noize_point['noize_distance'],
            step=stars_line_step,
            start_noize=noize_point['noize']
        )
    gdf_noize_stars = gpd.GeoDataFrame(noize_stars).set_crs(crs)
    gdf_noize_stars.to_file('stars.gpkg', driver='GPKG')
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Время выполнения: {execution_time} секунд")

