from tqdm import tqdm
from math import log10
from os import cpu_count
from pandas import Series
from multiprocessing import Pool
from geopandas import GeoDataFrame
from pyproj import Geod, Transformer
from typing import Tuple, List, Dict, Optional
from shapely.geometry import Point, LineString

from core.geom_transform import check_geomtype
from config import (
    base_crs,
    geometry_column,
    noise_level_column,
    building_level_column,
    amount_of_reflections,
    barrier_noise_level_column
)

# Constants
MAX_WORKERS = min(cpu_count() or 4, 32)
geoid = Geod(ellps='WGS84')
transformer = Transformer.from_crs(
    crs_from=f"EPSG:{base_crs}",
    crs_to="EPSG:4326",
    always_xy=True
)


def make_noise_reflection(
        noize: GeoDataFrame,
        barriers: GeoDataFrame
) -> Tuple[GeoDataFrame, GeoDataFrame]:
    """Main function to process noise reflections with parallel processing"""
    print('make noise reflection')
    print(f'🚀 Processing with {MAX_WORKERS} cores')

    chunk_size = max(len(noize) // (MAX_WORKERS * 2), 1)
    chunks = [
        noize.iloc[i:i + chunk_size]
        for i in range(0, len(noize), chunk_size)
    ]
    args = [(chunk, barriers) for chunk in chunks]

    with Pool(processes=MAX_WORKERS - 4) as pool:
        with tqdm(total=len(chunks), desc="Processing chunks",
                  unit="chunk") as pbar:
            results = []
            for chunk_result in pool.imap_unordered(process_chunk, args):
                results.append(chunk_result)
                pbar.update(1)

    lines_results = []
    barriers_results = []
    for lines, _barriers in results:
        lines_results.extend(lines)
        barriers_results.extend(_barriers)

    if lines_results:
        noize_lines = GeoDataFrame(lines_results, crs=noize.crs)
    else:
        noize_lines = GeoDataFrame(geometry=[], crs=noize.crs)

    if barriers_results:
        barriers_gdf = GeoDataFrame(barriers_results, crs=barriers.crs)
        result = barriers_gdf.groupby(
            [geometry_column, building_level_column], as_index=False).agg(
            noise_level=(barrier_noise_level_column, 'max'))
        barriers_gdf = GeoDataFrame(result, crs=barriers.crs)
    else:
        barriers_gdf = GeoDataFrame(geometry=[], crs=barriers.crs)

    return noize_lines, barriers_gdf


def process_chunk(
        args: Tuple[GeoDataFrame, GeoDataFrame]
) -> Tuple[List[Dict], List[Dict]]:
    """Process a chunk of noise lines with barriers"""
    chunk, barriers = args
    lines_results = []
    barriers_results = []

    for _, row in chunk.iterrows():
        noise_list, barriers_list = process_noize_line(row, barriers)
        lines_results.extend(
            [noise_line.to_dict() for noise_line in noise_list]
        )
        barriers_results.extend(
            [barrier.to_dict() for barrier in barriers_list]
        )

    return lines_results, barriers_results


def process_noize_line(noize_line: Series, barriers: GeoDataFrame) -> \
        Tuple[List[Series], List[Series]]:
    """Process single noise line with reflections"""
    inter_barriers = []
    noize_lines = []
    intersect_barrier = get_intersect_barrier(noize_line, barriers)

    if intersect_barrier.empty:
        return noize_lines, inter_barriers

    closest_line = find_near_line(noize_line.geometry, intersect_barrier)

    for _ in range(amount_of_reflections):
        if closest_line is None or intersect_barrier.empty:
            break

        noize_line, barrier = get_line_reflect(noize_line, closest_line)
        if barrier is None:
            break

        inter_barriers.append(barrier)
        intersect_barrier = get_intersect_barrier(noize_line, barriers)
        closest_line = find_near_line(noize_line.geometry, intersect_barrier)
    noize_lines.append(noize_line)
    return noize_lines, inter_barriers


def get_line_reflect(
        noise: Series,
        barrier: Series
) -> Tuple[Optional[Series], Optional[Series]]:
    """Calculate noise reflection and return updated noise line and barrier"""
    noise_geom = noise.geometry
    barrier_geom = barrier.geometry

    last_segment = LineString([noise_geom.coords[-2], noise_geom.coords[-1]])
    intersection = last_segment.intersection(barrier_geom)

    if intersection.is_empty or not isinstance(intersection, Point):
        return noise, None

    # Calculate reflection
    x1, y1 = barrier_geom.coords[0]
    x2, y2 = barrier_geom.coords[1]
    dx, dy = x2 - x1, y2 - y1

    if dx == 0:  # Vertical line
        reflected_x = 2 * x1 - noise_geom.coords[-1][0]
        reflected_y = noise_geom.coords[-1][1]
    else:
        m = dy / dx
        c = y1 - m * x1
        x, y = noise_geom.coords[-1]
        d = (x + (y - c) * m) / (1 + m ** 2)
        reflected_x = 2 * d - x
        reflected_y = 2 * d * m - y + 2 * c

    new_coords = [*noise_geom.coords[:-1], (intersection.x, intersection.y),
                  (reflected_x, reflected_y)]
    len_initial = calculate_geodesic_length(new_coords[:-1])

    noise_level = noise['start_noise'] - (
        10 * log10((len_initial ** 2 + noise[noise_level_column] ** 2) ** 0.5)
    )

    reflected_noise = noise.copy()
    reflected_noise[geometry_column] = LineString(new_coords)

    reflected_barrier = barrier.copy()
    reflected_barrier[barrier_noise_level_column] = noise_level

    return reflected_noise, reflected_barrier


def calculate_geodesic_length(coords: List[Tuple[float, float]]) -> float:
    """Calculate geodesic length for coordinates in EPSG:3857"""
    total_length = 0.0
    for i in range(len(coords) - 1):
        lon1, lat1 = transformer.transform(*coords[i])
        lon2, lat2 = transformer.transform(*coords[i + 1])
        _, _, dist = geoid.inv(lon1, lat1, lon2, lat2)
        total_length += abs(dist)
    return total_length


def find_near_line(
        line: LineString,
        target_lines: GeoDataFrame
) -> Optional[Series]:
    """Find the nearest line from target lines"""
    if not check_geomtype(target_lines, 'LineString'):
        raise ValueError('Expected LineString geometry type')

    first_noise_point = Point(line.coords[-2])
    min_distance = float('inf')
    closest_line = None

    for _, target_line in target_lines.iterrows():
        intersection = line.intersection(target_line.geometry)
        if intersection.is_empty:
            continue

        distance = first_noise_point.distance(intersection)
        if 0.1 <= distance < min_distance:
            min_distance = distance
            closest_line = target_line

    return closest_line


def get_intersect_barrier(noize_line: Series,
                          barriers: GeoDataFrame) -> GeoDataFrame:
    """Find intersecting barriers with filtering by level."""

    level_mask = (barriers[building_level_column] == (
                noize_line[noise_level_column] / 3))
    filtered = barriers[level_mask]

    if filtered.empty:
        return filtered

    geom = noize_line.geometry
    possible_matches_index = list(filtered.sindex.intersection(geom.bounds))
    possible_matches = filtered.iloc[possible_matches_index]
    return possible_matches[possible_matches.intersects(geom)]
