import os
from math import log10
import multiprocessing
from typing import Tuple, List, Dict, Optional
from tqdm import tqdm
import numpy as np

import geopandas as gpd
from pandas import Series
from geopandas import GeoDataFrame, sjoin
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points
from core.geom_transform import check_geomtype
from config import (
    noise_level_column,
    building_level_column,
    amount_of_reflections
)

# Настройки для максимальной производительности
MAX_WORKERS = min(os.cpu_count() or 4,
                  32)  # Ограничиваем максимальное количество workers
CHUNKSIZE = 250  # Оптимальный размер чанка для баланса нагрузки


def process_chunk(args: Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, str]) -> \
Tuple[List[Dict], List[Dict]]:
    """Обработка чанка данных в отдельном процессе"""
    chunk, barriers, crs = args
    lines_results = []
    barriers_results = []

    for _, row in chunk.iterrows():
        line, barriers_list = process_noize_line(row, barriers, crs)
        lines_results.append(line.to_dict())
        barriers_results.extend(barriers_list)

    return lines_results, barriers_results


def process_noize_line(noize_line: Series, barriers: GeoDataFrame, crs: str) -> \
Tuple[Series, List[Dict]]:
    """Обработка одной линии шума с барьерами для отражений"""
    inter_barriers = []
    intersect_barrier = get_intersect_barrier(noize_line, barriers, crs)

    if intersect_barrier.empty:
        return noize_line, inter_barriers

    closest_line = find_near_line(noize_line.geometry, intersect_barrier)

    for _ in range(amount_of_reflections):
        if closest_line is None or intersect_barrier.empty:
            break

        noize_line, barrier = get_line_reflect(noize_line, closest_line)
        if barrier is None:
            break

        inter_barriers.append(barrier.to_dict())
        intersect_barrier = get_intersect_barrier(noize_line, barriers, crs)
        closest_line = find_near_line(noize_line.geometry, intersect_barrier)

    return noize_line, inter_barriers


def make_noise_reflection(noize: gpd.GeoDataFrame,
                          barriers: gpd.GeoDataFrame) -> None:
    """Основная функция с максимальной оптимизацией и визуализацией прогресса"""
    print(f'🚀 Запуск обработки с {MAX_WORKERS} ядрами')

    # Создаем spatial index для барьеров один раз
    # if not barriers.sindex.is_valid:
    #     barriers.sindex

    # Разбиваем данные на чанки для параллельной обработки
    chunks = np.array_split(noize,
                            max(len(noize) // CHUNKSIZE, MAX_WORKERS * 2))
    args = [(chunk, barriers, noize.crs) for chunk in chunks]

    total_chunks = len(chunks)
    lines_results = []
    barriers_results = []

    with multiprocessing.Pool(processes=MAX_WORKERS) as pool:
        with tqdm(total=total_chunks, desc="Обработка чанков",
                  unit="chunk") as pbar:
            for chunk_lines, chunk_barriers in pool.imap_unordered(
                    process_chunk, args):
                lines_results.extend(chunk_lines)
                barriers_results.extend(chunk_barriers)
                pbar.update(1)

    # Сохранение результатов
    if barriers_results:
        print("💾 Сохранение результатов...")
        GeoDataFrame(lines_results, crs=noize.crs).to_file('reflect.gpkg',
                                                           driver='GPKG')
        inter_barriers_gdf = gpd.GeoDataFrame(barriers_results,
                                              geometry='geometry',
                                              crs=barriers.crs)
        inter_barriers_gdf.to_file('barrier_noise_before_agr_2.gpkg',
                                   driver='GPKG')
        # Оптимизированная агрегация
        result = inter_barriers_gdf.groupby(['geometry', 'et'],
                                            as_index=False).agg(
            maximum=('noise_level', 'max')
        )

        gpd.GeoDataFrame(result, geometry='geometry',
                         crs=barriers.crs).to_file(
            'barrier_noise.gpkg', driver='GPKG'
        )
    print("✅ Готово!")


def get_intersect_barrier(noize_line: Series, barriers: GeoDataFrame,
                          crs: str) -> GeoDataFrame:
    """Оптимизированная версия поиска пересекающихся барьеров"""
    # Быстрый фильтр по уровню здания
    mask = barriers[building_level_column] == (
                noize_line[noise_level_column] / 3)
    filtered = barriers[mask]

    if filtered.empty:
        return filtered

    # Используем spatial index для ускорения поиска
    geom = noize_line.geometry
    possible_matches_index = list(filtered.sindex.intersection(geom.bounds))
    possible_matches = filtered.iloc[possible_matches_index]

    return possible_matches[possible_matches.intersects(geom)]


def find_near_line(line: LineString, target_lines: GeoDataFrame) -> Optional[
    Series]:
    """Оптимизированный поиск ближайшей линии"""
    if not check_geomtype(target_lines, 'LineString'):
        raise ValueError('Неверный тип геометрии: ожидался LineString')

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


def get_line_reflect(noise: Series, barrier: Series) -> Tuple[
    Optional[Series], Optional[Series]]:
    """Оптимизированная версия расчета отражения"""
    noise_geom = noise.geometry
    barrier_geom = barrier.geometry

    # Создаем последний сегмент линии
    last_segment = LineString([noise_geom.coords[-2], noise_geom.coords[-1]])
    intersection = last_segment.intersection(barrier_geom)

    if intersection.is_empty or not isinstance(intersection, Point):
        return noise, None

    # Вычисляем отражение
    x1, y1 = barrier_geom.coords[0]
    x2, y2 = barrier_geom.coords[1]
    dx, dy = x2 - x1, y2 - y1

    # Вертикальная линия
    if dx == 0:
        reflected_x = 2 * x1 - noise_geom.coords[-1][0]
        reflected_y = noise_geom.coords[-1][1]
    else:
        m = dy / dx
        c = y1 - m * x1
        x, y = noise_geom.coords[-1]
        d = (x + (y - c) * m) / (1 + m ** 2)
        reflected_x = 2 * d - x
        reflected_y = 2 * d * m - y + 2 * c

    # Создаем новую геометрию линии
    new_coords = [*noise_geom.coords[:-1], (intersection.x, intersection.y),
                  (reflected_x, reflected_y)]

    # Вычисляем новый уровень шума
    len_initial = LineString(new_coords[:-1]).length
    noise_level = noise['start_noise'] - (
                10 * log10((len_initial ** 2 + noise["level"] ** 2) ** 0.5)
    )
                # Создаем новые объекты с минимальным копированием
    reflected_noise = noise.copy()
    reflected_noise['geometry'] = LineString(new_coords)

    # reflected_barrier = barrier.copy()
    # reflected_barrier['noise_level'] = noise_level
    # reflected_barrier['noise_height'] = noise["level"]
    # reflected_barrier['len_initial'] = len_initial
    print(f'______\n {noise_level} \n {noise["level"]}\n {len_initial}\n {noise['start_noise']}\n______ ')
    reflected_barrier = {
        'noise_level': noise_level,
        'noise_height': noise["level"],
        'len_initial': len_initial,
        **barrier.to_dict()
    }

    return reflected_noise, Series(reflected_barrier)