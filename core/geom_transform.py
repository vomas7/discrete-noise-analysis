import math
import warnings
import numpy as np
from tqdm import tqdm
from pandas import concat
from typing import Literal
from shapely.geometry import LineString
from multiprocessing import Pool, cpu_count
from geopandas import GeoDataFrame, options
from config import noise_segment_size, building_level_column, geometry_column

warnings.filterwarnings('ignore')

# Включаем оптимизацию геоопераций
options.use_pygeos = True


def polygons_to_segments(gdf: GeoDataFrame):
    """Конвертация полигонов в сегменты с многопроцессорной обработкой"""
    if check_geomtype(gdf, 'MultiPolygon'):
        gdf = gdf.explode(index_parts=True).reset_index(drop=True)
        gdf = gdf[gdf.geometry.type == 'Polygon'].copy()
    if not check_geomtype(gdf, 'Polygon'):
        raise ValueError('Тип геометрии должен быть Polygon')

    # Разбиваем данные на чанки для обработки
    chunks = np.array_split(gdf, cpu_count() * 4)

    with Pool(processes=cpu_count()) as pool:
        results = list(tqdm(
            pool.imap_unordered(_process_polygon_chunk, chunks),
            total=len(chunks),
            desc="Converting polygons to lines"
        ))

    lines = concat(results)
    return lines_to_segments(GeoDataFrame(lines, crs=gdf.crs))


def _process_polygon_chunk(chunk):
    """Вспомогательная функция для обработки чанка полигонов"""
    return polygons_to_lines(chunk)


def lines_to_segments(gdf: GeoDataFrame):
    """Разделение линий на сегменты с многопроцессорной обработкой"""
    if check_geomtype(gdf, 'MultiLineString'):
        gdf = gdf.explode(index_parts=True).reset_index(drop=True)
        gdf = gdf[gdf.geometry.type == 'LineString'].copy()
    if not check_geomtype(gdf, 'LineString'):
        raise ValueError(
            f'Тип геометрии должен быть LineString, а сейчас {gdf.geom_type}')

    # Разбиваем данные на чанки
    chunks = [(i, gdf.iloc[i]) for i in range(len(gdf))]

    with Pool(processes=cpu_count()) as pool:
        results = list(tqdm(
            pool.imap_unordered(_process_line_chunk, chunks),
            total=len(chunks),
            desc="Splitting lines into segments"
        ))

    # Собираем все сегменты в один DataFrame
    all_segments = []
    for segments in results:
        all_segments.extend(segments)

    return GeoDataFrame(all_segments).set_crs(gdf.crs)


def _process_line_chunk(args):
    """Вспомогательная функция для обработки одной линии"""
    idx, line = args
    segments = split_line_into_segments(line.geometry)
    line_dict = line.to_dict()
    line_dict.pop(geometry_column)
    return [{geometry_column: segment, **line_dict} for segment in segments]


def polygons_to_lines(gdf: GeoDataFrame):
    """Оптимизированная конвертация полигонов в линии"""
    lines_gdf = GeoDataFrame(
        gdf.drop(columns=geometry_column),
        geometry=gdf.boundary
    ).set_crs(gdf.crs)
    return lines_gdf.explode(index_parts=True).reset_index(drop=True).loc[
        lambda x: x.geometry.type == 'LineString'
    ].copy()


def split_line_into_segments(line):
    """Векторизованное разделение линии на сегменты"""
    line = line.segmentize(noise_segment_size)
    coords = np.array(line.coords)
    return [LineString([coords[i], coords[i + 1]]) for i in
            range(len(coords) - 1)]


def check_geomtype(
        gdf: GeoDataFrame,
        geomtype: Literal[
            'Point', 'LineString',
            'Polygon', 'MultiPoint',
            'MultiLineString', 'MultiPolygon'
        ]
) -> bool:
    """Быстрая проверка типа геометрии"""
    return all(gdf.geometry.type == geomtype)


def segmentation_of_barrier_by_floors(barriers: GeoDataFrame):
    """Многопроцессорная сегментация барьеров по этажам"""
    # Разбиваем данные на чанки
    chunks = np.array_split(barriers, cpu_count() * 4)

    with Pool(processes=cpu_count()) as pool:
        results = list(tqdm(
            pool.imap_unordered(_process_barrier_chunk, chunks),
            total=len(chunks),
            desc="Processing barriers by floors"
        ))

    # Объединяем результаты
    new_rows = []
    for chunk_result in results:
        new_rows.extend(chunk_result)

    return GeoDataFrame(concat(
        [barriers,
         GeoDataFrame(new_rows, geometry=geometry_column, crs=barriers.crs)],
        ignore_index=True
    ), geometry=geometry_column, crs=barriers.crs)


def _process_barrier_chunk(chunk):
    """Обработка чанка барьеров"""
    new_rows = []
    for _, barrier in chunk.iterrows():
        if not math.isnan(barrier[building_level_column]):
            top_level = int(barrier[building_level_column])
        else:
            top_level = 1
        for level in range(1, top_level + 1):
            new_barrier = barrier.copy()
            new_barrier[building_level_column] = level
            new_rows.append(new_barrier)
    return new_rows
