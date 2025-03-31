from pandas import concat
from typing import Literal
from geopandas import GeoDataFrame
from shapely.geometry import LineString
from config import noise_segment_size, building_level_column


def polygons_to_segments(gdf: GeoDataFrame):
    if check_geomtype(gdf, 'MultiPolygon'):
        gdf = gdf.explode()
    if not check_geomtype(gdf, 'Polygon'):
        raise ValueError('тип геометрии должен быть полигон')
    lines = polygons_to_lines(gdf)
    return lines_to_segments(lines)


def lines_to_segments(gdf: GeoDataFrame):
    if check_geomtype(gdf, 'MultiLineString'):
        gdf = gdf.explode()
    if not check_geomtype(gdf, 'LineString'):
        raise ValueError('тип геометрии должен быть линией')
    all_segments = []
    for _, line in gdf.iterrows():
        segments = split_line_into_segments(line.geometry)
        line_dict = line.to_dict()
        line_dict.pop('geometry')
        for segment in segments:
            all_segments.append(
                {'geometry': segment,
                 **line_dict}
            )
    return GeoDataFrame(all_segments).set_crs(gdf.crs)


def polygons_to_lines(gdf: GeoDataFrame):
    return GeoDataFrame(
        gdf.drop(columns='geometry'),
        geometry=gdf.boundary
    ).set_crs(gdf.crs)


def split_line_into_segments(line):
    line = line.segmentize(noise_segment_size)
    coords = list(line.coords)
    segments = [LineString([coords[i], coords[i + 1]]) for i in
                range(len(coords) - 1)]
    return segments


def check_geomtype(
        gdf: GeoDataFrame,
        geomtype: Literal[
            'Point', 'LineString',
            'Polygon', 'MultiPoint',
            'MultiLineString', 'MultiPolygon'
        ]
) -> bool:
    return all(gdf['geometry'].geom_type == geomtype)


def segmentation_of_barrier_by_floors(barriers: GeoDataFrame):
    new_rows = []
    for _, barrier in barriers.iterrows():
        top_level = barrier[building_level_column]
        level = 1
        while level < top_level:
            new_barrier = barrier.copy()
            new_barrier[building_level_column] = level
            new_rows.append(new_barrier)
            level += 1
    return GeoDataFrame(concat(
        [
            barriers,
            GeoDataFrame(new_rows, geometry='geometry', crs=barriers.crs)
        ],
        ignore_index=True
    ), geometry='geometry', crs=barriers.crs)
