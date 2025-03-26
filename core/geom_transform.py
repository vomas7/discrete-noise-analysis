import geopandas as gpd
from typing import Literal, Any
from shapely.geometry import LineString


def polygons_to_segments(gdf: gpd.GeoDataFrame):
    if check_geomtype(gdf, 'MultiPolygon'):
        gdf = gdf.explode()
    if not check_geomtype(gdf, 'Polygon'):
        raise ValueError('тип геометрии должен быть полигон')
    lines = polygons_to_lines(gdf)
    return lines_to_segments(lines)


def lines_to_segments(gdf: gpd.GeoDataFrame):
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
    return gpd.GeoDataFrame(all_segments).set_crs(gdf.crs)


def polygons_to_lines(gdf: gpd.GeoDataFrame):
    return gpd.GeoDataFrame(
        gdf.drop(columns='geometry'),
        geometry=gdf.boundary
    ).set_crs(gdf.crs)


def split_line_into_segments(line):
    coords = list(line.coords)
    segments = [LineString([coords[i], coords[i + 1]]) for i in
                range(len(coords) - 1)]
    return segments


def check_geomtype(
        gdf: gpd.GeoDataFrame,
        geomtype: Literal[
            'Point', 'LineString',
            'Polygon', 'MultiPoint',
            'MultiLineString', 'MultiPolygon'
        ]
) -> bool:
    return all(gdf['geometry'].geom_type == geomtype)
