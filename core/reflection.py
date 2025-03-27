from pandas.core.series import Series
from core.geom_transform import check_geomtype
from geopandas import GeoDataFrame, sjoin
from shapely.geometry import Point, LineString
from math import degrees, atan2, radians, cos, sin
from config import building_level_column, noise_level_column


def make_noise_reflection(noize: GeoDataFrame, barriers: GeoDataFrame):
    inter_barriers = GeoDataFrame(columns=barriers.columns,
                                      geometry='geometry', crs=barriers.crs)
    for _, noize_line in noize.iterrows():

        intersect_barrier = get_intersect_barrier(noize_line, barriers,
                                                  noize.crs)
        if intersect_barrier.empty:
            continue

        closest_line = find_near_line(noize_line.geometry, inter_barriers)
        line_reflect = get_line_reflect




def get_intersect_barrier(
        noize_line: Series,
        barriers: GeoDataFrame, crs
) -> GeoDataFrame:
    gdf_noize_line = GeoDataFrame(
        geometry=[noize_line.geometry]
    ).set_crs(crs)

    filter_barriers = barriers[
        barriers[building_level_column] == noize_line[noise_level_column]
        ]
    if 'index_right' in gdf_noize_line.columns:
        gdf_noize_line = gdf_noize_line.drop(columns=['index_right'])
    if 'index_right' in filter_barriers.columns:
        filter_barriers = filter_barriers.drop(columns=['index_right'])
    return sjoin(
        filter_barriers, gdf_noize_line, how="inner", predicate='intersects'
    )


def find_near_line(line: LineString, target_lines: GeoDataFrame) -> Series:
    if not check_geomtype(target_lines, 'LineString'):
        raise ValueError('не верный тип геометрии ожидался LineString')
    first_noise_point = Point(line.coords[0])
    closest_line = None
    min_distance = .0

    for _, target_line in target_lines.iterrows():
        intersection = line.intersection(target_line.geometry)
        distance = first_noise_point.distance(intersection)

        if distance < min_distance:
            min_distance = distance
            closest_line = target_line

    return closest_line


def get_line_reflect(point_noize, angle_noize_line, line_noize, line_reflect):

    intersection = line_noize.intersection(line_reflect)

    intersection_point = intersection if intersection.is_empty else intersection.centroid

    dx = point_noize.x - intersection_point.x
    dy = point_noize.y - intersection_point.y
    directional_angle = degrees(atan2(dy, dx)) % 360

    angle_noize_line = angle_noize_line % 360

    if 0 <= angle_noize_line <= 90 or 270 < angle_noize_line <= 360:
        directional_angle += 180

    line_reflect_coords = list(line_reflect.coords)
    dx_wall = line_reflect_coords[1][0] - line_reflect_coords[0][0]
    dy_wall = line_reflect_coords[1][1] - line_reflect_coords[0][1]

    d_angle_intersect = degrees(atan2(dy_wall, dx_wall)) % 360
    directional_angle_normal = (d_angle_intersect + 90) % 360

    if 0 < angle_noize_line < 180:
        directional_angle_normal += 180

    len_ost_line = LineString(
        [intersection_point, line_noize.coords[-1]]).length

    angle_reflect = directional_angle_normal - (
                directional_angle - directional_angle_normal)

    endpoint_x_reflect = intersection_point.x + len_ost_line * cos(
        radians(angle_reflect))
    endpoint_y_reflect = intersection_point.y + len_ost_line * sin(
        radians(angle_reflect))

    noize_line = LineString([point_noize, intersection_point,
                             Point(endpoint_x_reflect, endpoint_y_reflect)])

    return noize_line, angle_reflect