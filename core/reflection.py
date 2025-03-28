from pandas.core.series import Series
from geopandas import GeoDataFrame, sjoin
from core.geom_transform import check_geomtype
from math import degrees, atan2, radians, cos, sin, log10
from config import building_level_column, noise_level_column
from shapely.geometry import Point, LineString, MultiPoint, GeometryCollection, MultiLineString


def make_noise_reflection(noize: GeoDataFrame, barriers: GeoDataFrame):
    inter_barriers = []
    lines_reflect = []
    print('создаю отражения')

    for _, noize_line in noize.iterrows():
        i = 0
        intersect_barrier = get_intersect_barrier(noize_line, barriers,
                                                      noize.crs)
        if intersect_barrier.empty:
            continue
        while not intersect_barrier.empty and i < 3:
            i += 1
            intersect_barrier = get_intersect_barrier(noize_line, barriers,
                                                      noize.crs)
            if intersect_barrier.empty:
                break
            closest_line = find_near_line(noize_line.geometry, intersect_barrier)

            noize_line, barrier = get_line_reflect(noize_line, closest_line, i)
            # if not noize_line:
            #     print('линия не создалась')
            #     break
            # else:
            #     print('линия создалась')

            inter_barriers.append(barrier)


        lines_reflect.append(noize_line)
    GeoDataFrame(lines_reflect, crs=noize.crs).to_file('reflect.gpkg', driver='GPKG')
    inter_barriers = GeoDataFrame(inter_barriers, crs=barriers.crs)
    result = inter_barriers.groupby(['geometry', 'et'], as_index=False).agg(maximum=('noise_level', 'max'))
    result_geo = GeoDataFrame(result, geometry='geometry', crs=barriers.crs)
    result_geo.to_file('barrier_noise.gpkg', driver='GPKG')

def get_intersect_barrier(
        noize_line: Series,
        barriers: GeoDataFrame, crs
) -> GeoDataFrame:
    gdf_noize_line = GeoDataFrame(
        geometry=[noize_line.geometry]
    ).set_crs(crs)

    filter_barriers = barriers[
        barriers[building_level_column] * 3 == noize_line[noise_level_column]
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
    first_noise_point = Point(line.coords[-2])
    closest_line = None
    min_distance = float('inf')

    for _, target_line in target_lines.iterrows():
        intersection = line.intersection(target_line.geometry)
        distance = first_noise_point.distance(intersection)

        if distance < min_distance:
            min_distance = distance
            closest_line = target_line

    return closest_line


def get_line_reflect(noise: Series, barrier: Series, number_ref) -> Series | None:
    noise_geom = noise.geometry
    barrier_geom = barrier.geometry
    penultimate_point = Point(noise.geometry.coords[-2])

    intersection = noise_geom.intersection(barrier_geom)

    if intersection.is_empty:
        return None, None
    if isinstance(intersection, GeometryCollection):
        intersection = intersection.geoms[0]
    if isinstance(intersection, MultiLineString):
        intersection = intersection.geoms[0]
    if isinstance(intersection, LineString):
        intersection = intersection.centroid
    if isinstance(intersection, MultiPoint):
        intersection = intersection.geoms[0]

    dx = penultimate_point.x - intersection.x
    dy = penultimate_point.y - intersection.y

    directional_angle = degrees(atan2(dy, dx)) % 360

    line_reflect_coords = list(barrier_geom.coords)
    dx_wall = line_reflect_coords[1][0] - line_reflect_coords[0][0]
    dy_wall = line_reflect_coords[1][1] - line_reflect_coords[0][1]

    d_angle_intersect = degrees(atan2(dy_wall, dx_wall)) % 360
    if d_angle_intersect == 0:
        d_angle_intersect = 1.57

    normal_angle = (d_angle_intersect + 90) % 360

    angle_diff = (directional_angle - normal_angle + 180) % 360 - 180 # Разница углов от -180 до 180
    if angle_diff > 0: # Если падающая линия "слева" от нормали (против часовой стрелки)
        normal_angle = (d_angle_intersect - 90) % 360 # Берем другую нормаль

    if directional_angle < normal_angle:
        normal_angle += 180


    len_ost_line = LineString(
        [intersection, noise_geom.coords[-1]]).length

    angle_reflect = normal_angle - (directional_angle - normal_angle)

    endpoint_x_reflect = intersection.x + len_ost_line * cos(
        radians(angle_reflect))
    endpoint_y_reflect = intersection.y + len_ost_line * sin(
        radians(angle_reflect))

    noise_geom = LineString([
        *[Point(coord) for coord in noise_geom.coords[:-1]],
        intersection,
        Point(endpoint_x_reflect, endpoint_y_reflect)
    ])

    attr = noise.to_dict()
    attr.pop('geometry')

    len_initial_line = LineString([noise_geom.coords[-2], intersection]).length


    noise_level = noise['start_noise'] - (10 * log10((len_initial_line ** 2 + noise["level"] ** 2) ** 0.5))

    reflected_noise_line = {
        'geometry': noise_geom,
        'angle': angle_reflect,
        'directional_angle': directional_angle,
        'normal_angle': normal_angle,
        'number_ref': number_ref,
        'd_angle_intersect': d_angle_intersect,
        **attr
    }

    barrier = {
        'noise_level': noise_level,
        **barrier.to_dict()
    }

    return Series(reflected_noise_line), Series(barrier)
