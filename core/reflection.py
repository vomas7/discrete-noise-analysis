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
    last_noise_geom = Point(noise.geometry.coords[-1])

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

    x1, y1 = barrier_geom.coords[0]
    x2, y2 = barrier_geom.coords[1]

    m = (y2 - y1) / (x2 - x1) if x2 != x1 else float('inf')
    c = y1 - m * x1 if x2 != x1 else x1

    if m == float('inf'):
        reflected_x = 2 * x1 - last_noise_geom.x
        reflected_y = last_noise_geom.y
    else:
        d = (last_noise_geom.x + (last_noise_geom.y - c) * m) / (1 + m**2)
        reflected_x = 2 * d - last_noise_geom.x
        reflected_y = 2 * d * m - last_noise_geom.y + 2 * c

    noise_geom = LineString([
        *[Point(coord) for coord in noise_geom.coords[:-1]],
        intersection,
        Point(reflected_x, reflected_y)
    ])

    attr = noise.to_dict()
    attr.pop('geometry')

    len_initial_line = LineString([noise_geom.coords[-2], intersection]).length


    noise_level = noise['start_noise'] - (10 * log10((len_initial_line ** 2 + noise["level"] ** 2) ** 0.5))

    reflected_noise_line = {
        'geometry': noise_geom,
        'number_ref': number_ref,
        **attr
    }

    barrier = {
        'noise_level': noise_level,
        **barrier.to_dict()
    }

    return Series(reflected_noise_line), Series(barrier)
