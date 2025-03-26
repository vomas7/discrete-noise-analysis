import geopandas as gpd
from config import building_level_column, noise_level_column
from pandas.core.series import Series


def make_noise_reflection(noize: gpd.GeoDataFrame, barriers: gpd.GeoDataFrame):
    for _, noize_line in noize.iterrows():
        get_intersect_barrier(noize_line, barriers, noize.crs)



def get_intersect_barrier(noize_line: Series, barriers: gpd.GeoDataFrame, crs):
    gdf_noize_line = gpd.GeoDataFrame(
        geometry=[noize_line.geometry]
    ).set_crs(crs)

    filter_barriers = barriers[
        barriers[building_level_column] >= noize_line[noise_level_column]
    ]
    if 'index_right' in gdf_noize_line.columns:
        gdf_noize_line = gdf_noize_line.drop(columns=['index_right'])
    if 'index_right' in filter_barriers.columns:
        filter_barriers = filter_barriers.drop(columns=['index_right'])
    return gpd.sjoin(
        filter_barriers, gdf_noize_line, how="inner", predicate='intersects'
    )
