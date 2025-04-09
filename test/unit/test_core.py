import os
import geopandas as gpd
from core.main_noise_creator import create_noise


def test_create_noise():
    street = gpd.read_file(os.path.join('..', 'files', 'test_street.gpkg'))
    buildings = gpd.read_file(
        os.path.join('..', 'files', 'test_buildings.gpkg')
    )
    noise_lines, noise_barrier = create_noise(street, buildings)
    assert isinstance(noise_lines, gpd.GeoDataFrame)
    assert isinstance(noise_barrier, gpd.GeoDataFrame)
    assert len(noise_lines) > 1
    assert len(noise_barrier) > 1
