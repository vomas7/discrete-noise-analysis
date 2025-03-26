import time
import geopandas as gpd
from core.geom_transform import polygons_to_segments
from core.stars_maker import make_noise_stars
import matplotlib.pyplot as plt
from core.reflection import make_noise_reflection
from config import (
    noise_limit,
    point_interval,
    stars_line_step,
    noise_level_column,
    building_level_column
)

def visual_gdf(layer1, layer2):
    # Визуализация
    fig, ax = plt.subplots(figsize=(10, 10))  # Создание фигуры и осей

    # Отображение пересекающихся линий
    layer1.plot(ax=ax, color='blue', linewidth=1,
                            label='Lines')

    # Дополнительно: можно отобразить здания или другие слои для контекста
    layer2.plot(ax=ax, color='lightgrey', edgecolor='black', alpha=0.5,
                   label='Buildings')

    # Настройка графика
    plt.title('Intersecting Lines with Buildings')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.legend()  # Показать легенду
    plt.grid(True)

    # Показать график
    plt.show()

if __name__ == '__main__':
    start_time = time.time()

    streets = gpd.read_file('core/street_3857.gpkg')
    crs = streets.crs

    noise_stars = make_noise_stars(
        street_layer=streets,
        stars_line_step=stars_line_step,
        noise_limit=noise_limit,
        point_interval=point_interval
    )

    buildings = gpd.read_file('core/Здания_3857.gpkg')
    building_segments = polygons_to_segments(buildings)

    intersect_noise_lines = gpd.sjoin(noise_stars, building_segments, how="inner",
                                      predicate='intersects')
    intersecting_building_segments = gpd.sjoin(building_segments, noise_stars, how="inner",
                                   predicate='intersects')

    filtered_noise_lines = intersect_noise_lines[
        intersect_noise_lines[noise_level_column] <= intersect_noise_lines[
            building_level_column] * 3
        ]

    filtered_noise_lines.to_file('noise_line.gpkg')

    make_noise_reflection(
        noize=filtered_noise_lines,
        barriers=intersecting_building_segments
    )


    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Время выполнения: {execution_time} секунд")
    # visual_gdf(intersect_noise_lines, intersecting_building_segments)
