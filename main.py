import time
import geopandas as gpd
from core.geom_transform import polygons_to_segments
from core.stars_maker import make_noise_stars
import matplotlib.pyplot as plt


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
    noise_limit = 45
    point_interval = 3
    stars_line_step = 3

    noise_stars = make_noise_stars(
        street_layer=streets,
        stars_line_step=stars_line_step,
        noise_limit=noise_limit,
        point_interval=point_interval
    )

    buildings = gpd.read_file('core/Здания_3857.gpkg')
    building_segments = polygons_to_segments(buildings)

    intersecting_noise_lines = gpd.sjoin(noise_stars, building_segments, how="inner",
                                   predicate='intersects')
    intersecting_building_segments = gpd.sjoin(building_segments, noise_stars, how="inner",
                                   predicate='intersects')




    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Время выполнения: {execution_time} секунд")
    visual_gdf(intersecting_noise_lines, intersecting_building_segments)
