# 🌆 3D Urban Noise Propagation Analysis 🎯
# Discrete noise analysis

![Python](https://img.shields.io/badge/Python-3.13-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.68-green)
![Docker](https://img.shields.io/badge/Docker-✓-blue)
![PostGIS](https://img.shields.io/badge/PostGIS-3.0-orange)

Проект для моделирования распространения шума в городской среде с визуализацией на фасадах зданий.

## 🚀 Основные возможности

- 🌐 **3D моделирование** распространения шума от дорог
- 🏢 **Анализ отражений** звуковых волн от зданий
- 📊 **Секционирование фасадов** (3×3 метра) с расчетом уровня шума
- 🐍 **Реализация на Python** с использованием Geopandas
- 🚀 **REST API** на FastAPI
- 🐳 **Готовый Docker-образ** для быстрого развертывания

## 📦 Структура проекта

```bash
.
├── .idea/                  # Конфигурация IDE
├── .pytest_cache/          # Кеш тестов
├── core/                   # Основная логика
│   ├── geom_transform/     # Преобразования геометрии
│   ├── stars_maker/        # Генерация полусфер шума
│   └── reflection/         # Алгоритмы отражения
├── test/                   # Тесты
└── Dockerfile              # Конфигурация Docker
```
## 🛠️ Технологический стек

- **Язык:** Python 3.13

- **ГИС:** Geopandas, PostGIS

- **API:** FastAPI

- **Контейнеризация:** Docker

## 🐳 Запуск через Docker
```bash
Copy
docker build -t noise-model .
docker run -p 80:80 noise-model
```

_После запуска API будет доступно на http://localhost:80_

## 🧮 Научная методика
1. **Генерация полусфер шума:**
    - Построение изолиний шума с заданным шагом
    - Расчет затухания звука с расстоянием

2. **Моделирование отражений:**

   - Трассировка лучей от дороги к зданиям

   - Учет поглощения энергии при отражениях

3. **Анализ фасадов:**

   - Разбивка на секции 3×3 метра

   - Расчет интегрального уровня шума для каждой секции

**Urban Noise Analysis** © **2025** | **vomas7** | M**aking cities quieter, one simulation at a time 🌇🔇**