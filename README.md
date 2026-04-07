# dbt-ClickHouse

## Структура проекта
dbt/ DBT модели и конфиги<br/>
python_scripts/ Python скрипт загрузки данных<br/>
dockerfile/ для ClickHouse и DBT<br/>

## Сноска
.root - заменить на путь к папке проекта

## Запуск

### 1. Запустить ClickHouse в Docker
```cmd
docker run -d --name clickhouse-s1 -p 8123:8123 -p 9000:9000 -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 clickhouse/clickhouse-server
```

### 2. Создать Images dbt-clickhouse запустив файл dockerfile
```cmd
cd .root\dockerfile
docker build -t dbt-clickhouse .
```

### 3. Запустить скрипт для загрузки данных в СlickHouse
```cmd
cd .root/python_scripts
python -m venv venv
venv\Scripts\activate
pip install -r libs.txt
python load_data.py
```

### 4. Подключится [скрипт диактевирует и удаляет контейнер после отработки]
```cmd
docker run --network=host --mount type=bind,source=.root/dbt/imdb,target=/usr/app /
--mount type=bind,source=.root/dbt/imdb/profiles.yml,target=/root/.dbt/profiles.yml /
--rm -it dbt-clickhouse run
```

### 5. Проверка результатов
Все результаты храняться в default базе данных.

```cmd
docker exec -it clickhouse-s1 clickhouse-client
```

```cmd
1) select * from raw_data
2) select * from people
3) select * from people_stat_list
```
