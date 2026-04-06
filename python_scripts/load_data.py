import hashlib
import requests
import time
import logging
import clickhouse_driver
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
json_url = "http://api.open-notify.org/astros.json"

client = clickhouse_driver.Client(host="localhost", port=9000, user="default", password="")


def load_data(url, n_time=5, base_time=2):

    def time_sleep(_n, _max, _time):
        if _n != _max:
            logger.info(f"{_n} - Retry after {_time}...")
            time.sleep(_time)

    for i in range(n_time):

        time_stop = base_time ** i

        try:
            response = requests.get(url, timeout=n_time)
            resp_code = response.status_code
            if resp_code == 200:
                logger.info(f"Load data: Success - code {resp_code}")
                return json.dumps(response.json(), ensure_ascii=False)
            elif resp_code == 429:
                logger.warning(f"Load data: Error - Code {resp_code} - Rate Limit")
                time_sleep(i + 1, n_time, time_stop)
                continue
            elif 400 <= resp_code <= 600:
                logger.warning(f"Load data: Error - Code {resp_code}")
                time_sleep(i + 1, n_time, time_stop)
                continue
        except requests.exceptions.RequestException as new_error:
            logger.warning(f"Load data: An unexpected error - {new_error}")
            time_sleep(i + 1, n_time, time_stop)
            continue

    logger.error(f"Load data: {n_time} failed attempts.")
    raise


def create_raw_table(name="raw_data"):

    client.execute(
        f"""
            create table if not exists {name}(
                data_id String, 
                json_data String, 
                _inseserted_at DateTime default now() 
            ) 
            engine = ReplacingMergeTree(_inseserted_at) 
            order by data_id
        """
    )


def add_json_str(dat, json_obj):

    dat_id = hashlib.md5(json_obj.encode()).hexdigest()

    client.execute(
        f"""
            insert into {dat}(data_id, json_data) 
            values ('{dat_id}', '{json_obj}')
        """
    )

    client.execute(f"optimize table {dat} final")


def create_parsed_table(name="people"):

    client.execute(
        f"""
        create table if not exists {name}( 
            craft String, 
            name String, 
            _inseserted_at DateTime 
        ) 
        engine = MergeTree() 
        order by _inseserted_at 
        """
    )


def mv_obj(dat1, dat2, name="mv_table"):

    client.execute(
        f"""
            create materialized view if not exists {name} to {dat2}
            as select
                JSONExtractString(arr, 'craft') as craft,
                JSONExtractString(arr, 'name') as name,
                _inseserted_at
        from {dat1}
        array join JSONExtractArrayRaw(json_data, 'people') as arr
        """
    )


json_str = load_data(json_url)
create_raw_table("raw_data")
create_parsed_table("people")
mv_obj("raw_data", "people")
add_json_str("raw_data", json_str)
