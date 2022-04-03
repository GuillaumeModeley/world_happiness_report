#!/usr/bin/python3

import os
import sqlite3
import pandas as pd

happiness_db_path = 'db/happiness.db'
modelling_record_csv_path = 'out/modelling_record.csv'
modelling_record_parquet_path = 'out/modelling_record.parquet'


def generate_modelling_record(db_path, csv_path, parquet_path):
    '''Generate modeling record files in CSV & parquet format with ranking info.'''

    if not os.path.exists(os.path.dirname(csv_path)):
        os.mkdir(os.path.dirname(csv_path))

    connection = sqlite3.connect(db_path)

    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")

    query = '''
        SELECT
            h.year,
            c.country_name,
            c.capital_city,
            c.latitude,
            c.longitude,
            c.image_url,
            r.region_code,
            CASE r.region_name
                WHEN ''
                    THEN 'Nan'
                ELSE UPPER(r.region_name)
            END region,
            RANK () OVER (
                PARTITION BY h.year
                ORDER BY h.score DESC
            ) score_rank_by_year,
            RANK () OVER (
                PARTITION BY h.year, r.region_name
                ORDER BY h.score DESC
            ) score_rank_by_year_by_region,
            h.score,
            CASE
                WHEN h.score > 5.6 THEN "Green"
                WHEN h.score < 2.6 THEN "Red"
                ELSE "Amber"
            END happiness_status,
            h.gdp_per_capita,
            h.family,
            h.social_support,
            h.healthy_life_expectancy,
            h.freedom_to_make_life_choices,
            h.generosity,
            h.perceptions_of_corruption
        FROM
            countries c
        JOIN
            happiness_report h
        ON
            h.country_id = c.country_id
        JOIN
            regions r
        ON
            c.region_id = r.region_id
        ORDER BY
            h.year, r.region_name, score_rank_by_year_by_region ASC;
            '''

    ds = pd.read_sql(query, connection)
    ds.to_csv(csv_path, index=False)
    #ds.to_parquet(parquet_path)


if __name__ == '__main__':
    generate_modelling_record(happiness_db_path, modelling_record_csv_path, modelling_record_parquet_path)
