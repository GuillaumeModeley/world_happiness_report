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
            h.year AS Year,
            c.country_name AS Country,
            c.capital_city AS "Capital City",
            c.latitude AS Latitude,
            c.longitude AS Longitude,
            c.image_url AS "Country Url",
            r.region_code AS "Region Code",
            CASE r.region_name
                WHEN ''
                    THEN 'Nan'
                ELSE UPPER(r.region_name)
            END Region,
            h.score_rank_by_year AS "Overall Rank",
            h.score_rank_by_year_by_region AS "Rank Per Region",
            h.score AS "Happiness Score",
            CASE
                WHEN h.score > 5.6 THEN "Green"
                WHEN h.score < 2.6 THEN "Red"
                ELSE "Amber"
            END "Happiness Status",
            h.gdp_per_capita AS "GDP per capita",
            h.family AS Family,
            h.social_support AS "Social support",
            h.healthy_life_expectancy AS "Healthy life expectancy",
            h.freedom_to_make_life_choices AS "Freedom to make life choices",
            h.generosity AS Generosity,
            h.perceptions_of_corruption AS "Perceptions of corruption"
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
