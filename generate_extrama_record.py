#!/usr/bin/python3

import os
import sqlite3
import pandas as pd

happiness_db_path = 'db/happiness.db'
extrema_record_path = 'out/extrema_record.json'


def generate_extrema_record(db_path, extrema_record_path):
    '''Generate JSON file with min/max Scores & min/max Ranks per country.'''

    if not os.path.exists(os.path.dirname(extrema_record_path)):
        os.mkdir(os.path.dirname(extrema_record_path))

    connection = sqlite3.connect(db_path)

    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")

    query = '''
        SELECT
            c.country_name AS Country,
            MAX(h.score_rank_by_year) AS "Highest Rank",
            MIN(h.score_rank_by_year) AS "Lowest Rank",
            MAX(h.score) AS "Highest Happiness Score",
            MIN(h.score) AS "Lowest Happiness Score"
        FROM happiness_report h
        JOIN countries c ON c.country_id = h.country_id
        GROUP BY c.country_id
            '''

    ds = pd.read_sql(query, connection)
    ds.to_json(extrema_record_path, orient="records", lines=True)

    connection.commit()
    connection.close()


if __name__ == '__main__':
    generate_extrema_record(happiness_db_path, extrema_record_path)
