#!/usr/bin/python3

import glob
import json
import os
import re
import sqlite3
import pandas as pd

happiness_db_path = 'db/happiness.db'
modelling_record_csv_path = 'out/modelling_record.csv'


def extract_countries_info(countries_info_path, db_path):
    '''Extract countries information from a JSON-formatted file,
       and store them in the db_path database.
    '''
    connection = sqlite3.connect(db_path)

    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")

    with open(countries_info_path, 'r') as f:
        for country_data in json.load(f):
            cursor.execute('''INSERT OR IGNORE INTO regions
                                  (region_name,
                                   region_code)
                              VALUES
                                  (?, ?)''',
                           (country_data['region'],
                            country_data['region-code']))

            cursor.execute('''INSERT INTO countries
                                  (country_name,
                                   country_code,
                                   image_url,
                                   region_id)
                              VALUES
                                  (?, ?, ?,
                                  (SELECT region_id FROM regions WHERE region_name = ?)
                                  )''',
                           (country_data['country'],
                            country_data['country-code'],
                            country_data['image_url'],
                            country_data['region']))

    connection.commit()
    connection.close()


def normalize_column_name(name):
    normalized_names = [ 'country', 'score', 'gdp', 'family', 'social', 'health', 'freedom', 'generosity', 'corruption' ]
    for n in normalized_names:
        if n in name.lower():
            return n
    return name


def extract_year(filename):
    '''Extract YYYY string from filename'''
    match = re.search(r'\d{4}', filename)
    if match:
        return match[0]
    return None


def extract_happiness_report_data(happiness_data_path, db_path, year):
    '''Extract yearly World Happiness Report data from a CSV-formatted file
       and store it in the db_path database.
    '''
    connection = sqlite3.connect(db_path)

    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")

    dataset = pd.read_csv(happiness_data_path)
    dataset.rename(columns=normalize_column_name, inplace=True)

    dataset['year'] = year

    # handle missing columns in CSV data
    for header in [ 'family', 'social' ]:
        if header not in  dataset.columns:
            dataset[header] = None

    cursor.executemany('''INSERT INTO happiness_report
                              (country_id,
                               year,
                               score,
                               gdp_per_capita,
                               family,
                               social_support,
                               healthy_life_expectancy,
                               freedom_to_make_life_choices,
                               generosity,
                               perceptions_of_corruption)
                          VALUES
                              ((SELECT country_id FROM countries WHERE country_name = ?),
                              ?, ?, ?, ?, ?, ?, ?, ?, ?
                              )''',
                       [ (c, y, s, g, fa, ss, hle, ftmlc, gen, cor) for c, y, s, g, fa, ss, hle, ftmlc, gen, cor in zip(
                           dataset['country'],
                           dataset['year'],
                           dataset['score'],
                           dataset['gdp'],
                           dataset['family'],
                           dataset['social'],
                           dataset['health'],
                           dataset['freedom'],
                           dataset['generosity'],
                           dataset['corruption']) ])

    connection.commit()
    connection.close()


def generate_modelling_record(db_path, csv_path):
    '''Generate modeling record file in CSV format with ranking info.'''

    if not os.path.exists(os.path.dirname(csv_path)):
        os.mkdir(os.path.dirname(csv_path))

    connection = sqlite3.connect(db_path)

    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")

    query = '''
        SELECT
            h.year,
            c.country_name,
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
    ds.to_csv(csv_path)


if __name__ == '__main__':
    extract_countries_info('data/countries_continents_codes_flags_url.json', happiness_db_path)

    for yearly_report_file in glob.glob('data/*.csv'):
        year = extract_year(yearly_report_file)
        if not year:
            raise Exception("Failed to extract year from filename: %s" % yearly_report_file)

        extract_happiness_report_data(yearly_report_file, happiness_db_path, year)

    generate_modelling_record(happiness_db_path, modelling_record_csv_path)
