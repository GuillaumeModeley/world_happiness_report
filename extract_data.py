#!/usr/bin/python3

import glob
import json
import re
import sqlite3
import pandas as pd


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


if __name__ == '__main__':
    extract_countries_info('data/countries_continents_codes_flags_url.json', 'out/happiness.db')

    for yearly_report_file in glob.glob('data/*.csv'):
        year = extract_year(yearly_report_file)
        if not year:
            raise Exception("Failed to extract year from filename: %s" % yearly_report_file)

        extract_happiness_report_data(yearly_report_file, 'out/happiness.db', year)
