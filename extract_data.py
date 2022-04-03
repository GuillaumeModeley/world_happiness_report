#!/usr/bin/python3

import json
import sqlite3

connection = sqlite3.connect('out/happiness.db')

cursor = connection.cursor()

cursor.execute("PRAGMA foreign_keys=ON")

with open('data/countries_continents_codes_flags_url.json', 'r') as f:
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
