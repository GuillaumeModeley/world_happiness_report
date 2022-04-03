#!/usr/bin/python3

import json
import sqlite3

connection = sqlite3.connect('out/happiness.db')

cursor = connection.cursor()

with open('data/countries_continents_codes_flags_url.json', 'r') as f:
    for country_data in json.load(f):
        cursor.execute('INSERT INTO countries (country_name, country_code) VALUES (?, ?)', (country_data['country'], country_data['country-code']))

connection.commit()
connection.close()

