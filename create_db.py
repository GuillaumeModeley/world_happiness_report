#!/usr/bin/python3

import os
import sqlite3

happiness_db_path = 'db/happiness.db'


def create_database(db_path):
    '''Create a database to store the World Happiness Report data'''

    if not os.path.exists(os.path.dirname(db_path)):
        os.mkdir(os.path.dirname(db_path))

    connection = sqlite3.connect(db_path)

    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")

    cursor.execute('''CREATE TABLE regions (
                          region_id INTEGER PRIMARY KEY,
                          region_name TEXT UNIQUE,
                          region_code INTEGER UNIQUE
                   )''')

    cursor.execute('''CREATE TABLE countries (
                          country_id INTEGER PRIMARY KEY,
                          country_name TEXT NOT NULL,
                          country_code INTEGER,
                          image_url TEXT,
                          region_id INTEGER,
                          FOREIGN KEY (region_id)
                              REFERENCES regions (region_id)
                   )''')

    cursor.execute('''CREATE TABLE happiness_report (
                          id INTEGER PRIMARY KEY,
                          country_id INTEGER,
                          score REAL,
                          year TEXT,
                          gdp_per_capita REAL,
                          family REAL,
                          social_support REAL,
                          healthy_life_expectancy REAL,
                          freedom_to_make_life_choices REAL,
                          generosity REAL,
                          perceptions_of_corruption REAL,
                          FOREIGN KEY (country_id)
                              REFERENCES countries (country_id)
                   )''')

    connection.commit()
    connection.close()


if __name__ == '__main__':
    create_database(happiness_db_path)
