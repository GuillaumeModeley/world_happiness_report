#!/usr/bin/python3

import sqlite3

def create_database(db_path):
    '''Create a database to store the World Happiness Report data'''

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
                          FOREIGN KEY (country_id)
                              REFERENCES countries (country_id)
                   )''')

    connection.commit()
    connection.close()


if __name__ == '__main__':
    create_database("out/happiness.db")

