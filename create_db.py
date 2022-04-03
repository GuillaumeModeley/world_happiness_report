#!/usr/bin/python3

import sqlite3

connection = sqlite3.connect("out/happiness.db")

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

connection.commit()

connection.close()
