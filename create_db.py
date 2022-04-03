#!/usr/bin/python3

import sqlite3

connection = sqlite3.connect("out/happiness.db")

cursor = connection.cursor()

cursor.execute('''CREATE TABLE countries (
                      country_id INT PRIMARY KEY,
                      country_name TEXT NOT NULL,
                      country_code INT
               )''')

connection.commit()

connection.close()

