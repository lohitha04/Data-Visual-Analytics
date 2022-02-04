"""
cse6242 s21
wrangling.py - utilities to supply data to the templates.

This file contains a pair of functions for retrieving and manipulating data
that will be supplied to the template for generating the table. """
import csv

def username():
    return 'lrajasekar3'

def data_wrangling():
    with open('data/movies.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        table = list()
        # Feel free to add any additional variables
        ...
        
        # Read in the header
        for header in reader:
            break
        
        # Read in each row
        for row in reader:
            table.append(row)
            
            # Only read first 100 data rows - [2 points] Q5.a
            if len(table) == 100:
                break

        # Order table by the last column - [3 points] Q5.b
        for row in table:
            for k in range(len(row)):
                if k == 2:
                    row[k] = float(row[k])


        table.sort(key=lambda x: -x[2])

        for row in table:
            for k in range(len(row)):
                if k == 2:
                    row[k] = str(row[k])




    return header, table

