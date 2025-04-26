import sqlite3
import pandas as pd

conn = sqlite3.connect('STAFF.db')

# Create a table in the database
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

# Read the CSV file
file_path = 'INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)

df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')

# Python Scripting: Running basic queries on data
# 1. Viewing all the data in the table
query_statement = f'SELECT * FROM {table_name}'
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
print()

# 2. Viewing only FNAME column of data
query_statement = f'SELECT FNAME FROM {table_name}'
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
print()

# 3. Viewing the total number of entries in the table.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Append data to the table
data_dict = {
    'ID': [100],
    'FNAME': ['John'],
    'LNAME': ['Doe'],
    'CITY': ['Paris'],
    'CCODE': ['FR']
}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

conn.close()