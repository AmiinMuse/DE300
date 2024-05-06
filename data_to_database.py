import csv
import mysql.connector

csv_file = "cleaned_heart_disease.csv ",
db_connection = "127.0.0.1",
table_name = "Heart_Data",


with open(csv_file, 'r') as csvfile:    
    reader = csv.reader(csvfile)
    headers = next(reader)

    sql = f"CREATE TABLE {table_name} ("
    for i, header in enumerate(headers):
        if header in ['age', 'sex', 'painloc', 'painexer', 'relrest', 'cp', 'trestbps', 
                        'htn', 'chol', 'smoke', 'cigs', 'years', 'fbs', 'dm', 'famhist', 'restecg', 'ekgmo', 'ekgday(day', 
                        'ekgyr', 'dig', 'prop', 'nitr', 'pro', 'diuretic', 'proto', 'thaldur', 'thaltime', 'met', 'thalach', 
                        'thalrest', 'tpeakbps', 'tpeakbpd', 'dummy', 'trestbpd', 'exang', 'xhypo', 'oldpeak', 'slope', 'rldv5', 
                        'rldv5e', 'ca', 'restckm', 'exerckm', 'restef', 'restwm', 'exeref', 
                        'exerwm', 'thal', 'thalsev', 'thalpul', 'earlobe', 'cmo', 'cday', 'cyr']:
            data_type = "FLOAT"  # Assuming these are numerical
        else:
            data_type = "VARCHAR(255)"  # Other columns as text by default
        sql += f"{header} {data_type} NOT NULL,"
        sql = sql[:-1] + ");"  # Remove trailing comma and add closing parenthesis

    # Execute the SQL statement
    cursor = db_connection.cursor()
    cursor.execute(sql)
    db_connection.commit()
    print(f"Table '{table_name}' created successfully.")

def load_csv_data(db_connection, table_name, csv_file):
  """
  Loads data from the CSV file into the specified table.

  Args:
      db_connection: A MySQL database connection object.
      table_name: The name of the database table.
      csv_file: The path to the CSV file.
  """
  with open(csv_file, 'r') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Skip header row (already processed)

    # Insert data row by row (adjust based on your data types)
    sql = f"INSERT INTO {table_name} VALUES (%s, %s, ...)"  # Add placeholders for columns
    cursor = db_connection.cursor()
    for row in reader:
      cursor.execute(sql, row)
    db_connection.commit()
    print(f"Data loaded into table '{table_name}'.")

# Modify these variables with your details
database = "hear_disease"
username = "amiin"
password = "amiin123!"
host = "localhost"  # Modify if your database is on a different host
port = "3306"
table_name = "your_table_name"
csv_file = "path/to/your/data.csv"

# Connect to the MySQL database
db_connection = mysql.connector.connect(
  database=database,
  user=username,
  password=password,
  host=host,
  port=port
)

try:
  # Create table schema
#   create_table_schema(db_connection, table_name, csv_file)

  # Load CSV data
  load_csv_data(db_connection, table_name, csv_file)

finally:
  # Close the database connection
  if db_connection:
    db_connection.close()
    print("Database connection closed.")

print("Script completed.")
