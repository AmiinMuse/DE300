import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from cleaning import clean_data
from cleaning import fix_data_type


# Database parameters
db_params = {
    'host': 'localhost',
    'port': 5432,
    'database': 'heartdisease',
    'user': 'postgres',
    'password': 'amiin123'
}

# load and clean data
csv_file_path = 'heart_disease.csv'
df = pd.read_csv(csv_file_path)


    
column_data_types = {col: fix_data_type(df[col]) for col in df.columns}


# clean data 
df = clean_data(df)
# print(df.head())



sql_create_table = """
CREATE TABLE IF NOT EXISTS heartdisease_data (
    age INTEGER,  -- Assuming 'age' should be an INTEGER
    sex REAL,
    painloc REAL,
    painexer REAL,
    relrest REAL,
    pncaden REAL,
    cp REAL,
    trestbps REAL,
    htn REAL,
    chol REAL,
    smoke REAL,
    cigs REAL,
    years REAL,
    fbs REAL,
    dm REAL,
    famhist REAL,
    restecg REAL,
    ekgmo REAL,
    ekgday REAL,  -- Check this column, the name might be incorrectly formatted as 'ekgday(day'
    ekgyr REAL,
    dig REAL,
    prop REAL,
    nitr REAL,
    pro REAL,
    diuretic REAL,
    proto REAL,
    thaldur REAL,
    thaltime REAL,
    met REAL,
    thalach REAL,
    thalrest REAL,
    tpeakbps REAL,
    tpeakbpd REAL,
    dummy REAL,
    trestbpd REAL,
    exang REAL,
    xhypo REAL,
    oldpeak REAL,
    slope REAL,
    rldv5 REAL,
    rldv5e REAL,
    ca REAL,
    restckm REAL,
    exerckm REAL,
    restef REAL,
    restwm REAL,
    exeref REAL,
    exerwm REAL,
    thal REAL,
    thalsev REAL,
    thalpul REAL,
    earlobe REAL,
    cmo REAL,
    cday REAL,
    cyr REAL,
    target REAL
);
"""

# Create an SQLAlchemy engine
db = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
engine = create_engine(db)



with engine.connect() as conn:
    conn.execute(text(sql_create_table))


# Use pandas to insert data directly into the table
try:
    df.to_sql('heartdisease_data', con=engine, if_exists='append', index=False)
    print("Data inserted successfully into 'heartdisease_data'.")
except Exception as e:
    print("An error occurred during data insertion:", e)

