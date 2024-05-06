import pandas as pd
    
# clean data 
def clean_data(df):

    # calculate the median age excluding negative values and replace the negative values
    df['age'] = pd.to_numeric(df['age'], errors='coerce')
    median_age = df[df['age'] > 0]['age'].median()
    df.loc[df['age'] < 0, 'age'] = median_age


    # # median imputation for numeric variables
    for col in ['trestbps', 'chol']:
        median_value = df[col].median()
        df[col].fillna(value=median_value, inplace=True)

    # # mode imputation for categorical variables
    for col in ['painloc', 'painexer']:
        if df[col].isnull().any():
            mode_value = df[col].mode()[0]
            df[col].fillna(value=mode_value, inplace=True)


    # Convert numeric columns safely
    numeric_cols = ['trestbps', 'chol']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Consider dropping rows with NaN values in critical columns only
    critical_columns = ['age', 'trestbps', 'chol'] 
    df.dropna(subset=critical_columns, inplace=True)

    # trim spaces from string columns
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)



    return df

# fix data types for columsn
def fix_data_type(df):
     # fix data types
    if pd.api.types.is_integer_dtype(df):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(df):
        return 'REAL'




