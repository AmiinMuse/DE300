import pandas as pd 
import numpy as np

def clean_data(data):
    # age cannot be negative or zero 
    data['age'] = pd.to_numeric(data['age'], errors='coerce')
    median_age = data[data['age'] > 0]['age'].median()
    data.loc[data['age'] < 0, 'age'] = median_age
    
    # a. painloc, painexer

    
    # b. tresbps: Replace values less than 100 mm Hg
    data['trestbps'] = data['trestbps'].apply(lambda x: x if x >= 100 else np.nan)
    
    # c. oldpeak: Replace values less than 0 and those greater than 4
    data['oldpeak'] = data['oldpeak'].apply(lambda x: np.nan if x < 0 or x > 4 else x)

    # d. thaldur, thalach: Replace the missing values
    data['thaldur'] = data['thaldur'].fillna(data['thaldur'].median())
    data['thalach'] = data['thalach'].fillna(data['thalach'].median())
    
    # e. fbs, prop, nitr, pro, diuretic: Replace the missing values and values greater than 
    for col in ['fbs', 'prop', 'nitr', 'pro', 'diuretic']:
        data[col] = data[col].apply(lambda x: np.nan if pd.isna(x) or x > 1 else x)
        data[col] = data[col].fillna(data[col].median())
        
    # f. exang, slope: Replace the missing values
    data['exang'] = data['exang'].fillna(data['exang'].mode()[0])  # Assuming categorical, replace with mode
    data['slope'] = data['slope'].fillna(data['slope'].mode()[0])  # Assuming categorical, replace with mode

    return data 