import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
import random
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import os

# # Set the aesthetic style of the plots
sns.set_style("whitegrid")

# Create an engine instance
engine = create_engine("postgresql://postgres:amiin123@localhost:5432/heartdisease")

# Load data into a DataFrame
df = pd.read_sql("SELECT * FROM heartdisease_data", engine)

print(df.head())


# analyzing Missing Values:
print("Checking missing values or NaN: ")
print(df.isnull().sum())

# data description
print("original data description...")
print(df.describe())

# Identifying categorical and numerical columns
categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
numerical_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()

# Removing the target variable from numerical_cols if it's there
numerical_cols = [col for col in numerical_cols if col != 'target']


# Creating transformers
transformers = [
    ('num', StandardScaler(), numerical_cols),
    ('cat', OneHotEncoder(), categorical_cols)
]

# Combining transformers into a single preprocessor
preprocessor = ColumnTransformer(transformers)

# Applying transformations
print("After applied tranformations descriptions... ")
df_transformed = preprocessor.fit_transform(df)

# Optional: Convert transformed data back to a DataFrame
# This part assumes you want to see the transformed DataFrame
transformed_columns = preprocessor.transformers_[0][2] + \
    list(preprocessor.named_transformers_['cat'].get_feature_names_out(categorical_cols))
df_transformed = pd.DataFrame(df_transformed, columns=transformed_columns)
print("transformed DataFrame: ")
print(df_transformed.head())

# create new directory for plots:
plot_directory = "plots"
if not os.path.exists(plot_directory):
    os.makedirs(plot_directory)

# Box plots for numerical features
for col in numerical_cols:
    plt.figure(figsize=(10, 4))
    sns.boxplot(x=df[col])
    plt.title(f'Box plot of {col}')
    # plt.show()
    plt.savefig(f"{plot_directory}/boxplot_{col}.png")
    plt.close()

# the scatter plot between 'age' and 'trestbps'
plt.figure(figsize=(10, 6))
sns.scatterplot(x=df['age'], y=df['trestbps'], hue=df['target'])
plt.title('Scatter plot between Age and Resting Blood Pressure')
plt.savefig(f"{plot_directory}/scatter_age_trestbps.png")
# plt.show()
plt.close() 



# add df_transformed to the database
try:
    df_transformed.to_sql('cleaned_heartdisease_data', con=engine, if_exists='replace', index=False)
    print("Data successfully stored in 'cleaned_heartdisease_data' table.")
except Exception as e:
    print(f"An error occurred: {e}")

