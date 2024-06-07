# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from sqlalchemy import create_engine
# import pandas as pd
# import os
# import requests
# from scrapy import Selector
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, monotonically_increasing_id, mean
# from pyspark.ml.feature import VectorAssembler
# from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
# from pyspark.ml.evaluation import BinaryClassificationEvaluator
# from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Amiin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'assignment_dag',
    default_args=default_args,
    description='Assignment 4 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 7),
    catchup=False,
)

# Define the functions for each task
def load_data(**kwargs):
    import pandas as pd
    from sqlalchemy import create_engine
    # Assuming the database is PostgreSQL
    engine = create_engine("postgresql://postgres:amiin123@localhost:5432/heartdisease")
    df = pd.read_sql("SELECT * FROM heartdisease_data", engine)
    df.to_csv('/tmp/heartdisease_data.csv', index=False)

def clean_data(**kwargs):
    import pandas as pd
    # Load data
    df = pd.read_csv('/tmp/heart_disease.csv')

    # Clean and impute data (code from your cleaning functions)
    def clean_data(df):
        df['age'] = pd.to_numeric(df['age'], errors='coerce')
        median_age = df[df['age'] > 0]['age'].median()
        df.loc[df['age'] < 0, 'age'] = median_age

        for col in ['trestbps', 'chol']:
            median_value = df[col].median()
            df[col].fillna(value=median_value, inplace=True)

        for col in ['painloc', 'painexer']:
            if df[col].isnull().any():
                mode_value = df[col].mode()[0]
                df[col].fillna(value=mode_value, inplace=True)

        df['trestbps'] = pd.to_numeric(df['trestbps'], errors='coerce')
        df['chol'] = pd.to_numeric(df['chol'], errors='coerce')
        df.dropna(subset=['age', 'trestbps', 'chol'], inplace=True)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        return df

    df_cleaned = clean_data(df)
    df_cleaned.to_csv('/tmp/heart_disease_cleaned.csv', index=False)

def standard_eda(**kwargs):
    import pandas as pd
    import seaborn as sns
    import matplotlib.pyplot as plt
    from sklearn.preprocessing import StandardScaler, OneHotEncoder
    from sklearn.compose import ColumnTransformer
    import os

    df = pd.read_csv('/tmp/heart_disease_cleaned.csv')
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    numerical_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    numerical_cols = [col for col in numerical_cols if col != 'target']

    transformers = [
        ('num', StandardScaler(), numerical_cols),
        ('cat', OneHotEncoder(), categorical_cols)
    ]

    preprocessor = ColumnTransformer(transformers)
    df_transformed = preprocessor.fit_transform(df)
    transformed_columns = preprocessor.transformers_[0][2] + \
        list(preprocessor.named_transformers_['cat'].get_feature_names_out(categorical_cols))
    df_transformed = pd.DataFrame(df_transformed, columns=transformed_columns)
    df_transformed.to_csv('/tmp/standard_eda_transformed.csv', index=False)

    plot_directory = "/tmp/plots"
    if not os.path.exists(plot_directory):
        os.makedirs(plot_directory)

    for col in numerical_cols:
        plt.figure(figsize=(10, 4))
        sns.boxplot(x=df[col])
        plt.title(f'Box plot of {col}')
        plt.savefig(f"{plot_directory}/boxplot_{col}.png")
        plt.close()

    plt.figure(figsize=(10, 6))
    sns.scatterplot(x=df['age'], y=df['trestbps'], hue=df['target'])
    plt.title('Scatter plot between Age and Resting Blood Pressure')
    plt.savefig(f"{plot_directory}/scatter_age_trestbps.png")
    plt.close()

def spark_eda(**kwargs):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, mean, when
    import pandas as pd

    spark = SparkSession.builder.appName("HeartDiseasePrediction").getOrCreate()
    df = spark.read.csv('/tmp/heart_disease_cleaned.csv', header=True, inferSchema=True)

    def clean_data(df):
        df = df.withColumn('age', col('age').cast('float'))
        median_age = df.filter(df['age'] > 0).approxQuantile('age', [0.5], 0.0)[0]
        df = df.withColumn('age', when(col('age') <= 0, median_age).otherwise(col('age')))
        mean_painexer = df.select(mean("painexer")).collect()[0][0]
        df = df.fillna({"painexer": mean_painexer})
        df = df.withColumn("trestbps", when(col("trestbps") < 100, 100).otherwise(col("trestbps")))
        df = df.withColumn("oldpeak", when(col("oldpeak") < 0, 0).otherwise(col("oldpeak")))
        df = df.withColumn("oldpeak", when(col("oldpeak") > 4, 4).otherwise(col("oldpeak")))
        mean_thaldur = df.select(mean("thaldur")).collect()[0][0]
        mean_thalach = df.select(mean("thalach")).collect()[0][0]
        df = df.fillna({"thaldur": mean_thaldur, "thalach": mean_thalach})
        for column in ['fbs', 'prop', 'nitr', 'pro', 'diuretic']:
            mode_value = df.groupBy(column).count().orderBy('count', ascending=False).first()[0]
            df = df.fillna({column: mode_value})
            df = df.withColumn(column, when(col(column) > 1, mode_value).otherwise(col(column)))
        for column in ['exang', 'slope']:
            mode_value = df.groupBy(column).count().orderBy('count', ascending=False).first()[0]
            df = df.withColumn(column, when(col(column).isNull(), mode_value).otherwise(col(column)))
        mode_target = df.groupBy('target').count().orderBy('count', ascending=False).first()[0]
        df = df.withColumn('target', when(col('target').isNull(), mode_target).otherwise(col('target')))
        return df

    df_cleaned = clean_data(df)
    df_cleaned.write.mode("overwrite").csv("/tmp/spark_eda_cleaned.csv", header=True)

def web_scraping(**kwargs):
    import requests
    from scrapy import Selector
    import pandas as pd

    def scrape_data(url):
        response = requests.get(url)
        response.raise_for_status()
        html_content = response.content
        full_sel = Selector(text=html_content)
        tables = full_sel.xpath('//table[@class="responsive-enabled"]')
        rows = tables[1].xpath('.//thead//tr | .//tbody//tr')

        def parse_row(row):
            cells = row.xpath('.//th | .//td')
            row_data = [cell.xpath('.//text()').get().strip() for cell in cells]
            return row_data

        header = parse_row(rows[0])
        table_data = [parse_row(row) for row in rows[1:]]
        df = pd.DataFrame(table_data, columns=header)
        return df

    url1 = 'https://www.abs.gov.au/statistics/health/health-conditions-and-risks/smoking-and-vaping/latest-release'
    df1 = scrape_data(url1)
    df1.to_csv('/tmp/smoking_data_1.csv', index=False)

    url2 = 'https://www.cdc.gov/tobacco/data_statistics/fact_sheets/adult_data/cig_smoking/index.htm'
    response2 = requests.get(url2)
    response2.raise_for_status()
    html_content2 = response2.content
    text = Selector(text=html_content2)
    div_list = text.xpath('//div[contains(@class, "card-body")]')
    div_sex = text.xpath('//div[contains(@class, "card-body") and .//p[contains(text(), "Current cigarette smoking was higher among men than women")]]/ul')
    by_sex_list = div_sex.xpath('.//li/text()').getall()
    div_age = text.xpath('//div[contains(@class, "card-body") and .//p[contains(text(), "Current cigarette smoking was highest among people aged 25–44 years and 45–64 years. Current cigarette smoking was lowest among people aged 18-24 years")]]/ul')
    by_age_list = div_age.xpath('.//li/text()').getall()
    df2 = pd.DataFrame({'Sex': by_sex_list, 'Age': by_age_list})
    df2.to_csv('/tmp/smoking_data_2.csv', index=False)

def feature_engineering_1(**kwargs):
    import pandas as pd
    df = pd.read_csv('/tmp/heartdisease_cleaned.csv')
    df['feature_engineered_1'] = df['age'] * df['trestbps']
    df.to_csv('/tmp/heartdisease_feature_1.csv', index=False)

def feature_engineering_2(**kwargs):
    import pandas as pd
    df = pd.read_csv('/tmp/heartdisease_cleaned.csv')
    df['feature_engineered_2'] = df['chol'] / df['thalach']
    df.to_csv('/tmp/heartdisease_feature_2.csv', index=False)

def train_model_1(**kwargs):
    import pandas as pd
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    df = pd.read_csv('/tmp/heartdisease_feature_1.csv')
    X = df.drop(columns=['target'])
    y = df['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = LogisticRegression()
    model.fit(X_train, y_train)
    # Save model (placeholder, save the model to a file or database)
    import pickle
    with open('/tmp/model_1.pkl', 'wb') as f:
        pickle.dump(model, f)

def train_model_2(**kwargs):
    import pandas as pd
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    df = pd.read_csv('/tmp/heartdisease_feature_2.csv')
    X = df.drop(columns=['target'])
    y = df['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestClassifier()
    model.fit(X_train, y_train)
    # Save model (placeholder, save the model to a file or database)
    import pickle
    with open('/tmp/model_2.pkl', 'wb') as f:
        pickle.dump(model, f)

def select_best_model(**kwargs):
    # Placeholder: Load models and select the best one based on evaluation metrics
    import pickle
    model_1 = pickle.load(open('/tmp/model_1.pkl', 'rb'))
    model_2 = pickle.load(open('/tmp/model_2.pkl', 'rb'))
    # Dummy comparison, replace with actual evaluation metrics
    best_model = model_1 if model_1.score > model_2.score else model_2
    with open('/tmp/best_model.pkl', 'wb') as f:
        pickle.dump(best_model, f)

def evaluate_model(**kwargs):
    import pandas as pd
    from sklearn.metrics import accuracy_score
    import pickle
    df = pd.read_csv('/tmp/heartdisease_cleaned.csv')
    X = df.drop(columns=['target'])
    y = df['target']
    model = pickle.load(open('/tmp/best_model.pkl', 'rb'))
    predictions = model.predict(X)
    accuracy = accuracy_score(y, predictions)
    print(f"Model accuracy: {accuracy}")

# Define tasks
load_data_task = PythonOperator(
    task_id='load_data',
    provide_context=True,
    python_callable=load_data,
    dag=dag,
)

clean_data_task = PythonOperator(
    task_id='clean_data',
    provide_context=True,
    python_callable=clean_data,
    dag=dag,
)

standard_eda_task = PythonOperator(
    task_id='standard_eda',
    provide_context=True,
    python_callable=standard_eda,
    dag=dag,
)

spark_eda_task = PythonOperator(
    task_id='spark_eda',
    provide_context=True,
    python_callable=spark_eda,
    dag=dag,
)

web_scraping_task = PythonOperator(
    task_id='web_scraping',
    provide_context=True,
    python_callable=web_scraping,
    dag=dag,
)

feature_engineering_1_task = PythonOperator(
    task_id='feature_engineering_1',
    provide_context=True,
    python_callable=feature_engineering_1,
    dag=dag,
)

feature_engineering_2_task = PythonOperator(
    task_id='feature_engineering_2',
    provide_context=True,
    python_callable=feature_engineering_2,
    dag=dag,
)

train_model_1_task = PythonOperator(
    task_id='train_model_1',
    provide_context=True,
    python_callable=train_model_1,
    dag=dag,
)

train_model_2_task = PythonOperator(
    task_id='train_model_2',
    provide_context=True,
    python_callable=train_model_2,
    dag=dag,
)

select_best_model_task = PythonOperator(
    task_id='select_best_model',
    provide_context=True,
    python_callable=select_best_model,
    dag=dag,
)

evaluate_model_task = PythonOperator(
    task_id='evaluate_model',
    provide_context=True,
    python_callable=evaluate_model,
    dag=dag,
)

# Set up task dependencies
load_data_task >> clean_data_task
clean_data_task >> [standard_eda_task, spark_eda_task]
standard_eda_task >> feature_engineering_1_task
spark_eda_task >> feature_engineering_2_task
web_scraping_task.set_downstream([feature_engineering_1_task, feature_engineering_2_task])
feature_engineering_1_task >> train_model_1_task
feature_engineering_2_task >> train_model_2_task
[train_model_1_task, train_model_2_task] >> select_best_model_task
select_best_model_task >> evaluate_model_task
