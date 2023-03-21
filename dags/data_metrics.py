from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from df_utils import *

def data_metrics_ftn():
    """
    Read the parquet files from the output folder, analyses metrics and writes the results to a text file in output folder
    """

    # create a sparksession with the below mentioned app name
    spark = SparkSession.builder.appName("analyse-metrics").getOrCreate()

    # Set the path to the folder containing the Parquet files
    parquet_folder_path = "/opt/airflow/output/cleansed_data/"

    # Read all Parquet files in the folder to a datarrame
    df = spark.read.parquet(parquet_folder_path)

    # analyse the metrics
    unique_users = df.select('first_name', 'last_name', 'email').distinct().count()
    most_represented = df.groupBy('country').agg({'country': 'count'}).orderBy('count(country)', ascending=False).first()
    least_represented = df.groupBy('country').agg({'country': 'count'}).orderBy('count(country)').first()

    # print the computed values
    print("--------------------------------------------------------------------------------------------------")
    print('Unique users:',unique_users)
    print('Most represented country:', most_represented['country'], 'with', most_represented['count(country)'], 'users')
    print('Least represented country:', least_represented['country'], 'with', least_represented['count(country)'],
          'users')
    print("--------------------------------------------------------------------------------------------------")


    # write the results to a file in specified location
    with open('/opt/airflow/output/data_metrics_final_output.txt', 'w') as f:
        f.write(
            f'Unique users: {unique_users}\n')
        f.write(
            f'Most represented country: {most_represented["country"]} ; with users count: {most_represented["count(country)"]} \n')
        f.write(
            f'Least represented country: {least_represented["country"]} ; with users count: {least_represented["count(country)"]} \n')


if __name__== '__main__':
    data_metrics_ftn()