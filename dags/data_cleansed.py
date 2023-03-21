from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from df_utils import *

def data_cleansed_ftn():
    """
    Cleanses the raw data in json file and saves the cleansed data to the specified output path as parquet
    """

    # create a sparksession with the below mentioned app name
    spark = SparkSession.builder.appName("cleanse-raw-data").getOrCreate()

    #read the data from the json file into a dataframe
    df = spark.read.option("multiLine", "true").json("/opt/airflow/output/raw_data.json")

    #cleanse the data in dataframe by applying various transformations
    df_cleansed = df.withColumn('country', initcap(cleanse_string(df.country))) \
                    .withColumn("first_name", cleanse_string(df.first_name)) \
                    .withColumn("last_name", cleanse_string(df.last_name)) \
                    .withColumn('gender', cleanse_string(df.gender)) \
                    .withColumn("email", cleanse_email(df.email)) \
                    .withColumn("id",cleanse_id(df.id)) \
                    .withColumn('is_valid_ip_address', check_ip_format(df.ip_address)) \
                    .withColumn('date', cleanse_date(df.date)) \

    #Display the sample cleansed data
    df_cleansed.show(5)

    #write the cleansed data into the parquet file at the specified location
    df_cleansed.write.parquet("/opt/airflow/output/cleansed_data",mode="overwrite")

if __name__== '__main__':
    data_cleansed_ftn()