from jobs.sample1 import extraction
from pyspark.sql import SparkSession
from coreUtils.etl import Etl

#def loading(df,):


if __name__=="__main__":


        spark = SparkSession.builder.appName("sample2").master("local[1]").\
                getOrCreate()

        etl = Etl(1)
        df = etl.get_df_postgres_query(spark,1)
        #etl.save_df_to_hive(df)

        etl.save_df_to_postgres(df)


        spark.stop()