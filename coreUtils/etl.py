import configparser
import subprocess as sb
from configparser import ConfigParser
from coreUtils.feedConfig import FeedConnection
from datetime import date
import os 


class Etl:
    """
     this is customized ETL class , provide ready made code and promote code 
      resuability"""


    def __init__(self,feed_key):

        feedConn = FeedConnection()
        self.config = feedConn.get_feed_config(feed_key)

    @staticmethod
    def create_directory(path):

        if not os.path.exists(path):
            os.makedirs(path)

        #sb.call(["mkdir",path])


    def copy_files(self, src_path, dest_path):
        " this method will copy file from 1 place to another"

        sb.call(['copy',src_path,dest_path],shell=True)

    def get_df_postgres_query(self, spark,feed_key):

        """ This functionality will connect to database and execute query 
            and create dataframe """

        
        db_properties={}

        db_url = self.config['Database Url']+"/"+self.config["Database Name"]
        db_properties['user']=self.config['User Name']
        db_properties['password']=self.config['Password']
        db_properties['driver']=self.config['Driver']

        print(db_url)
        print(db_properties)

        # df = spark.read \
        #     .format("jdbc")\
        #     .option("url", r"jdbc:postgresql://database-1.cfgcrzvguhx1.ap-southeast-1.rds.amazonaws.com:5432/postgres") \
        #     .option("dbtable", "emp") \
        #     .option("user", "postgres") \
        #     .option("password", "Etlpostgres") \
        #     .option("driver","org.postgresql.Driver")\
        #     .load()

        df = spark.read.jdbc(url=db_url,table="emp",properties=db_properties).cache()
    
        return df

    def save_df_to_postgres(self,df):

        db_properties={}

        db_url = self.config['Database Url']+"/"+self.config["Database Name"]
        db_properties['user']=self.config['User Name']
        db_properties['password']=self.config['Password']
        db_properties['driver']=self.config['Driver']

        target_table = self.config['Target Table Name']

        print(target_table)
        df.write.mode("append").jdbc(url=db_url,table=target_table,properties =db_properties)

    @staticmethod
    def def_stringify_date():
        #this method give today date in string format.

        return date.today().strftime("%Y_%m_%d")


    def save_df_to_hive(self,df):
        #this function will save dataframe from to file

        target_file_format = self.config['Target File format']
        target_file_name = self.config['Target File Name']
        target_table_name = self.config['Target Table Name']
        partition = Etl.def_stringify_date()
        
        path =r"D:\user\app\data\ad\staging\ad"

        full_path = os.path.join(path, target_table_name,partition)
        
        print(full_path)
        #Etl.create_directory(full_path)
        df.write.format(target_file_format).option("header","true").save(full_path)
    

    def test(self):
        print("this is test")

# etl = Etl()

# etl.save_df_to_hive('spark', 'config','query')
