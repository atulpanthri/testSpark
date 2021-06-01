import subprocess as sb
from configparser import ConfigParser
import pandas




class Etl:
    """
     this is customized ETL class , provide ready made code and promote code 
      resuability"""


    def create_directory(self,path):

        sb.call(["mkdir",path])


    def copy_files(self, src_path, dest_path):
        " this method will copy file from 1 place to another"

        sb.call(['copy',src_path,dest_path],shell=True)

    def get_df_postgres_query(self, spark, config,query):

        """ This functionality will connect to database and execute query 
            and create dataframe """


        config_parser = ConfigParser()

        config_parser.read(r'D:\spark\testSpark\coreUtils\config.ini')

        print(config_parser.sections())

        db_config = config_parser['Database']

        url = db_config['url']
        driver = db_config['driver']
        user   = db_config['user']
        password = db_config['password']
        dbTable  = db_config['dbTable']

        df = spark.read.format("jdbc")\
                .option("url",url)\
                .option("driver",driver)\
                .option("user",user) \
                .option("password",password)\
                .option("dbtable",query)

        return df

    def test(self):
        print("this is test")


#etl = Etl()

#etl.get_df_postgres_query('spark', 'config','query')
