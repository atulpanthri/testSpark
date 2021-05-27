from utils import cleanup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType
import json
import os
from pyspark.sql.functions import regexp_replace, col
from datetime import date

import subprocess as sp


#from pyspark.sql.functions import regexp_replace, col

def config_parser(team,control_id,path):
    "the method will parse file return "

    json_exten = "_config.json"
    text_exten = "_schema.txt"

    ctntl_id = str(control_id)
    commonPath= os.path.join(path,team,team+'_'+ctntl_id)
    
    path_config_file  = os.path.join(commonPath,team+'_'+ctntl_id+json_exten)
    path_schema_file  = os.path.join(commonPath,team+'_'+ctntl_id+text_exten)  

    with open (path_schema_file,'r') as schema_file:
        schema = schema_file.readlines()

    
    with open (path_config_file,'r') as config_file:
        config = json.load(config_file)

    return(schema,config)

def extraction(team, control_id ,path):
    """
        extraction of files from source. 
        putting into staging area.

    """
    sourcePath      = config["sourcePath"]
    destinitionPath = config["destinitionPath"]
    fileFormat      = config["fileFormat"]
    fileSeparator   = config["fileSeparator"]
    fileName        = config["srcFileName"]

    dateString = date.today().strftime("%Y%m%d")

    sourceFile = os.path.join(sourcePath,fileName)

    desFile = os.path.join(destinitionPath,team,str(control_id),dateString,fileName)

    #print(sourceFile)
    #print(desFile)

    #sourceFile = rsourceFile
    print(sourceFile)
    print(desFile)

    sp.call(["copy",sourceFile,desFile],shell = True)


def transformation(team, control_id ,path, spark):
    """
     this function do extraction

    """
    schema,config = config_parser(team,control_id,path)

    schema = schema[0]
    print(schema)

    sourcePath      = config["sourcePath"]
    destinitionPath = config["destinitionPath"]
    fileFormat      = config["fileFormat"]
    fileDelimeter   = config["fileDelimeter"]
    fileName        = config["srcFileName"]
    destFileName    = config["destFileName"]
    destFileExt     = config["destiFileExt"]
    table           = config["tableName"]

    dateString  = date.today().strftime("%Y%m%d")

    file = os.path.join(sourcePath,fileName)

    out_file = os.path.join(destinitionPath,team,table,dateString)

    data = spark.read.format(fileFormat).schema(schema).option("sep",fileDelimeter ).\
            option("mode","failFast").load(file)

    print(out_file)

    data1 = data.withColumn("manager_id", regexp_replace(col("manager_id"),"null","0")).\
            withColumn("department_id",regexp_replace(col("department_id"),"null","0"))
    
    data2 = data1.withColumn("manager_id",col("manager_id").cast('int')).\
            withColumn("department_id",col("department_id").cast('int'))

   
    data2.write.csv(out_file)
    
    cleanup.cleanup(out_file,table+'_'+dateString+'.'+destFileExt)
  

if __name__ == "__main__":
    
    with open ('D:/spark-project/configs/etl_config.json', 'r') as f:
        conf_dict = json.load(f)

    path = conf_dict['configPath']

    #print(path)

    schema,config = config_parser('ad',100,path)

    extraction("ad",100,path)

    
    #spark = SparkSession.builder.appName("sample1").master("local[*]").getOrCreate()

    #extraction("ad", 100 ,path, spark)
    
    #result = config_parser("ad",100,path)

    #print(result[1])
