import os

from datetime import date

def cleanup(path,file_name):
    """ this file will clean up output of spark jobs """

    for f in os.listdir(path):
        
        try:
            if f.split('.')[1] not in ['csv','parquet','orc','txt']:
                os.remove(path+"\\"+f)
                
            else:
                os.rename(path+"\\"+f, path+"\\"+file_name)

        except IndexError:
            os.remove(path+"\\"+f)
    
    