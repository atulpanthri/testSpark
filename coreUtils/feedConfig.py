import configparser
import psycopg2
from configparser import ConfigParser


class FeedConnection:

    def __init__(self):

        config_parser = ConfigParser()
        config_parser.read(r"D:\spark\testSpark\config\config.ini")

        feed_info = config_parser['feedDatabase']
        self.server = feed_info['url']
        self.database = feed_info['database']
        self.user     = feed_info['user']
        self.password = feed_info['password']
        self.port   = feed_info['port']
        self.feed_query = feed_info["feed_query"]

        
    def get_connection(self):
        self.conn = psycopg2.connect(f"dbname={self.database} host={self.server} user={self.user} password={self.password} port={self.port}")
        return self.conn

    def get_feed_config(self,feed_key):
        
        self.feed_query = self.feed_query.replace("??",str(feed_key))

        conn = self.get_connection()

        cur = conn.cursor()

        cur.execute(self.feed_query)

        feed_attr= []

        for rec in cur:
            out_folder_name = rec[3]
            feed_attr.append((rec[6],rec[7]))

        print(feed_attr)

        config_dict = {i[0]:i[1] for i in feed_attr}

        
        config_dict["Out Folder Name"] = out_folder_name

        print(config_dict)

        conn.commit()
        cur.close()
        conn.close()

        return config_dict


fc = FeedConnection()

fc.get_feed_config(1)