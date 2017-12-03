import configparser

class baseConfig:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('/home/trantu/Desktop/engine_recommendation.git/trunk/configure/config.ini')
        self.update_trending = self.config['save-log']['update-trending']
        self.host_name = self.config['cassandra-config']['host-name']
    pass