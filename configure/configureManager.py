import configparser

class baseConfig:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('/home/tutn6/Desktop/engine_recommendation.git/trunk/configure/config.ini')
        # self.config.read('config.ini')
        self.host_name = self.config['cassandra-config']['host-name']
        self.keyspace = self.config['cassandra-config']['keyspace']


class collaborativeConfig(baseConfig):
    def __init__(self):
        super().__init__()
        self.path_log_process = self.config['save-log']['collaborative-filtering']
        self.path_log_update = self.config['save-log']['collaborative-update']
        self.maxIter=self.config['colla-filtering-default']['maxIter']
        self.regParam=self.config['colla-filtering-default']['regParam']
        self.coldStartStrategy=self.config['colla-filtering-default']['coldStartStrategy']
        self.top_movie=self.config['colla-filtering-default']['top-movie']

class updateTrendingConfig(baseConfig):
    def __init__(self):
        super().__init__()
        self.update_trending = self.config['save-log']['update-trending']

class similarityConfig(baseConfig):
    def __init__(self):
        super().__init__()
        self.path_log_process = self.config['similarity-item-default']['path-log-process-data']
        self.path_log_update = self.config['similarity-item-default']['path-log-update']