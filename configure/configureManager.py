import configparser

class baseConfig:
    def __init__(self):
        self.config = configparser.ConfigParser()
        # self.config.read('/home/tutn6/Desktop/engine_recommendation.git/trunk/configure/config.ini')
        self.config.read('/home/tutn6/Desktop/engine_recommendation.git/trunk/configure/config.ini')
        # self.config.read('config.ini')
        self.host_name = self.config['cassandra-config']['host-name']
        self.keyspace = self.config['cassandra-config']['keyspace']


class collaborativeConfig(baseConfig):
    def __init__(self):
        super().__init__()
        self.path_log_process = self.config['colla-filtering-default']['collaborative-filtering']
        self.path_log_update = self.config['colla-filtering-default']['collaborative-update']
        self.maxIter=self.config['colla-filtering-default']['maxIter']
        self.regParam=self.config['colla-filtering-default']['regParam']
        self.coldStartStrategy=self.config['colla-filtering-default']['coldStartStrategy']
        self.top_movie=self.config['colla-filtering-default']['top-movie']

class updateTrendingConfig(baseConfig):
    def __init__(self):
        super().__init__()
        self.update_trending = self.config['trending-default']['update-trending']

class similarityConfig(baseConfig):
    def __init__(self):
        super().__init__()
        self.path_log_process = self.config['similarity-item-default']['path-log-process-data']
        self.path_log_update = self.config['similarity-item-default']['path-log-update']
        self.offsets = self.config['similarity-item-default']['offsets']
        self.top_moive = self.config['similarity-item-default']['top-moive']

class lastActionConfig(baseConfig):
    def __init__(self):
        super().__init__()
        self.path_log_process = self.config['last-action-default']['path-log-process-data']
        self.path_log_update = self.config['last-action-default']['path-log-update']
        self.top_moive = self.config['last-action-default']['top-movie']

class whatIsPopularConfig(baseConfig):
    def __init__(self):
        super().__init__()
        self.path_log_process = self.config['what-is-popular-default']['path-log-process-data']
        self.path_log_update = self.config['what-is-popular-default']['path-log-update']
        self.top_moive = self.config['what-is-popular-default']['top-movie']

class specificProfileConfig(baseConfig):
    def __init__(self):
        super().__init__()
        self.path_log_process = self.config['specific-profile']['path-log-process-data']
        self.path_log_update = self.config['specific-profile']['path-log-update']

class userProfileConfig(baseConfig):
    def __init__(self):
        super().__init__()
        self.path_log_process = self.config['user-profile']['path-log-process-data']
        self.path_log_update = self.config['user-profile']['path-log-update']

if __name__ == '__main__':
    a = collaborativeConfig()
    print(a.path_log_process)