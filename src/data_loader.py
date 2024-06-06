from pyspark.sql import SparkSession

class DataLoader:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder \
            .appName("AccidentAnalysis") \
            .getOrCreate()
    
    def load_data(self):
        data = {}
        for key, path in self.config['data_sources'].items():
            data[key] = self.spark.read.csv(path, header=True, inferSchema=True)
        return data
