from bicimad.data import DataLoader

from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col

PATH = 'file:///home/alattes/'

def sample_n(df:DataFrame, n:int):
    count = df.count()
    n = n if count > n else count
    fraction = n / df.count()
    return df.sample(False, fraction)

class Sampler():
    def __init__(self):
        self.data_loader = DataLoader(appName='bicimad sampler')
        self.samplers = [self.sample_01, self.sample_02, self.sample_03]

    def run(self, n:int):
        print(f"Running sampler {n}")
        self.samplers[n-1]()


    def sample_01(self):
        movements = self.data_loader.get_data(start="202103", end="202103", type="movements")
        movements.load_df()
        sample_n(movements.df, 1000).coalesce(1).write\
                .save(path = '202103_movements_random_sample_1000',
                        format = 'json', mode = 'overwrite')

    def sample_02(self):
        movements = self.data_loader.get_data(type="movements")
        movements.load_df()
        df = movements.df.filter(col('track').isNotNull())
        sample_n(df, 100).coalesce(1).write\
                .save(path = 'movements_gps_random_sample_100',
                        format = 'json', mode = 'overwrite')

    def sample_03(self):
        def fn_map(row):
            filtered_stations = [row['stations'][0]]
            return Row(_id=row['_id'], stations = filtered_stations)
        stations = self.data_loader.get_data(start="202103", end="202103", type="stations")
        stations.no_schema = True
        stations.load_df()
        df = stations.df.rdd.map(fn_map).toDF()
        df.coalesce(1).write\
                .save(path = '202103_station_0',
                        format = 'json', mode = 'overwrite')


__all__ = ['Sampler']
