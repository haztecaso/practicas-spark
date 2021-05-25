#!/usr/bin/env python3
from bicimad import DataLoader
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

PATH = 'file:///home/alattes/'

def sample_n(df:DataFrame, n:int):
    count = df.count()
    n = n if count > n else count
    fraction = n / df.count()
    return df.sample(False, fraction)

def sample_01(data_loader:DataLoader):
    df_last_month = data_loader.get_df(start="202103", end="202103", type="movements")
    sample_n(df_last_month, 1000).coalesce(1).write\
            .save(path = '202103_movements_random_sample_1000',
                    format = 'json', mode = 'overwrite')

def sample_02(data_loader:DataLoader):
    df = data_loader.get_df(type="movements")\
            .filter(col('track').isNotNull())
    sample_n(df, 100).coalesce(1).write\
            .save(path = 'movements_gps_random_sample_100',
                    format = 'json', mode = 'overwrite')

if __name__ == '__main__':
    data = DataLoader(appName = "bicimad-resumen")
    # sample_01(data)
    # sample_02(data)
