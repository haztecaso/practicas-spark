#!/usr/bin/env python3
from bicimad import DataLoader
from pyspark.sql.functions import col, udf, dayofweek, hour
from pyspark.sql import Row
from pyspark.ml.feature import VectorAssembler, PCA, StandardScaler
from pyspark.sql.types import FloatType, ArrayType
import numpy as np

def prepare(df):
    return df\
          .filter(df['user_type'] != 3)\
          .drop('track', '_id')\
          .withColumn('day_of_week', dayofweek('unplug_hourTime'))\
          .withColumn('hour', hour('unplug_hourTime'))


def assemble_and_center(df):
    vecAssembler = VectorAssembler(outputCol="features", handleInvalid='skip')
    vecAssembler.setInputCols(['ageRange', 'idunplug_station', 'idplug_station', 'travel_time', 'hour', 'day_of_week'])
    assembled = vecAssembler.transform(df)
    centerer = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=False, withMean=True)
    return centerer.fit(assembled).transform(assembled)


def make_pca_model(df, k):
    pca = PCA(k=k, inputCol="scaled_features", outputCol="pca_features")
    model = pca.fit(df)
    return model


def extract_components(k):
    def inner(row):
        values = row['pca_features'].toArray()
        cols = {}
        for i in range(k):
            cols[f'PCA{i+1}'] = values[i].item()
        return Row(**cols)
    return inner


def make_pca_df(df, model, k):
    return model.transform(df).select('pca_features')\
            .rdd.map(extract_components(k)).toDF()


def test():
    sample = './samples/202103_movements_random_sample_1000.json'
    data = DataLoader(appName = 'bicimad-test', test_file=sample)
    df = data.get_df()
    df = prepare(df)
    df.printSchema()
    df = assemble_and_center(df)
    pca_model = make_pca_model(df, 2)
    pca_df = make_pca_df(df, pca_model, 2)
    pca_df.printSchema()
    fig = pca_df.toPandas().plot.scatter(x='PCA1', y='PCA2').get_figure()
    fig.savefig("pca_plot.pdf")

if __name__ == '__main__':
    test()
