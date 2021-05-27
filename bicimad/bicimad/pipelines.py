import bicimad.transformers as T

from pyspark.ml import Pipeline

stations_pca_data= {
        'type': 'stations',
        'default_sample': '202103_station_0.json',
        'pipeline': Pipeline(stages=[T.PrepareStations()]),
        }

pipelines_data = {
        'stations_pca': stations_pca_data
        }


__ALL__ = ['pipelines_data']
