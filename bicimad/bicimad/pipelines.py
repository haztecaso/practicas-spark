import bicimad.transformers as T

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA


pipelines_data = {
        'stations_pca': {
            'type': 'stations',
            'default_sample': '202103_station_0.json',
            'pipeline': Pipeline(stages = [
                T.FlattenStations(),
                T.SelectStationsData(),
                T.SelectStation(station_id = 1),
                T.DatetimeExtractStations(),
                T.ReduceStationHours(),
                VectorAssembler(inputCols=[
                        'sum(dock_bikes)',
                        'sum(free_bases)',
                        # 'day_of_week'
                        ],
                    outputCol='features'
                    ),
                # StandardScaler(inputCol='features', outputCol='scaled_features',
                #     withStd=False, withMean=True),
                # T.GroupByDays()
                # PCA(k=2, inputCol='scaled_features', outputCol='pca_features'),
                # T.FlattenPCAVariables(k = 2, input_col= "pca_features")
                ])
            },
        }
