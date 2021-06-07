import bicimad.transformers as T


from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA


pipelines_data = {
        'stations': {
            'type': 'stations',
            'default_sample': '202103_station_0.json',
            'args': 'station_id',
            'pipeline': lambda args: Pipeline(stages = [
                T.FlattenStations(),
                T.SelectStationsData(),
                T.SelectStation(station_id = args[0]),
                T.DatetimeExtractStations(),
                T.WeekDays(),
                T.GroupByHour()
                ])
            },
        'stations_pca': {
            'type': 'stations',
            'default_sample': '202103_station_0.json',
            'args': 'station_id',
            'pipeline': lambda args: Pipeline(stages = [
                T.FlattenStations(),
                T.SelectStationsData(),
                T.SelectStation(station_id = args[0]),
                T.DatetimeExtractStations(),
                T.WeekDays(),
                T.GroupByDate(),
                VectorAssembler(inputCols=[str(i) for i in range(24)],
                    outputCol='features', handleInvalid='keep'),
                StandardScaler(inputCol='features', outputCol='scaled_features',
                    withStd=False, withMean=True),
                PCA(k=2, inputCol='scaled_features', outputCol='pca_features'),
                T.FlattenPCAVariables(k = 2, input_col= "pca_features"),
                T.SelectPCAVariables(k = 2)
                ])
            },
        }
