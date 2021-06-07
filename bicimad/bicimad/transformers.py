from pyspark import keyword_only
from bicimad.data import Stations

from pyspark.ml import Transformer
from pyspark.sql import DataFrame, Row
import pyspark.sql.functions as F
from pyspark.ml.param.shared import Param, Params, TypeConverters

class PrintAndAskContinue(Transformer):
    def _transform(self, df: DataFrame):
        df.printSchema()
        df.show(10)
        input('Continue?')
        return df

class FlattenStations(Transformer):
    def _transform(self, df: DataFrame):
        print("  - Flattening stations")
        result = df.select(\
                F.col('_id').alias('timestamp'),\
                F.explode('stations').alias('station')\
                )
        for key in Stations.element_schema:
            result = result.withColumn(key.name, F.col(f'station.{key.name}'))
        return result.drop('station')


class SelectStationsData(Transformer):
    def _transform(self, df: DataFrame):
        print("  - Selecting station parameters")
        return df.select(
                'timestamp',
                'dock_bikes',
                'free_bases',
                )


class SelectStation(Transformer):
    station_id = Param(Params._dummy(), 'station_id', 'stations id to select')

    @keyword_only
    def __init__(self, station_id):
        super().__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def get_id(self):
        return self.getOrDefault('station_id')

    def _transform(self, df: DataFrame):
        id = self.get_id()
        print(f"  - Selecting station with id {id}")
        return df.filter(F.col('id') == id)


class DatetimeExtractStations(Transformer):
    def _transform(self, df: DataFrame):
        print("  - Adding date and hour columns")
        return df\
                .withColumn('date', F.to_date('timestamp'))\
                .withColumn('hour', F.hour('timestamp'))


class WeekDays(Transformer):
    def _transform(self, df: DataFrame):
        print("  - Marking weekends")
        return df\
                .withColumn('weekend', F.dayofweek('timestamp').isin([7, 0]))\


class GroupByHour(Transformer):
    def _transform(self, df: DataFrame):
        print("  - Grouping by hours")
        return df.groupBy('weekend', 'hour').sum('free_bases').na.fill(0)

class GroupByDate(Transformer):
    def _transform(self, df: DataFrame):
        print("  - Grouping by date (days) and pivot by hour.")
        return df.groupBy('date', 'weekend')\
                .pivot('hour').sum('dock_bikes').na.fill(0)


class FlattenPCAVariables(Transformer):
    k = Param(Params._dummy(), 'k', 'PCA k parameter')
    input_col = Param(Params._dummy(), "input_col",
            "input column name.", typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, k:int=2, input_col:str="pca_features"):
        super().__init__()
        self._setDefault(k=2, input_col="pca_features")
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(self, k:int=2, input_col:str="pca_features"):
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def get_k(self):
        return self.getOrDefault('k')

    def get_input_col(self):
        return self.getOrDefault('input_col')

    def _transform(self, df: DataFrame):
        print("  - Flattening PCA variables")
        k = self.get_k()
        input_col = self.get_input_col()
        def extract_components(row:Row):
            values = row[input_col].toArray()
            cols = {}
            for i in range(k):
                cols[f'PCA{i+1}'] = values[i].item()
            return Row(**row.asDict(),**cols)
        return df.rdd.map(extract_components).toDF()


class SelectPCAVariables(Transformer):
    k = Param(Params._dummy(), 'k', 'number of PCA variables')

    @keyword_only
    def __init__(self, k):
        super().__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def get_k(self):
        return self.getOrDefault('k')

    def _transform(self, df: DataFrame):
        k = self.get_k()
        print(f"  - Selecting PCA variables")
        cols = [F.col('date'), F.col('weekend')]
        cols += [F.col(f'PCA{i+1}') for i in range(k)]
        return df.select(cols)


