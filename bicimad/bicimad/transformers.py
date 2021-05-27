from pyspark.ml import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode

class PrepareStations(Transformer):
    def _transform(self, df: DataFrame):
        return df\
                .select('_id', explode('stations'))

