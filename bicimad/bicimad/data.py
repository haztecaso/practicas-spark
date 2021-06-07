# -*- coding: utf-8 -*-

import pickle, subprocess
from datetime import datetime
from operator import itemgetter

from bicimad.globals import *

from pyspark.sql import SparkSession
import pyspark.sql.types as tp
from pyspark.sql.functions import explode


is_iterable = lambda obj: hasattr(obj, '__iter__')


class Data():
    def __init__(self, **kwargs):
        self.data_loader = kwargs['data_loader']
        self.spark = self.data_loader.spark
        self.sample = kwargs.get('sample', False)
        self.files = kwargs['files']
        self.no_schema = kwargs.get('no_schema', False)
        self.df = None

    def _init_spark(self):
        if not self.spark:
            print('Initializing SparkSession')
            self.spark = SparkSession.builder\
                    .appName(self.data_loader.appName)\
                    .getOrCreate()

    def load_df(self):
        self._init_spark()
        read_options = { 'timestampFormat': TIMESTAMP_FORMAT }
        if not self.no_schema:
            read_options['schema'] = self.schema
        print(f'Reading {len(self.files)} json files into {type(self).__name__} object')
        self.df = self.spark.read.json(self.files, **read_options)


class Movements(Data):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.schema = tp.StructType()\
                .add('_id', tp.StructType().add('$oid', tp.StringType(), False), False)\
                .add("user_day_code", tp.StringType(), False)\
                .add("idplug_base", tp.IntegerType(), False)\
                .add("idunplug_base", tp.IntegerType(), False)\
                .add("travel_time", tp.IntegerType(), False)\
                .add("idunplug_station", tp.IntegerType(), False)\
                .add("idplug_station", tp.IntegerType(), False)\
                .add("ageRange", tp.IntegerType(), False)\
                .add("user_type", tp.IntegerType(), False)\
                .add("unplug_hourTime", tp.TimestampType(), False)\
                .add("zip_code", tp.StringType(), True)


class Stations(Data):
    element_schema = tp.StructType()\
            .add("activate", tp.IntegerType(), False)\
            .add("address", tp.StringType(), False)\
            .add("dock_bikes", tp.IntegerType(), False)\
            .add("free_bases", tp.IntegerType(), False)\
            .add("id", tp.IntegerType(), False)\
            .add("latitude", tp.StringType(), False)\
            .add("light", tp.IntegerType(), False)\
            .add("longitude", tp.StringType(), False)\
            .add("name", tp.StringType(), False)\
            .add("no_available", tp.IntegerType(), False)\
            .add("number", tp.StringType(), False)\
            .add("reservations_count", tp.IntegerType(), False)\
            .add("total_bases", tp.IntegerType(), False)
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.schema = tp.StructType()\
                .add('_id', tp.TimestampType() ,False)\
                .add("stations", tp.ArrayType(self.element_schema), False)

    def load_df(self):
        super().load_df()

class DataLoader():
    def __init__(self, **kwargs):
        self.dir = kwargs.get('dir','/public_data/bicimad')
        self.cache = kwargs.get('cache_file', 'cache_files.data')
        self.appName = kwargs.get('appName', 'bicimad')
        self.spark = kwargs['spark'] if 'spark' in kwargs else None
        if self.spark:
            self
        self.test = False
        self.files = []
        if kwargs.get('test_file', None):
            self.test = True
            self.files.append(kwargs['test_file'])
        else:
            self._load_cache()
            if len(self.files) == 0:
                self._load_hdfs()
                self._write_cache()

    def get_files(self, **kwargs):
        if self.test:
            return self.files
        date_from = kwargs.get('start', self.files[0]['date'])
        if type(date_from) == str:
            date_from = datetime.strptime(date_from, DATE_FORMAT)
        date_to = kwargs.get('end', self.files[-1]['date'])
        if type(date_to) == str:
            date_to = datetime.strptime(date_to, DATE_FORMAT)
        files = filter(lambda f: f['date'] >= date_from and f['date'] <= date_to, self.files)
        if 'type' in kwargs:
            assert kwargs['type'] in ['movements', 'stations'], 'Invalid type'
            files = filter(lambda f: f['type'] == kwargs['type'], files)
        if kwargs.get('path', False):
            files = map(itemgetter('path'), files)
        return files

    def get_data(self, **kwargs):
        assert 'type' in kwargs, 'You must specify a type to create a DF!'
        data_type = kwargs['type']
        files = list(self.get_files(**kwargs, path=True))
        if data_type == 'movements':
            return Movements(data_loader = self, files=files, spark=self.spark)
        elif data_type == 'stations':
            return Stations(data_loader = self, files=files, spark=self.spark)

    def _load_hdfs(self):
        print(f'Getting list of json files in hdfs folder {self.dir}')
        cmd = f'{HDFS_PATH} dfs -ls {self.dir}'.split(' ')
        cmd_out = subprocess.run(cmd, stdout=subprocess.PIPE).stdout.decode()
        self.files = []
        for match in re.finditer(LS_REGEX, cmd_out):
            path = match.group(3)
            file_info = re.match(LS_FILEINFO, path).groups()
            date = datetime.strptime(file_info[0], DATE_FORMAT)
            self.files.append({
                'path': path,
                'size': match.group(2),
                'permissions': match.group(1),
                'date': date,
                'year': date.year,
                'month': date.month,
                'type': file_info[1],
                })
        self.files.sort(key=itemgetter('date'))

    def _write_cache(self):
        assert len(self.files) > 0, 'Cannot cache empty list of files'
        with open(self.cache, 'wb') as cache:
            pickle.dump(self.files, cache)

    def _load_cache(self):
        try:
            with open(self.cache, 'rb') as cache:
                print(f"Loading cache file '{self.cache}'")
                self.files = pickle.load(cache)
        except FileNotFoundError:
            print(f"Cache file '{self.cache}' not found")


__all__ = ['DataLoader', 'Movements', 'Stations']
