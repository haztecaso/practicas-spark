# -*- coding: utf-8 -*-

import pickle, re, subprocess
from datetime import datetime
from operator import itemgetter

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

LS_REGEX = re.compile(r'^([-|r|w]+)\s+\d+\s+[^\d]*(\d+)[^/]+(/.+)$', re.MULTILINE)
LS_FILEINFO = re.compile(r'^[^\d]+(\d+)_(\w*)\.json$')
DATE_FORMAT = '%Y%m'

is_iterable = lambda obj: hasattr(obj, '__iter__')

schema = StructType()\
    .add('_id', StructType().add('$oid', StringType(), False), False)\
    .add("user_day_code", StringType(), False)\
    .add("idplug_base", IntegerType(), False)\
    .add("idunplug_base", IntegerType(), False)\
    .add("travel_time", IntegerType(), False)\
    .add("idunplug_station", IntegerType(), False)\
    .add("idplug_station", IntegerType(), False)\
    .add("ageRange", IntegerType(), False)\
    .add("user_type", IntegerType(), False)\
    .add("unplug_hourTime", TimestampType(), False)\
    .add("zip_code", StringType(), True)


class DataLoader():
    def __init__(self, **kwargs):
        self.dir = kwargs.get('dir','/public_data/bicimad')
        self.cache = kwargs.get('cache_file', 'cache_files.data')
        self.appName = kwargs.get('appName', 'bicimad')
        self.spark = kwargs['spark'] if 'spark' in kwargs else None
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

    def get(self, **kwargs):
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

    def get_df(self, **kwargs):
        files = list(self.get(**kwargs, path=True))
        print(f'Reading {len(files)} json files into DataFrame')
        self._init_spark()
        df = self.spark.read.json(files, schema=schema, timestampFormat="yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        return df

    def _init_spark(self):
        if not self.spark:
            self.spark = SparkSession.builder\
                    .appName(self.appName)\
                    .getOrCreate()

    def _load_hdfs(self):
        print(f'Getting list of json files in hdfs folder {self.dir}')
        cmd = f'/opt/hadoop/current/bin/hdfs dfs -ls {self.dir}'.split(' ')
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
                print(f"'Loading cache file '{self.cache}'")
                self.files = pickle.load(cache)
        except FileNotFoundError:
            print("Cache file '{self.cache}' not found")

__all__ = ['DataLoader']
