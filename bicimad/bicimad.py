#!/usr/bin/env python3
import subprocess, pickle, re
from datetime import datetime
from operator import itemgetter
from pyspark.sql import SparkSession, DataFrame
from functools import reduce

LS_REGEX = re.compile(r'^([-|r|w]+)\s+\d+\s+[^\d]*(\d+)[^/]+(/.+)$', re.MULTILINE)
LS_FILEINFO = re.compile(r'^[^\d]+(\d+)_(\w*)\.json$')
DATE_FORMAT = '%Y%m'

is_iterable = lambda obj: hasattr(obj, '__iter__')


class JsonFiles():
    def __init__(self, **kwargs):
        self.dir = kwargs.get('dir','/public_data/bicimad')
        self.cache = kwargs.get('cache_file', 'cache_files.data')
        self.appName = kwargs.get('appName', 'bicimad')
        if 'spark' in kwargs:
            self.spark = kwargs['spark']
        else:
            self.spark = None
        self.files = []
        self.load_cache()
        if len(self.files) == 0:
            self.load_hdfs()
            self.write_cache()

    def get(self, **kwargs):
        result = self.files
        f_check = lambda f_key, arg_key: (lambda f: f[f_key] in kwargs[arg_key])
        if kwargs.get('years', None):
            assert is_iterable(kwargs['years'])
            result = filter(f_check('year','years'), result)
        if kwargs.get('months', None): 
            assert is_iterable(kwargs['months'])
            result = filter(f_check('month', 'months'), result)
        if 'type' in kwargs:
            result = filter(lambda f: f['type'] == kwargs['type'], result)
        if kwargs.get('path', False):
            result = map(itemgetter('path'), result) 
        return result

    def get_dfs(self, **kwargs):
        selection = self.get(**kwargs, path=True)
        self._init_spark()
        for f in selection:
            print(f'Reading {f} into DataFrame')
            df = self.spark.read.json(f)
            if kwargs.get('filenames', False):
                yield (f,df)
            else:
                yield df

    def _init_spark(self):
        if not self.spark:
            self.spark = SparkSession.builder\
                    .appName(self.appName)\
                    .getOrCreate()

    def load_hdfs(self):
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

    def write_cache(self):
        assert len(self.files) > 0, 'Cannot cache empty list of files' 
        with open(self.cache, 'wb') as cache:
            pickle.dump(self.files, cache)

    def load_cache(self):
        try:
            with open(self.cache, 'rb') as cache:
                print(f'Loading cache file {self.cache}')
                self.files = pickle.load(cache)
        except FileNotFoundError:
            pass


def main():
    files = JsonFiles().get(type="movements", path=False)
    days = 0
    from calendar import monthrange
    for f in files:
        year, month = f['date'].year, f['date'].month
        days += monthrange(year, month)[1]
    print(days, days*24)


if __name__ == '__main__':
    main()
