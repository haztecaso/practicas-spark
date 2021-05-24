#!/usr/bin/env python3
from bicimad import JsonFiles
from pyspark.sql import DataFrame

PATH = 'file:///home/alattes/'

def sample_n(df:DataFrame, n:int):
    count = df.count()
    n = n if count > n else count
    fraction = n / df.count()
    return df.sample(False, fraction)

def sample(**kwargs):
    n_movements, n_stations = kwargs.get('n_movements', 100), kwargs.get('n_stations', 10)
    years, months = kwargs.get('years', None), kwargs.get('months', None)
    sample_name = lambda f,n: f"{f.split('/')[-1].split('.')[0]}_rand_{n}.sample"
    files = JsonFiles(appName="bicimad-resumen")

    data = files.get(years=years, months=months, path=True)
    print("These files will be created:")
    for f in data:
        n = n_movements if 'movements' in f else n_stations
        print('·', sample_name(f, n))
    input('Press enter to confirm (ctrl-c to cancel).')
    
    data = files.get_dfs(filenames=True, years=years, months=months)
    for f, df in data:
        n = n_movements if 'movements' in f else n_stations
        print(f'Saving sample: {sample_name(f,n)}')
        sample_n(df, n).coalesce(1).write\
                .save(path=sample_name(f,n), format='json', mode='overwrite')


if __name__ == '__main__':
    sample()
