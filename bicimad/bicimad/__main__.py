import argparse

from pyspark.sql.session import SparkSession
from bicimad.globals import HDFS_PATH, DATE_FORMAT
from bicimad.data import DataLoader

def detect_hdfs():
    try:
        open(HDFS_PATH)
        return True
    except Exception:
        return False

def ask_time_interval(args):
    result = {}
    result['start'] = args.start if args.start\
            else input(f'Select START year and month (format {DATE_FORMAT}): ')
    result['end'] = args.end if args.end\
            else input(f'Select END year and month (format {DATE_FORMAT}): ')
    return result

def parse_args():
    parser = argparse.ArgumentParser(prog='bicimad', description='BiciMad data analysis')
    subparsers = parser.add_subparsers(dest='command')
    pipeline_parser = subparsers.add_parser('pipeline', help='run pipeline')  
    pipeline_parser.add_argument('name', type=str, help="pipeline name")
    pipeline_parser.add_argument('--start', type=str, metavar=f'"{DATE_FORMAT}"', help=f"start year and month")
    pipeline_parser.add_argument('--end', type=str, metavar=f'"{DATE_FORMAT}"', help=f"end year and month")
    pipeline_parser.add_argument("--load-sample", type=str, help="load sample file") 
    pipeline_parser.add_argument('--args', type=str, help=f"pipeline args")
    sampler_parser = subparsers.add_parser('sampler', help='run sampler')
    sampler_parser.add_argument('id', type=int, help="sampler id (int)")
    list_parser = subparsers.add_parser('list', help='list pipelines or samplers')
    list_parser.add_argument('options', type=str, choices=['pipelines', 'samples'])
    return parser.parse_args()


def main():
    args = parse_args() 
    hdfs = detect_hdfs()
    if args.command == 'sampler':
        assert hdfs, 'Cannot run without hdfs'
        from bicimad.sampler import Sampler
        sampler = Sampler()
        sampler.run(args.id) 
    elif args.command == 'pipeline':
        spark = SparkSession.builder\
                .appName(f'bicimad pipeline {args.name}')\
                .getOrCreate()
        from bicimad.pipelines import pipelines_data
        pipeline_data = pipelines_data[args.name] 
        if hdfs:
            data_loader = DataLoader(spark=spark)
            time_interval = ask_time_interval(args)
            data = data_loader.get_data(type=pipeline_data['type'], **time_interval) 
        else:
            sample_file = args.load_sample if args.load_sample\
                    else 'samples/'+pipeline_data['default_sample']
            data_loader = DataLoader(spark=spark, test_file = sample_file)
            data = data_loader.get_data(type=pipeline_data['type']) 
        data.load_df()
        print(f'Starting pipeline {args.name}')
        pipeline_args = args.args.split(',') if args.args else []
        assert len(pipeline_args) == len(pipeline_data['args'].split(',')), 'Missing pipeline args!'
        pipeline = pipeline_data['pipeline'](pipeline_args)
        print('- Fitting data')
        model = pipeline.fit(data.df)
        print('- Transforming data')
        results = model.transform(data.df)
        print(results)
        results.printSchema()
        results.show()
        pd_df = results.toPandas()
        if hdfs:
            csv_filename = f"station_{pipeline_args[0]}_{args.name}_{time_interval['start']}_{time_interval['end']}.csv"
        else:
            csv_filename = f"station_{pipeline_args[0]}_{args.name}.csv"
        pd_df.to_csv(csv_filename, index=False)
        # colors = { True: 'red', False: 'blue' }
        # pd_df.plot.scatter(x='PCA1', y='PCA2', c=pd_df['weekend'].map(colors))\
        #         .get_figure().savefig(f"stations_pca_{pipeline_args[0]}.png")
    elif args.command == 'list':
        if args.options == 'pipelines':
            print("Available pipelines")
            from bicimad.pipelines import pipelines_data
            for pipeline_name in pipelines_data.keys():
                print(pipeline_name)
        elif args.options == 'sampler':
            from bicimad.sampler import Sampler
            sampler = Sampler()

main()
