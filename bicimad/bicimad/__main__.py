import argparse
from bicimad.globals import HDFS_PATH, DATE_FORMAT
from bicimad.data import DataLoader
from bicimad.pipelines import pipelines_data as P

def detect_hdfs():
    try:
        open(HDFS_PATH)
        return True
    except Exception:
        return False

def ask_time_interval():
    result = {}
    print('SELECT TIME INTERVAL') 
    print('--------------------') 
    result['start'] = input(f'Start (format {DATE_FORMAT}): ')
    result['end']   = input(f'End (format {DATE_FORMAT}): ')
    return result

def parse_args():
    parser = argparse.ArgumentParser(prog='bicimad', description='BiciMad data analysis')
    subparsers = parser.add_subparsers(dest='command')
    pipeline_parser = subparsers.add_parser('pipeline', help='run pipeline')  
    pipeline_parser.add_argument('name', type=str, help="pipeline name")
    pipeline_parser.add_argument("--load-sample", type=str, help="load sample file") 
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
        pipeline_data = P[args.name] 
        if hdfs:
            data_loader = DataLoader(appName=f'bicimad pipeline {args.name}')
            data = data_loader.get_data(type=pipeline_data['type'], **ask_time_interval()) 
        else:
            sample_file = args.load_sample if args.load_sample\
                    else 'samples/'+pipeline_data['default_sample']
            data_loader = DataLoader(appName=f'bicimad pipeline test {args.name}', test_file = sample_file)
            data = data_loader.get_data(type=pipeline_data['type']) 
        data.no_schema = True
        data.load_df()
        print(f'Starting pipeline {args.name}')
        pipeline = pipeline_data['pipeline']
        print('- Fitting data')
        model = pipeline.fit(data.df)
        print('- Transforming data')
        results = model.transform(data.df)
        results.printSchema()
        results.show()
    elif args.command == 'list':
        if args.options == 'pipelines':
            print("Available pipelines")
            for pipeline_name in P.keys():
                print(pipeline_name)
        elif args.options == 'sampler':
            from bicimad.sampler import Sampler
            sampler = Sampler()

main()
