import re

LS_REGEX = re.compile(r'^([-|r|w]+)\s+\d+\s+[^\d]*(\d+)[^/]+(/.+)$', re.MULTILINE)
LS_FILEINFO = re.compile(r'^[^\d]+(\d+)_(\w*)\.json$')
DATE_FORMAT = '%Y%m'
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
HDFS_PATH = '/opt/hadoop/current/bin/hdfs'
