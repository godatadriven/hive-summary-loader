# set the global start values. we assume that this script is run in a folder that has all the hive data
import os

PATH = '/Users/admin/Projects/ing/test'
HDFS_PATH = '/org_data/folder'
HIVE_SCRIPT_PATH = PATH + '/hive'
BIG_HIVE_SCRIPT = PATH + '/all_hive/create_all_tables.hive'
HIVE_QUERY_SCRIPTS_PATH = PATH + '/scripts'
JSON_DATA_PATH = PATH + '/db_connections.json'

HIVE_DB='default'

dry_run = True