import os
from settings import PATH, HIVE_SCRIPT_PATH, HIVE_DB, HDFS_PATH, HIVE_QUERY_SCRIPTS_PATH
from common import create_dir, color, get_files_in_dir_with_extension
        
#FILES = [file for file in os.listdir(HIVE_SCRIPT_PATH)]
#DATA_FILES = [ thing for thing in FILES if os.path.isfile(HIVE_SCRIPT_PATH+"/"+thing) and thing.endswith('.hive')]
DATA_FILES = get_files_in_dir_with_extension(HIVE_SCRIPT_PATH, '.hive')

if not DATA_FILES:
    print color.RED + "No hive files found in directory "+PATH+ color.END
    sys.exit(0)

#Hive variables
HIVE_SETUP="use "+HIVE_DB+"; set hive.cli.print.header=true; "

create_dir(HIVE_QUERY_SCRIPTS_PATH)

def generate_hive_queries(fields, tablename):
    #Create dir where we will write the results
    RESULTS_DIR=HIVE_QUERY_SCRIPTS_PATH+'/'+tablename
    create_dir(RESULTS_DIR)
    query_file = open( HIVE_QUERY_SCRIPTS_PATH + '/' + tablename + '.sh', "w")
    query_file.write("#!/bin/bash \n")
    file_name=RESULTS_DIR+'/'+tablename+'.txt'
    to_execute = "hive -e \""+HIVE_SETUP+" select count(*) as all_rows from "+tablename+";\" > "+file_name+'\n';
    to_execute+="echo \"----\" >>"+file_name+'\n';
    to_execute += "hive -e \""+HIVE_SETUP+" describe "+tablename+";\" > "+file_name+'\n';
    query_file.write(to_execute)
    for field in fields:
        column_name=field[0]
        file_name=RESULTS_DIR+'/'+column_name+'.txt'
        to_execute=''
        if field[1]=='STRING':
            to_execute+="hive -e \""+HIVE_SETUP+" select count(distinct "+column_name+") as distinct_col from "+tablename+";\" > "+file_name+'\n';
            to_execute+="echo \"----\" >>"+file_name+'\n';
            to_execute+="echo \"Least frequent:\">>"+file_name+'\n';
            to_execute+="hive -e \""+HIVE_SETUP+" select "+column_name+", count(*) as count from "+tablename+" GROUP BY "+column_name+" ORDER BY count asc LIMIT 10;\" | sed \'s/\\t/|/g\'  >> "+file_name+'\n';
            to_execute+="echo \"----\" >>"+file_name+'\n';
            to_execute+="echo \"Most frequent:\">>"+file_name+'\n';
            to_execute+="hive -e \""+HIVE_SETUP+" select "+column_name+", count(*) as count from "+tablename+" GROUP BY "+column_name+" ORDER BY count desc LIMIT 10;\" | sed \'s/\\t/|/g\'  >> "+file_name+'\n';
        else:
            to_execute+="hive -e \""+HIVE_SETUP+" select max("+column_name+") as max from "+tablename+";\" > "+file_name+'\n';
            to_execute+="echo \"----\" >>"+file_name+'\n';
            to_execute+="hive -e \""+HIVE_SETUP+" select min("+column_name+") as min from "+tablename+";\" >> "+file_name+'\n';
            to_execute+="echo \"----\" >>"+file_name+'\n';
            to_execute+="hive -e \""+HIVE_SETUP+" select percentile_approx("+column_name+", 0.5) as median from "+tablename+";\" >> "+file_name+'\n';
            to_execute+="echo \"----\" >>"+file_name+'\n'; 
            to_execute+="hive -e \""+HIVE_SETUP+" select avg("+column_name+") as mean from "+tablename+";\" >> "+file_name+'\n';
            to_execute+="echo \"----\" >>"+file_name+'\n';
            to_execute+="hive -e \""+HIVE_SETUP+" select stddev_pop("+column_name+") as stdev from "+tablename+";\" >> "+file_name+'\n';
        query_file.write(to_execute)
    print color.GREEN + "Generated hive queries for table "+ tablename + color.END
            
def create_summary_for_table(datafile):
    tablename = os.path.basename(datafile)[:os.path.basename(datafile).find('.hive')]
    file_to_process = open(datafile).read()
    columns_dump = file_to_process[file_to_process.find('(')+1: file_to_process.find(')')]
    columns_dump = columns_dump.replace(',\n',',')
    columns_dump = columns_dump.replace('\n','')
    columns = columns_dump.split(',')
    fields = [column.strip().split(' ') for column in columns]
    generate_hive_queries(fields, tablename)

    
for datafile in DATA_FILES:
    create_summary_for_table(datafile)
