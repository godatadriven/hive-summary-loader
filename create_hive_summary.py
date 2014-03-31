import os
        
PATH = os.getcwd()
HIVE_SCRIPT_PATH = PATH + '/hive'
FILES = [file for file in os.listdir(HIVE_SCRIPT_PATH)]
DATA_FILES = [ thing for thing in FILES if os.path.isfile(HIVE_SCRIPT_PATH+"/"+thing) and thing.endswith('.hive')]
SCRIPT_DIR=HIVE_SCRIPT_PATH+'/'+'scripts'

#Hive variables
DATABASE='default'
HIVE_SETUP="use "+DATABASE+"; set hive.cli.print.header=true; "

try:
    os.stat(SCRIPT_DIR)
except:
    os.mkdir(SCRIPT_DIR)

def generate_hive_queries(fields, tablename):
    #Create dir where we will write the results
    RESULTS_DIR=SCRIPT_DIR+'/'+tablename
    try:
        os.stat(RESULTS_DIR)
    except:
        os.mkdir(RESULTS_DIR)
    query_file = open( SCRIPT_DIR + '/' + tablename + '.hive', "w")
    query_file.write("#!/bin/bash \n")
    for field in fields:
        column_name=field[0]
        file_name=RESULTS_DIR+'/'+column_name+'.txt'
        to_execute=''
        if field[1]=='STRING':
            to_execute+="hive -e \""+HIVE_SETUP+" select count(distinct "+column_name+") as distinct_col from "+tablename+";\" > "+file_name+'\n';
            echo "----" >> file_name 
            echo "Least frequent:" >> file_name
            to_execute+="hive -e \""+HIVE_SETUP+" select "+column_name+", count(*) as count from "+tablename+" GROUP BY "+column_name+" ORDER BY count asc LIMIT 10;\" | sed \'s/\\t/|/g\'  >> "+file_name+'\n';
            echo "----" >> file_name 
            echo "Most frequent:" >> file_name
            to_execute+="hive -e \""+HIVE_SETUP+" select "+column_name+", count(*) as count from "+tablename+" GROUP BY "+column_name+" ORDER BY count desc LIMIT 10;\" | sed \'s/\\t/|/g\'  >> "+file_name+'\n';
        else:
            to_execute+="hive -e \""+HIVE_SETUP+" select max("+column_name+") as max from "+tablename+";\" > "+file_name+'\n';
            echo "----" >> file_name 
            to_execute+="hive -e \""+HIVE_SETUP+" select min("+column_name+") as min from "+tablename+";\" >> "+file_name+'\n';
            echo "----" >> file_name 
            to_execute+="hive -e \""+HIVE_SETUP+" select percentile_approx("+column_name+", 0.5) as median from "+tablename+";\" >> "+file_name+'\n';
            echo "----" >> file_name  
            to_execute+="hive -e \""+HIVE_SETUP+" select avg("+column_name+") as mean from "+tablename+";\" >> "+file_name+'\n';
            echo "----" >> file_name 
            to_execute+="hive -e \""+HIVE_SETUP+" select stddev_pop("+column_name+") as stdev from "+tablename+";\" >> "+file_name+'\n';
        query_file.write(to_execute)
            
def create_summary_for_table(datafile):
    tablename = datafile[:datafile.find('.hive')]
    file_to_process = open(HIVE_SCRIPT_PATH+"/"+datafile).read()
    columns_dump = file_to_process[file_to_process.find('(')+1: file_to_process.find(')')]
    columns_dump = columns_dump.replace(',\n',',')
    columns_dump = columns_dump.replace('\n','')
    columns = columns_dump.split(',')
    fields = [column.strip().split(' ') for column in columns]
    generate_hive_queries(fields, tablename)

    
for datafile in DATA_FILES:
    create_summary_for_table(datafile)
