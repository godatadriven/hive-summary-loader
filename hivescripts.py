# import require packages 
import os
import pandas as pd
from settings import PATH, HIVE_SCRIPT_PATH, HIVE_DB, HDFS_PATH, BIG_HIVE_SCRIPT, dry_run
from common import create_dir, color, get_files_in_dir_with_extension
import subprocess
import sys

#Get files with extension in the defined directory
DATA_FILES = get_files_in_dir_with_extension(PATH, '.csv')
DATA_FILES.extend(get_files_in_dir_with_extension(PATH, '.txt'))

#If we did not find CSV or TXT files in the folder, just exit.
if not DATA_FILES:
    print color.RED + "No csv or txt found in directory "+PATH+ color.END
    sys.exit(0)

#Create the Hive script directory if not exists
create_dir(HIVE_SCRIPT_PATH)

# define a bunch of nice helper functions

'''
For each file create a directory, because in Hive we need to put the data into a folder named the table
'''

def generate_folder(datafile): 
    directory = os.path.splitext(datafile)[0]
    create_dir(directory)
    print "Created the following directory : " + directory
    return directory

'''
Get only the first few lines from each file. This is used to generate the 
column definitions for the table and to try to identify the column types.
'''

def generate_small_csv(datafile):
    filename = os.path.dirname(datafile)+"/small_"+os.path.basename(datafile)
    os.system("head "+datafile+" > "+filename)
    print "Created the following file : "+filename
    return filename
    
'''
Eliminate first line from the file containing the table, because this is the line containing 
the table definition and move the data to the created folder, named the file name
Also move the folder for the table to HDFS.
'''

def move_data_file(datafile):
    folder=generate_folder(datafile)
    os.system('tail -n +2 ' + datafile + ' > ' + folder +'/'+ os.path.basename(datafile) )
    if dry_run:
        print color.YELLOW + 'We would run hadoop fs -put '+folder+' '+HDFS_PATH + color.END
    else:
        subprocess.check_call('hadoop fs -put '+folder+' '+HDFS_PATH, shell=True, stderr=subprocess.STDOUT)
        os.system('rm -r ' + folder )
        print "Just copied data to HDFS"
    os.system('rm ' + datafile ) 
    print "Just removed " + datafile

'''
In the first line of the file find the most commonly used character, i.e. , or ; or | or tab.
We assume that only these characters can be delimiters in our file.
Because the first line only contains the column names we do not handle cases when we have a 
string field containing a lot of commas.
Assumtion is that the first line only contains column names and does not contain delimiters in the 
name of the columns.
'''

def find_delimiter( line_of_text ): 
    possible_delimiters = [',',';',' ','|','\t']
    max_count = 0; 
    best_delimiter = ','
    for delimiter in possible_delimiters: 
        if line_of_text.count(delimiter) > max_count:
            best_delimiter = delimiter
            max_count = line_of_text.count(delimiter)
    print "We assume that this is the delimiter: " + best_delimiter
    return best_delimiter

'''
Try to identify what is the type of the column. At the moment we handle: int, double and string.
Note: We receive a type in Python and need to return back a type in Hive.
'''

def parse_coltype( coltype ): 
    if 'int' in coltype: 
        return 'BIGINT' 
    if 'float' in coltype: 
        return 'DOUBLE'
    return 'STRING' 
    
'''

'''

def generate_column_types(datafile, delimiter):
    smallfile = generate_small_csv( datafile )
    firstline = open( smallfile,'r').readline()
    df = pd.read_csv(smallfile, delimiter = delimiter )
    middlebit = "" 
    for col in range(0,len(df.dtypes)): 
        middlebit += df.columns[col] +" "+ parse_coltype( str( df.dtypes[col] ) ) + ',\n'
    return middlebit[:-2] + ')\n' 

def generate_hive_script(datafile): 
    print color.BLUE + datafile + color.END
    tablename = os.path.basename(datafile)[:os.path.basename(datafile).find('.csv')]
    print color.BLUE + tablename + color.END
    firstline = open(datafile,'r').readline().replace('\r','').replace('\n','')
    delimiter = find_delimiter(firstline)
    startbit = "USE "+ HIVE_DB + ';\n'
    startbit += "DROP TABLE IF EXISTS " + tablename + ';\n'
    startbit += "CREATE EXTERNAL TABLE " + tablename + "(\n"
    middlebit = generate_column_types(datafile, delimiter)
    endbit = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '"+delimiter+"'\n"  
    endbit += "LOCATION '"+HDFS_PATH+"/"+tablename+"';" 
    total = startbit + middlebit + endbit 
    text_file = open(HIVE_SCRIPT_PATH+'/'+tablename + '.hive', "w")
    text_file.write( total )
    text_file.close()
    print 'Hive script created at: ' + HIVE_SCRIPT_PATH+'/'+tablename + '.hive' 
    return tablename + '.hive'   
 
def create_hive_table(datafile):
    script=generate_hive_script(datafile)
    if dry_run:
        print color.YELLOW + 'We would run: hive -f '+script + color.END
    else:
        subprocess.check_call('hive -f '+script, shell=True, stderr=subprocess.STDOUT)
    print color.GREEN + "Hive table created with script " + script + color.END
    
def process_data_file(datafile): 
    print "Now starting with:" + datafile 
    create_hive_table(datafile)
    move_data_file(datafile) 

def create_big_hive_query(): 
    print color.UNDERLINE + "Now starting with creating big hive query" + color.END
    create_dir(os.path.dirname(BIG_HIVE_SCRIPT))
    if os.path.exists(BIG_HIVE_SCRIPT): os.remove(BIG_HIVE_SCRIPT)
    scripts = [ script for script in os.listdir(HIVE_SCRIPT_PATH) if not os.path.isdir(HIVE_SCRIPT_PATH+'/'+script) ]
    big_script = open(BIG_HIVE_SCRIPT,"w")    
    for script in scripts: 
        print "Now adding: " + script + color.END
        small_script = open(HIVE_SCRIPT_PATH + '/' + script,"r")
        big_script.write(small_script.read()+'\n\n')
        small_script.close()         
    big_script.close()
    print color.GREEN + "Congratulations. It's seems like all hive scripts have been made." + color.END
    print "You can find the results in "+ color.UNDERLINE + BIG_HIVE_SCRIPT + color.END

# these few lines actually cause the script to run 

# this creates all the files 
for datafile in DATA_FILES: 
    process_data_file(datafile) 
    
# this only causes the create_all_tables.hive table to get created 
# this can be run after you do some manual tweeking
create_big_hive_query()
