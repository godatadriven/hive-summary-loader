# import require packages 
import pandas as pd
import os 

# define a color scheme for print output 
class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

# set the global start values. we assume that this script is run in a folder that has all the hive data         
PATH = os.getcwd() 
FILES = [file for file in os.listdir(os.getcwd())]
DATA_FILES = [ thing for thing in FILES if os.path.isfile(thing) and ( thing.endswith('.csv') or thing.endswith('.txt'))]
HDFS_PATH = 'foobar/folder'
HIVE_SCRIPT_PATH = PATH + '/hive'

try: 
    os.stat(HIVE_SCRIPT_PATH) 
except: 
    os.mkdir(HIVE_SCRIPT_PATH)
    
# define a bunch of nice helper functions 
def generate_folder(datafile): 
    directory = PATH + '/' + os.path.splitext(datafile)[0]
    try: 
        os.stat(directory) 
    except: 
        os.mkdir(directory)
    print "Created the following directory : " + directory
    return directory

def generate_small_csv(datafile):
    filename = "small_"+datafile
    os.system("head "+datafile+" > "+filename)
    print "Created the following file : "+filename
    return filename
    
def move_data_file(datafile):
    os.system('tail -n +2 ' + datafile + ' > ' + generate_folder(datafile) + '/' + datafile )
    os.system('rm ' + datafile ) 
    print "Just (re)moved " + datafile 

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

def parse_coltype( coltype ): 
    if 'int' in coltype: 
        return 'BIGINT' 
    if 'float' in coltype: 
        return 'DOUBLE'
    return 'STRING' 
    
def generate_column_types(datafile, delimiter):
    smallfile = generate_small_csv( datafile )
    firstline = open( smallfile,'r').readline()
    df = pd.read_csv( PATH + '/' + smallfile, delimiter = delimiter )
    middlebit = "" 
    for col in range(0,len(df.dtypes)): 
        middlebit += df.columns[col] +" "+ parse_coltype( str( df.dtypes[col] ) ) + ',\n'
    return middlebit[:-2] + ')\n' 

def generate_hive_script(datafile): 
    tablename = datafile[:datafile.find('.csv')]
    firstline = open(datafile,'r').readline().replace('\r','').replace('\n','')
    delimiter = find_delimiter(firstline)
    startbit = "DROP TABLE IF EXISTS " + tablename + ';\n'
    startbit += "CREATE EXTERNAL TABLE " + tablename + "(\n"
    middlebit = generate_column_types(datafile, delimiter)
    endbit = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '"+delimiter+"'\n"
    endbit += "LOCATION '"+HDFS_PATH+"/"+tablename+"';" 
    total = startbit + middlebit + endbit 
    text_file = open( HIVE_SCRIPT_PATH + '/' + tablename + '.hive', "w")
    text_file.write( total )
    text_file.close()
    print 'Hive script created at: ' + HIVE_SCRIPT_PATH + '/' + tablename + '.hive' 
        
def process_data_file(datafile): 
    print "Now starting with:" + datafile 
    generate_hive_script(datafile)
    move_data_file(datafile) 

def create_big_hive_query(): 
    print color.UNDERLINE + "Now starting with creating big hive query" + color.END
    big_script_path = HIVE_SCRIPT_PATH+'/create_all_tables.hive'
    if os.path.exists(big_script_path): os.remove(big_script_path)
    scripts = [ script for script in os.listdir(HIVE_SCRIPT_PATH) if not os.path.isdir(HIVE_SCRIPT_PATH+'/'+script) ]
    big_script = open(big_script_path,"w")    
    for script in scripts: 
        print "Now adding: " + script + color.END
        small_script = open(HIVE_SCRIPT_PATH + '/' + script,"r")
        big_script.write(small_script.read()+'\n\n')
        small_script.close()         
    big_script.close()
    print color.GREEN + "Congratulations. It's seems like all hive scripts have been made." + color.END
    print "You can find the results in "+ color.UNDERLINE + big_script_path + color.END

# these few lines actually cause the script to run 

# this creates all the files 
for datafile in DATA_FILES: 
    process_data_file(datafile) 
    
# this only causes the create_all_tables.hive table to get created 
# this can be run after you do some manual tweeking
create_big_hive_query()