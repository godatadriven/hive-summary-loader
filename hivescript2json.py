# import require packages 
import os 
import json 
import itertools 

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
PATH = '/Users/code/Desktop'
HIVE_SCRIPT_PATH = PATH + '/maris.hive'
JSON_DATA_PATH = PATH + '/db_connections.json'

print "Now starting with parsing the hive script to a table layout."

try: 
   os.stat(HIVE_SCRIPT_PATH)
except: 
   print color.RED + "WARNING! HIVE SCRIPT PATH DOES NOT EXIST!" + color.END
   print "Check if everything went alright during " + color.UNDERLINE + "hivescripts.py" + color.END 

try: 
    os.stat(JSON_DATA_PATH) 
except: 
    os.mkdir(JSON_DATA_PATH)

hive_blob = open(HIVE_SCRIPT_PATH,"r").read()

# this ugly script creates a dict. key = table_name, values = [colnames]
table_blobs = hive_blob.split("DROP TABLE IF EXISTS ")
table_info = {} 
for table in table_blobs[1:]:
   name = table[0:table.find(';')]
   print 'I have found a table: ' + color.UNDERLINE + name + color.END 
   table_info[ name ] = [] 
   colnames = table[table.find('(')+1:table.find(')')]
   for col in colnames.replace('\n','').split(','): 
      table_info[ name ].append( col[:col.find(" ")] )

# this is the end result for d3 
result = { 'nodes' : [ {'name' : table} for table in table_info ], 'links' : []}

# this mapper is for d3 technical reasons here 
table_mapper = {} 
counter = 0
for i in table_info: 
   table_mapper[ i ] = counter 
   counter += 1 

link_dict = {} 
for table in table_info: 
    for link in table_info[table]: 
        if link in link_dict.keys():
            link_dict[link].append( table ) 
        else: 
            link_dict[link] = [table] 

arc_dict = {}            
for link in link_dict:
    if not len(link_dict[link]) > 1:
        pass
    elif link == '':
        pass
    else: 
        table_combinations = list(itertools.combinations( link_dict[link] , 2 )) 
        for arc in table_combinations: 
            if arc not in arc_dict.keys(): 
                arc_dict[arc] = [link]
            else:
                arc_dict[arc].append(link)
                    
for arc in arc_dict:
    link = { 'source' : table_mapper[ arc[0] ],
             'target' : table_mapper[ arc[1] ], 
             'values' : arc_dict[arc] } 
    result['links'].append( link ) 

print "We seem to have " + str(len(table_mapper.keys())) + " tables."
print "These tables have about " + str(len(arc_dict)) + " connections in between them."

json_blob = open(JSON_DATA_PATH,"w")
json_blob.write( json.dumps( result ) ) 
json_blob.close()

print "JSON file has been saved to " + JSON_DATA_PATH