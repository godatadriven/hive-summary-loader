# import require packages 
import os 
import json 
import itertools 
from common import color, create_dir
from settings import PATH, HIVE_SCRIPT_PATH, HIVE_DB, HDFS_PATH, BIG_HIVE_SCRIPT, JSON_DATA_PATH

print "Now starting with parsing the hive script to a table layout."

try: 
   os.stat(os.path.dirname(BIG_HIVE_SCRIPT))
except: 
   print color.RED + "WARNING! HIVE SCRIPT PATH DOES NOT EXIST!" + color.END
   print "Check if everything went alright during " + color.UNDERLINE + "hivescripts.py" + color.END 

create_dir(os.path.dirname(JSON_DATA_PATH))

hive_blob = open(BIG_HIVE_SCRIPT,"r").read()

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