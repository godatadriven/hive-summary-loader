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
HIVE_SCRIPT_PATH = PATH + '/hive/create_all_tables.hive'
JSON_DATA_PATH = PATH + '/json'

try: 
   os.stat(HIVE_SCRIPT_PATH)
except: 
   print color.RED + "WARNING! HIVE SCRIPT PATH DOES NOT EXIST!" + color.END
   print "Check if everything went alright during " + color.UNDERLINE + "hivescripts.py" + color.END 

try: 
    os.stat(JSON_DATA_PATH) 
except: 
    os.mkdir(JSON_DATA_PATH)

json_blob = open(JSON_DATA_PATH + '/db_connections.json',"w")
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

# this bit creates an arc_dict. key = common_colname, values = set([tables])
colname_set = set([])
for colname in [ table_info[key] for key in table_info ]:
   colname_set.update( colname )
arc_info = {} 
for colname in colname_set: 
   arc_info[ colname ] = [ table for table in table_info if colname in table_info[table]]
   arc_info[ colname ].sort() 
   arc_info[ colname ] = tuple( arc_info[ colname ] )

arc_set = {} 
for colname in arc_info: 
   if arc_info[colname] in arc_set.keys():
      arc_set[ arc_info[colname] ].update(colname)
   else: 
      arc_set[ arc_info[colname] ] = set([colname])

for arc in arc_set: 
   if len(arc) == 1: 
      pass 
   else:
      for link in [{'source':table_mapper[e[0]],'target':table_mapper[e[1]], 'values' : list(arc_set[arc])} for e in itertools.combinations(arc, 2)]:
         result['links'].append(link)

print "We seem to have " + str(len(table_mapper.keys())) + " tables."
print "These tables have about " + str(len(arc_set)) + " connections in common."


