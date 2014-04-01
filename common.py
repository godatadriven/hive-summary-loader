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

def create_dir(directory):
	try: 
		os.stat(directory) 
	except: 
		os.mkdir(directory)


def get_files_in_dir_with_extension(directory, extension):
	FILES = [directory+'/'+file for file in os.listdir(directory)]
	DATA_FILES = [ thing for thing in FILES if os.path.isfile(thing) and thing.endswith(extension)]
	return DATA_FILES