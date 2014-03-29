hive-summary-loader
===================

### Package Goal 

We have a huge number of csv files and they need to be imported into hive. We assume that we do not know what these files are but that our client wants to do something with it. 

```
MAIN FOLDER 
	csv_file1.txt
	csv_file2.txt
	csv_file3.txt
	csv_file4.txt
	csv_file5.txt
	...
	csv_filen.txt 
```

This package contains scripts that do the following; 
- automatically generate the hive scripts to create external hive tables 
- automatically generate basic statistics about this data (min, max, ect)
- automatically suggest a database layout that the client might have used 

### Package dependencies 

```
pip install pandas 
```

### Package Contents 

##### ```hivescripts.py``` 

This file needs to be run from the MAIN FOLDER where all the csv_files are located. We suggest you also back this data up. This script will change this folder; 

```
MAIN FOLDER 
	csv_file1.txt
	csv_file2.txt
	csv_file3.txt
	csv_file4.txt
	csv_file5.txt
	...
	csv_filen.txt 
```

into this folder; 

```
MAIN FOLDER 
	HIVE 
		create_all_tables.hive
		csv_file1.hive
		csv_file2.hive
		csv_file3.hive
		csv_file4.hive
		csv_file5.hive
		...
		csv_fileN.hive
	CSV_FILE1 FOLDER
	    csv_file1.txt* // no firstline, hive needs this 
	CSV_FILE2 FOLDER
	    csv_file2.txt* // no firstline, hive needs this 
	...
	CSV_FILEn FOLDER
		csv_filen.txt* // no firstline, hive needs this 
	small_csv_file1.txt
	small_csv_file2.txt
	small_csv_file3.txt
	small_csv_file4.txt
	small_csv_file5.txt
	small_...
	small_csv_filen.txt 
```

The usage is rather simple. Just make sure that you are in the correct folder, and then; 

```
$ python hivescripts.py 
```
