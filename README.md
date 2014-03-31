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
##### ```create_hive_summary.py``` 

This script creates a bash script which can be run to get the table statistics using hive queries. 

Before we run this script we need to specify where the directory is which the Hive table definitions. These Hive table definitions need to be in a folder and each table has to be defined in a separate file. By default we look at the current working directory for a 'hive' folder.

```
MAIN FOLDER 
	HIVE
		csv_file1.hive
		csv_file2.hive
		csv_file3.hive
		csv_file4.hive
		csv_file5.hive
		...
		csv_fileN.hive
```

Note: The script in which all the table definitions are specified should not be in this folder.

Before running we also need to change the DATABASE parameter in create_hive_summary.py. This parameter should point to the Hive database where the tables were created. The default value is 'default'. If the tables are not in this Hive database, all the queries will fail.

This script creates a directory called 'scripts' (the default location is in the hive directory created in the current working directory). In this directory we generate a directory named as the table for each table. In this directory we will instruct Hive to write the table summaries. For each column in the table we will get a text file with the description of the given column.

At the moment we obtain the following statistics:

- in case the column is numeric (integer or double) : we get the min, max, mean, median and standard deviation

- in case the column is string: we get the number of distinct values, the 10 least frequently appearing values and the 10 most frequently appearing values.

To obtain these statistics we also generate bash script which contain the hive queries required to obtain the aforementioned values. We generate one script per table and this is saved also in the scripts directory.
So our directory structure looks like:

```
MAIN FOLDER 
	HIVE
		table1.hive
		table2.hive
		table3.hive
		...
		tableN.hive
		
		SCRIPTS
			TABLE1
			table1.sh
			TABLE2
			table2.sh	
			TABLE3
			table3.sh
			....
			TABLEN
			tableN.sh	
```

To use, just make sure that you are in the correct folder, the Hive table creation scripts are in the hive folder and you changed the Hive DB and then:

```
$ python create_hive_summary.py 
```
##### ```run_all_scripts.sh``` 

This script is used after we generated all the Hive queries to obtain the table statistics. It runs all the *.sh scripts in a folder ( by default in the hive folder in the current working directory).
After a given script is run it will be moved to the 'done' directory.

This is used so we can run the long Hive queries during the night. Make sure that you are in the correct directory and that the bash scripts which need to be run to obtain the table statistics are in the scripts directory:

```
$ nohup sh run_all_scripts.sh &
```

After running this script you will get:

```
MAIN FOLDER 
	HIVE
		table1.hive
		table2.hive
		table3.hive
		...
		tableN.hive
		
		SCRIPTS
			DONE
				table1.sh
				table2.sh
				table3.sh
				...
				tableN.sh
			TABLE1
				col1.txt
				col2.txt
				...
				colN.txt
			TABLE2
				col1.txt
				col2.txt
				...
				colN.txt
			TABLE3
				col1.txt
				col2.txt
				...
				colN.txt
			....
			TABLEN
				col1.txt
				col2.txt
				...
				colN.txt

```


