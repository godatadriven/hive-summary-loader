#!/bin/bash
pathDir=`pwd`/scripts
scripts_to_run_dir=$pathDir
scripts_archive_dir=$pathDir/done
mkdir -p $scripts_archive_dir
for each in "$scripts_to_run_dir"/*.sh
do 
	bash $each
	mv $each $scripts_archive_dir/
done
