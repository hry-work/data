#!/bin/bash

# 清理azkaban产生的临时文件

if [ $# > 0 ]; then
	echo $1
	cd $1
  rm -f ./*_output_*
  rm -f ./*_props_*
#  find . -name "mid_eve_afd_*_tmp" | xargs rm -f '*' 
else
	echo 'no param!'
  rm -f ../../*_output_*_tmp
  rm -f ../../*_props_*_tmp
fi
echo "tmp files dumped!"
