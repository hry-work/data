#!/bin/bash

# 备份数据库表结构
# 参考flow/sys/sys.flow调用此脚本
host=$1
port=$2
user=$3
password=$4
db=$5

mysqldump -h$host -P$port -u$user -p$password -d $db > resource/schema/$db.sql
echo "backup db "$db" SUCESS!"
