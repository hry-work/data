#! /bin/bash

echo cd $1
cd $1

# 定义命令日志输出文件
exec 1>>git.log
exec 2>>git.log

# 执行git操作
echo '*****************************************************************************'
echo '*****************************************************************************'
echo '*****************************************************************************'
echo '*****************************************************************************'
echo '*****************************************************************************'
echo "**************** "`date`" start****************"

echo git checkout prod
git checkout prod

echo git status
git status

echo git add -A
git add .

echo git commit -m '"backup schema everyday"'
git commit -m "backup schema everyday"

echo git push origin prod
git push origin prod
