# install.packages('utf8')
# install.packages('rJava')
# install.packages('glue')
# install.packages('tidyr')
# install.packages('dplyr')
# install.packages('lubridate')
# install.packages('plyr')
# install.packages('stringr')
# install.packages('readr')
# install.packages('readxl')
# install.packages('openxlsx')
# install.packages('zoo')
# install.packages('car')
# install.packages('RODBC')
# install.packages('RSQLite')
# install.packages('ggplot2')
# install.packages(c('vctrs' , 'httr' , 'rvest'))
# install.packages('xml2')
# install.packages("RJDBC")
# install.packages("DBI")
# install.packages("carData")


# 加载包
library('utf8')
library('rJava')
library('DBI')
library('RODBC')
library('RJDBC')
library('RSQLite')
library('glue')
library('tidyr')
library('plyr')
library('dplyr')
library('lubridate')
library('stringr')
library('readr')
library('readxl')
library('openxlsx')
library('zoo')
library('carData')
library('car')
library('ggplot2')
library('vctrs')
library('httr')
library('xml2')
library('rvest')



# 关联数据库
# net_orc <- odbcConnect("Oracle",uid="ls_xywy",pwd='ls_xywy') 

net_sql <- odbcConnect('orcl', uid='sa' , pwd='xywy2020.')


# 日期
today <- as_date(today())
day <- as_date(today - 1)
