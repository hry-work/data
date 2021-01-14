
source('env.R', encoding = 'utf-8')

t <- '9:00'

while (hm(str_c(hour(Sys.time()),minute(Sys.time()), sep=':')) < hm(t)) {
	print(glue('wait to {t} am to start'))
	sleep(60*10)
}
print('start HIVE ETL')
