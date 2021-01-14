
source('env.R', encoding = 'utf-8')

while (hm(str_c(hour(Sys.time()),minute(Sys.time()), sep=':')) < hm('1:00')) {
	print('wait to 1:30 am to start')
	sleep(60*10 + 1)
}
print('start afd ETL')
