
source('env.R', encoding = 'utf-8')

while (hm(str_c(hour(Sys.time()),minute(Sys.time()), sep=':')) < hm('0:30')) {
	print('wait to 0:30 am to start')
	sleep(60*10 + 1)
}
print('start log-serv ETL')
