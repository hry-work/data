copy every_1d@0h1m.flow bak1.flow
copy every_1d@0h1m.flow bak2.flow
copy every_1d@0h1m.flow bak3.flow
copy every_1d@0h1m.flow bak4.flow
copy every_1d@0h1m.flow bak5.flow
copy dk_30.flow bak_dk.flow
del  *.zip
"C:\Program Files\7-Zip\7z.exe" a aiyunxiao.zip *.*
"C:\Program Files\7-Zip\7z.exe" d aiyunxiao.zip zip.*
