#!/bin/sh
source /etc/profile
time=$(date '+%Y%m%d')
mysql_ip=''
mysql_user=''
mysql_passwd=''
mysql_db=''
oracle_ip1=''
oracle_ip2=''
oracle_db=''
oracle_schema=''
oracle_username=''
oracle_passwd=''
hive_db=''

data=$(mysql -h${mysql_ip} --default-character-set=utf8  -P53306 -u${mysql_user} -p${mysql_passwd} -D${mysql_db} -Ne "select table_name,field_name,first_column from table_import_all" |while read a b c;do echo "$a:$b:$c";done)
for i in $data
do
table_name=`echo $i|cut -d : -f 1`
column_name=`echo $i|cut -d : -f 2`
first_column=`echo $i|cut -d : -f 3`
# 导入
sqoop import \
-Dmapreduce.job.queuename=root.training-1  \
--connect jdbc:oracle:thin:@'(DESCRIPTION =(LOAD_BALANCE=YES)(ADDRESS = (PROTOCOL = TCP)(HOST = '${oracle_ip1}')(PORT = 1521))(ADDRESS = (PROTOCOL = TCP)(HOST = $oracle_ip2)(PORT = 1521))(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = '${oracle_db}')))' -username ${oracle_username} -password ${oracle_passwd} \
--query 'select '${column_name}' from '${oracle_schema}'.'${table_name}' WHERE $CONDITIONS' \
--hive-table ${table_name}_1 \
--columns ${column_name} \
--target-dir viewfs://cluster14/user/hive/warehouse/${hive_db}.db/${table_name}/day=${time} \
--hive-partition-key "day" \
--hive-partition-value ${time} \
--hive-import -m 50 \
--split-by 'mod(ora_hash('${first_column}'),50)' \
--null-string '\\N' \
--null-non-string '\\N' \
--fields-terminated-by '\b' \
--hive-database ${hive_db}

echo "$(date '+%Y%m%d%H%M%S'),${table_name} is finish" >>sqoop.log
done
