#!/bin/bash

if [ -z "$1" ]
  then
    FECHA=`date --date=$dt "+%Y-%m-%d"`
  else
    FECHA=$1	
fi


sqoop job --create addClientes -- import \
--connect jdbc:mysql://localhost/practica \
--username cloudera --password cloudera \
--table clientes \
--target-dir /recomendador/`echo $FECHA` \
--null-non-string '\\N' \
--split-by id \
--driver com.mysql.jdbc.Driver \
--fields-terminated-by '\t' \
--incremental append \
--check-column id
