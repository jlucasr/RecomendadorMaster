import csv
import MySQLdb

mydb = MySQLdb.connect(host='localhost', user='root', passwd='cloudera', db='practica')
cursor = mydb.cursor()
cursor.execute('create table if not exists clientes (id INT, gender VARCHAR(255), age VARCHAR(255), country VARCHAR(255), registered VARCHAR(255))')


file = 'userid-profile.tsv'

with open(file) as tsv:
  for linea in csv.reader(tsv, dialect="excel-tab"):
    #print "id: %s, gender: %s, age: %s, country: %s, registered: %s" % (linea[0],linea[1],linea[2],linea[3],linea[4] )
    idAntes = linea[0]
    idNuevo = int(idAntes[5:])	
    #print "idNuevo %d" % idNuevo
   
    cursor.execute("insert into clientes(id, gender, age, country, registered) values(%s,%s,%s,%s,%s)", (idNuevo,linea[1],linea[2],linea[3],linea[4] ))
mydb.commit()
cursor.close()
print "Listo!"
