from random import randint
from tasks import writeTofile
from datetime import datetime
import psycopg2
import tasks
import time
import replicator


connect = psycopg2.connect(
    dbname = 'students',
    user = 'pmi-b8510',
    password = '2UdChucu',
    host = 'fpm2.ami.nstu.ru',
    options = '-c search_path=pmib8510,public')

cursor = connect.cursor()

changes = []
types = []
database = []
timer = 0
timing = time.time()
while (timer < 3):
    operation = randint(1, 3)
    bd = randint(1, 3)
    types.append(operation)
    database.append(bd)
    changes = tasks.task(bd, operation, connect, cursor, changes)
    timer += 1
    if timer % 3 == 0:
        timing = time.time()
        print("1 second passed")

        cursor.execute("lock table table_v_bd1 in exclusive mode")
        cursor.execute("lock table table_v_bd2 in exclusive mode")
        cursor.execute("lock table table_v_bd3 in exclusive mode")

        for i in range(len(types)):
            if(types[i] == 1):
                replicator.insert(connect, cursor, database[i], changes[i])
            if(types[i] == 2):
                replicator.update(connect, cursor, database[i], changes[i], changes[i + 1])
                changes.pop(i + 1)
            if(types[i] == 3):
                replicator.delete(connect, cursor, database[i], changes[i])

        cursor.execute("SELECT * FROM table_v_bd1")
        record1 = cursor.fetchall()
        cursor.execute("SELECT * FROM table_v_bd2")
        record2 = cursor.fetchall()
        cursor.execute("SELECT * FROM table_v_bd3")
        record3 = cursor.fetchall()
        content = []

        current_datetime = datetime.now()

        content.append("Time of replecation:" + str(current_datetime) + '\n\n')
        content.append("table_v_bd1\n")
        for i in range(len(record1)):
            content.append(str(record1[i][0]) + '\t' + str(record1[i][1]) + '\t' + str(record1[i][2]) + '\t' + str(record1[i][3]) + '\t' + str(record1[i][4]) + '\t' + str(record1[i][5]))
        
        content.append("---------------------------------------------------------------------------------------------------------------------------------------------------\n")   
        content.append("table_v_bd2\n")
        for i in range(len(record2)):
            content.append(str(record2[i][0]) + '\t' + str(record2[i][1]) + '\t' + str(record2[i][2]) + '\t' + str(record2[i][3]) + '\t' + str(record2[i][4]) + '\t' + str(record2[i][5]))
        
        content.append("---------------------------------------------------------------------------------------------------------------------------------------------------\n")   
        content.append("table_v_bd3\n")
        for i in range(len(record3)):
            content.append(str(record3[i][0]) + '\t' + str(record3[i][1]) + '\t' + str(record3[i][2]) + '\t' + str(record3[i][3]) + '\t' + str(record3[i][4]) + '\t' + str(record3[i][5]))
        content.append("---------------------------------------------------------------------------------------------------------------------------------------------------\n")   

        writeTofile("Logs.txt", content)

print("Process stoped")


cursor.close()
connect.close()