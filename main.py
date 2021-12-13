from random import randint
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
    operation = randint(2, 2)
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

        connect.commit()


print("Process stoped")


cursor.close()
connect.close()