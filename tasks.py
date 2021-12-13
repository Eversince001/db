import data
from random import randint
from datetime import datetime

def task(bd, operation, connect, cursor, changes):
    
    content = []

    if (bd == 1):
        if(operation == 1):
            
            values = data.getdata(randint(0, 29), randint(0, 29), randint(0, 29), randint(0, 29))
            current_datetime = datetime.now()
            values.append(current_datetime)
            changes.append(values)
            cursor.execute("INSERT INTO table_v_bd1 (title, author, year, pages, operation, add_date) VALUES(%s, %s, %s, %s, %s, %s)", (values[0], values[1], values[2], values[3], values[4], current_datetime))
            content.append("Insert in table_v_bd1\ntime:" + str(current_datetime) + '\n' + str(values[0]) + '\t' + str(values[1]) + '\t' + str(values[2]) + '\t' + str(values[3]) + '\t' + str(values[4]) + '\t' + str(current_datetime))
            content.append("---------------------------------------------------------------------------------------------------------------------------------------------------")
            writeTofile("Logs.txt", content)

            connect.commit()

        if(operation == 2):

            values = data.getdata(randint(0, 29), randint(0, 29), randint(0, 29), randint(0, 29))
            current_datetime = datetime.now()
            values.append(current_datetime)
            command = "UPDATE table_v_bd1 SET operation = 'Update table_v_bd1', title = %s, add_date = %s WHERE oid = (SELECT min(oid) FROM table_v_bd1)"
            cursor.execute('SELECT * FROM table_v_bd1 WHERE oid = (SELECT min(oid) FROM table_v_bd1)')
            records = cursor.fetchone()
            cursor.execute(command, (values[0], current_datetime))
            changes.append(records)
            changes.append(values)
            content.append("Update in table_v_bd1\ntime:" + str(current_datetime) + '\n')
            content.append(str(records[0]) + '\t' + str(records[1]) + '\t' + str(records[2]) + '\t' + str(records[3]) + '\t' + str(records[4]) + '\t' + str(records[5]))
            content.append(str(values[0]) + '\t\t\t\t' + str(records[1]) + '\t\t\t\t' + str(records[2]) + ' ' + str(records[3]) + '  ' + 'Update in table_v_bd1' + '\t\t\t\t' + str(current_datetime))
            content.append("---------------------------------------------------------------------------------------------------------------------------------------------------")
            writeTofile("Logs.txt", content)

            connect.commit()

        if(operation == 3):
            cursor.execute('SELECT * FROM table_v_bd1 WHERE oid = (SELECT max(oid) FROM table_v_bd1)')
            records = cursor.fetchone()
            current_datetime = datetime.now()
            changes.append(records)
            content.append("Delete from table_v_bd1\ntime:" + str(current_datetime) + '\n')
            content.append(str(records[0]) + '\t' + str(records[1]) + '\t' + str(records[2]) + '\t' + str(records[3]) + '\t' + str(records[4]) + '\t' + str(records[5]))
            content.append("---------------------------------------------------------------------------------------------------------------------------------------------------")
            cursor.execute("DELETE FROM table_v_bd1 WHERE oid = (SELECT max(oid) FROM table_v_bd1)")
            writeTofile("Logs.txt", content)
            
            connect.commit()

    if (bd == 2):
        if(operation == 1):
            
            values = data.getdata(randint(0, 29), randint(0, 29), randint(0, 29), randint(0, 29))
            current_datetime = datetime.now()
            values.append(current_datetime)
            changes.append(values)
            cursor.execute("INSERT INTO table_v_bd2 (title, author, year, pages, operation, add_date) VALUES(%s, %s, %s, %s, %s, %s)", (values[0], values[1], values[2], values[3], values[4], current_datetime))
            content.append("Insert in table_v_bd2\ntime:" + str(current_datetime) + '\n' + str(values[0]) + '\t' + str(values[1]) + '\t' + str(values[2]) + '\t' + str(values[3]) + '\t' + str(values[4]) + '\t' + str(current_datetime))
            content.append("---------------------------------------------------------------------------------------------------------------------------------------------------")
            writeTofile("Logs.txt", content)

            connect.commit()

        if(operation == 2):

            values = data.getdata(randint(0, 29), randint(0, 29), randint(0, 29), randint(0, 29))
            current_datetime = datetime.now()
            values.append(current_datetime)
            command = "UPDATE table_v_bd2 SET operation = 'Update table_v_bd2', title = %s, add_date = %s WHERE oid = (SELECT min(oid) FROM table_v_bd2)"
            cursor.execute('SELECT * FROM table_v_bd2 WHERE oid = (SELECT min(oid) FROM table_v_bd2)')
            records = cursor.fetchone()
            cursor.execute(command, (values[0], current_datetime))
            changes.append(records)
            changes.append(values)
            content.append("Update in table_v_bd2\ntime:" + str(current_datetime) + '\n')
            content.append(str(records[0]) + '\t' + str(records[1]) + '\t' + str(records[2]) + '\t' + str(records[3]) + '\t' + str(records[4]) + '\t' + str(records[5]))
            content.append(str(values[0]) + '\t\t\t\t' + str(records[1]) + '\t\t\t\t' + str(records[2]) + ' ' + str(records[3]) + '  ' + 'Update in table_v_bd2' + '\t\t\t\t' + str(current_datetime))
            content.append("---------------------------------------------------------------------------------------------------------------------------------------------------")
            writeTofile("Logs.txt", content)

            connect.commit()

        if(operation == 3):
            cursor.execute('SELECT * FROM table_v_bd2 WHERE oid = (SELECT max(oid) FROM table_v_bd2)')
            records = cursor.fetchone()
            current_datetime = datetime.now()
            changes.append(records)
            content.append("Delete from table_v_bd2\ntime:" + str(current_datetime) + '\n')
            content.append(str(records[0]) + '\t' + str(records[1]) + '\t' + str(records[2]) + '\t' + str(records[3]) + '\t' + str(records[4]) + '\t' + str(records[5]))
            content.append("---------------------------------------------------------------------------------------------------------------------------------------------------")
            cursor.execute("DELETE FROM table_v_bd2 WHERE oid = (SELECT max(oid) FROM table_v_bd2)")
            writeTofile("Logs.txt", content)
            
            connect.commit()

    if (bd == 3):
        if(operation == 1):
            
            values = data.getdata(randint(0, 29), randint(0, 29), randint(0, 29), randint(0, 29))
            current_datetime = datetime.now()
            values.append(current_datetime)
            changes.append(values)
            cursor.execute("INSERT INTO table_v_bd3 (title, author, year, pages, operation, add_date) VALUES(%s, %s, %s, %s, %s, %s)", (values[0], values[1], values[2], values[3], values[4], current_datetime))
            content.append("Insert in table_v_bd3\ntime:" + str(current_datetime) + '\n' + str(values[0]) + '\t' + str(values[1]) + '\t' + str(values[2]) + '\t' + str(values[3]) + '\t' + str(values[4]) + '\t' + str(current_datetime))
            content.append("---------------------------------------------------------------------------------------------------------------------------------------------------")
            writeTofile("Logs.txt", content)

            connect.commit()

        if(operation == 2):

            values = data.getdata(randint(0, 29), randint(0, 29), randint(0, 29), randint(0, 29))
            current_datetime = datetime.now()
            values.append(current_datetime)
            command = "UPDATE table_v_bd3 SET operation = 'Update table_v_bd3', title = %s, add_date = %s WHERE oid = (SELECT min(oid) FROM table_v_bd3)"
            cursor.execute('SELECT * FROM table_v_bd3 WHERE oid = (SELECT min(oid) FROM table_v_bd3)')
            records = cursor.fetchone()
            cursor.execute(command, (values[0], current_datetime))
            changes.append(records)
            changes.append(values)
            content.append("Update in table_v_bd3\ntime:" + str(current_datetime) + '\n')
            content.append(str(records[0]) + '\t' + str(records[1]) + '\t' + str(records[2]) + '\t' + str(records[3]) + '\t' + str(records[4]) + '\t' + str(records[5]))
            content.append(str(values[0]) + '\t\t\t\t' + str(records[1]) + '\t\t\t\t' + str(records[2]) + ' ' + str(records[3]) + '  ' + 'Update in table_v_bd3' + '\t\t\t\t' + str(current_datetime))
            content.append("---------------------------------------------------------------------------------------------------------------------------------------------------")
            writeTofile("Logs.txt", content)

            connect.commit()

        if(operation == 3):
            cursor.execute('SELECT * FROM table_v_bd3 WHERE oid = (SELECT max(oid) FROM table_v_bd3)')
            records = cursor.fetchone()
            current_datetime = datetime.now()
            changes.append(records)
            content.append("Delete from table_v_bd3\ntime:" + str(current_datetime) + '\n')
            content.append(str(records[0]) + '\t' + str(records[1]) + '\t' + str(records[2]) + '\t' + str(records[3]) + '\t' + str(records[4]) + '\t' + str(records[5]))
            content.append("---------------------------------------------------------------------------------------------------------------------------------------------------")
            cursor.execute("DELETE FROM table_v_bd3 WHERE oid = (SELECT max(oid) FROM table_v_bd3)")
            writeTofile("Logs.txt", content)
            
            connect.commit()
    return changes

            
def writeTofile(file, content):
    f = open(file, "a")
    f.seek(0)

    for elem in content:
        f.write(str((elem)) + "\n")

    f.close()