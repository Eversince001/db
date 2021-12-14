from datetime import datetime

def insert(connect, cursor, base, changes):
    
    if(base == 1):
        command = "SELECT table_v_bd2.title FROM table_v_bd2 WHERE table_v_bd2.title = %s and table_v_bd2.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            record2 = cursor.fetchone()
            if (changes[5] < record2[5]):
                    current_datetime = datetime.now()
                    command = "UPDATE table_v_bd2 SET operation = 'insert table_v_bd1', add_date = %s WHERE table_v_bd2.title = %s and table_v_bd2.author = %s"
                    cursor.execute(command, (current_datetime, record2[0], record2[1]))
        else:
            current_datetime = datetime.now()
            cursor.execute("INSERT INTO table_v_bd2 (title, author, year, pages, operation, add_date) VALUES(%s, %s, %s, %s, %s, %s)", (changes[0], changes[1], changes[2], changes[3], 'insert table_v_bd1', current_datetime))



        command = "SELECT table_v_bd3.title FROM table_v_bd3 WHERE table_v_bd3.title = %s and table_v_bd3.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            record3 = cursor.fetchone()
            if (changes[5] < record3[5]):
                current_datetime = datetime.now()
                command = "UPDATE table_v_bd3 SET operation = 'insert table_v_bd2', add_date = %s WHERE table_v_bd3.title = %s and table_v_bd3.author = %s"
                cursor.execute(command, (current_datetime, record3[0], record3[1]))

        else:
            current_datetime = datetime.now()
            cursor.execute("INSERT INTO table_v_bd3 (title, author, year, pages, operation, add_date) VALUES(%s, %s, %s, %s, %s, %s)", (changes[0], changes[1], changes[2], changes[3], 'insert table_v_bd2', current_datetime))


    if(base == 2):
        command = "SELECT table_v_bd3.title FROM table_v_bd3 WHERE table_v_bd3.title = %s and table_v_bd3.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            record3 = cursor.fetchone()
            if (changes[5] < record3[5]):
                current_datetime = datetime.now()
                command = "UPDATE table_v_bd3 SET operation = 'insert table_v_bd2', add_date = %s WHERE table_v_bd3.title = %s and table_v_bd3.author = %s"
                cursor.execute(command, (current_datetime, record3[0], record3[1]))

        else:
            current_datetime = datetime.now()
            cursor.execute("INSERT INTO table_v_bd3 (title, author, year, pages, operation, add_date) VALUES(%s, %s, %s, %s, %s, %s)", (changes[0], changes[1], changes[2], changes[3], 'insert table_v_bd2', current_datetime))


           
        command = "SELECT table_v_bd1.title FROM table_v_bd1 WHERE table_v_bd1.title = %s and table_v_bd1.author = %s"
        cursor.execute(command, (changes[0],changes[1]))
        if(len(cursor.fetchall()) > 0):
            record1 = cursor.fetchone()
            if (changes[5] < record1[5]):
                current_datetime = datetime.now()
                command = "UPDATE table_v_bd1 SET operation = 'insert table_v_bd2', add_date = %s WHERE table_v_bd1.title = %s and table_v_bd1.author = %s"
                cursor.execute(command, (current_datetime, record1[0], record1[1]))

        else:
            current_datetime = datetime.now()
            cursor.execute("INSERT INTO table_v_bd1 (title, author, year, pages, operation, add_date) VALUES(%s, %s, %s, %s, %s, %s)", (changes[0], changes[1], changes[2], changes[3], 'insert table_v_bd2', current_datetime))


    if(base == 3):  
        command = "SELECT table_v_bd2.title FROM table_v_bd2 WHERE table_v_bd2.title = %s and table_v_bd2.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            record2 = cursor.fetchone()
            if (changes[5] < record2[5]):
                current_datetime = datetime.now()
                command = "UPDATE table_v_bd2 SET operation = 'insert table_v_bd3', add_date = %s WHERE table_v_bd2.title = %s and table_v_bd2.author = %s"
                cursor.execute(command, (current_datetime, record2[0], record2[0]))
        else:
            current_datetime = datetime.now()
            cursor.execute("INSERT INTO table_v_bd2 (title, author, year, pages, operation, add_date) VALUES(%s, %s, %s, %s, %s, %s)", (changes[0], changes[1], changes[2], changes[3], 'insert table_v_bd3', current_datetime))
        
        command = "SELECT table_v_bd1.title FROM table_v_bd1 WHERE table_v_bd1.title = %s and table_v_bd1.author = %s"
        cursor.execute(command, (changes[0],changes[1]))
        if(len(cursor.fetchall()) > 0):
            record1 = cursor.fetchone()
            if (changes[5] < record1[5]):
                current_datetime = datetime.now()
                command = "UPDATE table_v_bd1 SET operation = 'insert table_v_bd2', add_date = %s WHERE table_v_bd1.title = %s and table_v_bd1.author = %s"
                cursor.execute(command, (current_datetime, record1[0], record1[1]))

        else:
            current_datetime = datetime.now()
            cursor.execute("INSERT INTO table_v_bd1 (title, author, year, pages, operation, add_date) VALUES(%s, %s, %s, %s, %s, %s)", (changes[0], changes[1], changes[2], changes[3], 'insert table_v_bd2', current_datetime))

    connect.commit()

def delete(connect, cursor, base, changes):

    if(base == 1):
        command = "SELECT table_v_bd2.title FROM table_v_bd2 WHERE table_v_bd2.title = %s and table_v_bd2.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            command = "DELETE FROM table_v_bd2 WHERE table_v_bd2.title = %s and table_v_bd2.author = %s"
            cursor.execute(command, (changes[0], changes[1]))

        command = "SELECT table_v_bd3.title FROM table_v_bd3 WHERE table_v_bd3.title = %s and table_v_bd3.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            command = "DELETE FROM table_v_bd3 WHERE table_v_bd3.title = %s and table_v_bd3.author = %s"
            cursor.execute(command, (changes[0], changes[1]))

    if(base == 3):
        command = "SELECT table_v_bd2.title FROM table_v_bd2 WHERE table_v_bd2.title = %s and table_v_bd2.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            command = "DELETE FROM table_v_bd2 WHERE table_v_bd2.title = %s and table_v_bd2.author = %s"
            cursor.execute(command, (changes[0], changes[1]))

        command = "SELECT table_v_bd1.title FROM table_v_bd1 WHERE table_v_bd1.title = %s and table_v_bd1.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            command = "DELETE FROM table_v_bd1 WHERE table_v_bd1.title = %s and table_v_bd1.author = %s"
            cursor.execute(command, (changes[0], changes[1]))

    if(base == 2):
        command = "SELECT table_v_bd3.title FROM table_v_bd3 WHERE table_v_bd3.title = %s and table_v_bd3.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            command = "DELETE FROM table_v_bd3 WHERE table_v_bd3.title = %s and table_v_bd3.author = %s"
            cursor.execute(command, (changes[0], changes[1]))

        command = "SELECT table_v_bd1.title FROM table_v_bd1 WHERE table_v_bd1.title = %s and table_v_bd1.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            command = "DELETE FROM table_v_bd1 WHERE table_v_bd1.title = %s and table_v_bd1.author = %s"
            cursor.execute(command, (changes[0], changes[1]))


    connect.commit()

def update(connect, cursor, base, changes, new):

    if(base == 1):
        command = "SELECT table_v_bd2.title FROM table_v_bd2 WHERE table_v_bd2.title = %s and table_v_bd2.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            command = "UPDATE table_v_bd2 SET operation = 'Update table_v_bd1', title = %s, add_date = %s WHERE table_v_bd2.title = %s and table_v_bd2.author = %s"
            current_datetime = datetime.now()
            cursor.execute(command, (new[0], current_datetime, changes[0], changes[1]))
        
        command = "SELECT table_v_bd3.title FROM table_v_bd3 WHERE table_v_bd3.title = %s and table_v_bd3.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            command = "UPDATE table_v_bd3 SET operation = 'Update table_v_bd2', title = %s, add_date = %s WHERE table_v_bd3.title = %s and table_v_bd3.author = %s"
            current_datetime = datetime.now()
            cursor.execute(command, (new[0], current_datetime, changes[0], changes[1]))

    if(base == 3):
        command = "SELECT table_v_bd2.title FROM table_v_bd2 WHERE table_v_bd2.title = %s and table_v_bd2.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            command = "UPDATE table_v_bd2 SET operation = 'Update table_v_bd3', title = %s, add_date = %s WHERE table_v_bd2.title = %s and table_v_bd2.author = %s"
            current_datetime = datetime.now()
            cursor.execute(command, (new[0], current_datetime, changes[0], changes[1]))

        command = "SELECT table_v_bd1.title FROM table_v_bd1 WHERE table_v_bd1.title = %s and table_v_bd1.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            command = "UPDATE table_v_bd1 SET operation = 'Update table_v_bd2', title = %s, add_date = %s WHERE table_v_bd1.title = %s and table_v_bd1.author = %s"
            current_datetime = datetime.now()
            cursor.execute(command, (new[0], current_datetime, changes[0], changes[1]))

    if(base == 2):
        command = "SELECT table_v_bd3.title FROM table_v_bd3 WHERE table_v_bd3.title = %s and table_v_bd3.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            command = "UPDATE table_v_bd3 SET operation = 'Update table_v_bd2', title = %s, add_date = %s WHERE table_v_bd3.title = %s and table_v_bd3.author = %s"
            current_datetime = datetime.now()
            cursor.execute(command, (new[0], current_datetime, changes[0], changes[1]))

        command = "SELECT table_v_bd1.title FROM table_v_bd1 WHERE table_v_bd1.title = %s and table_v_bd1.author = %s"
        cursor.execute(command, (changes[0], changes[1]))
        if(len(cursor.fetchall()) > 0):
            command = "UPDATE table_v_bd1 SET operation = 'Update table_v_bd2', title = %s, add_date = %s WHERE table_v_bd1.title = %s and table_v_bd1.author = %s"
            current_datetime = datetime.now()
            cursor.execute(command, (new[0], current_datetime, changes[0], changes[1]))


    connect.commit()

