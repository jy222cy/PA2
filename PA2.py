import mysql.connector
from mysql.connector import errorcode
import csv

# Authors:
# Erik Wen Han Sun  es225ki@student.lnu.se
# Jianing Yang  jy222cy@student.lnu.se


# the tutorial video is at https://www.youtube.com/watch?v=tYOWfqg9yuQ

# making connection here.
cnx= mysql.connector.connect(user='root',
                             password='root',
                             host='127.0.0.1')
                             #unix_socket= '/Applications/MAMP/tmp/mysql/mysql.sock',)

DB_NAME = 'PA2' # database name

cursor = cnx.cursor() # cursor

# This way of making a database is insipred by and modified from Ilir Jusufi's Workshop codes
# Here's the link: https://www.codepile.net/pile/aoyNe2kP
def create_database(cursor, DB_NAME): # create database here
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Faild to create database {}".format(err))
        exit(1)


# This Function of making a table in database is insipred by and modified from Ilir Jusufi's Workshop codes
# Here's the link:  https://www.codepile.net/pile/aoyNe2kP
def create_table_patientinfo(cursor): # creating the table, attributs and their datatypes
    creat_cars = "CREATE TABLE `patientinfo` (" \
                 "  `id` varchar(225) NOT NULL ," \
                 "  `firstname` varchar(255) NOT NULL," \
                 "  `lastname` varchar(255) NOT NULL," \
                 "  `phonenumber` varchar(225) NOT NULL," \
                 "  `address` varchar(255) NOT NULL," \
                 "  `age` varchar(255) NOT NULL," \
                 "  PRIMARY KEY (`id`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table patientinfo: ")
        cursor.execute(creat_cars)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


# This way of inserting data into table is insipred by and modified from a Youtube tutorial video and here's the citation:
#The Code City (2020) Read data from CSV and insert into Database | Python Tutorial .https://www.youtube.com/watch?v=LNgg_YJ29OY&list=PLfx9Y__lmb-ky2cQLea0HSUvm2Pyz7yxs&index=5
def insert_patientDATA(cursor): # this function is about opening and reading file and inserting data into table planets
    with open('patientinfo.csv') as csvfile: # open file
        read_file = csv.reader(csvfile) # read file
        for tuple in read_file: # read through
          data=(tuple[0], tuple[1], tuple[2], tuple[3], tuple[4], tuple[5] ) # data based 6 indexes
          insert="""INSERT INTO patientinfo (id, firstname, lastname, phonenumber, address, age) 
                VALUES (%s, %s, %s, %s, %s, %s)""" # sql for inserting data into the table
          try:          
              cursor.execute(insert,data) # run sql
          except mysql.connector.Error as err:
                print(err.msg)
          else:
              cnx.commit()


# This function of making a table in database is insipred by and modified from Ilir Jusufi's Workshop codes
# Here's the link:  https://www.codepile.net/pile/aoyNe2kP
def create_table_shotinfo(cursor): # setting up the table, attributs and their datatypes
    creat_cars = "CREATE TABLE `shotinfo` (" \
                 "  `patientid` varchar(225) NOT NULL ," \
                 "  `shot1` varchar(255) NOT NULL," \
                 "  `shot2` varchar(255) NOT NULL," \
                 "  `shot3` varchar(225) NOT NULL," \
                 "  PRIMARY KEY (`patientid`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table shotinfo: ")
        cursor.execute(creat_cars)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


# This way of inserting data into table is insipred by and modified from a Youtube tutorial video and here's the citation:
#The Code City (2020) Read data from CSV and insert into Database | Python Tutorial .https://www.youtube.com/watch?v=LNgg_YJ29OY&list=PLfx9Y__lmb-ky2cQLea0HSUvm2Pyz7yxs&index=5
def insert_shotinfoDATA(cursor): # this function is about opening and reading file and inserting data into table planets
    with open('shotinfo.csv') as csvfile: # open file
        read_file = csv.reader(csvfile) # read file
        for tuple in read_file: # read through
          data=(tuple[0], tuple[1], tuple[2], tuple[3] ) # data based 9 indexes
          insert="""INSERT INTO shotinfo (patientid, shot1, shot2, shot3) 
                VALUES (%s, %s, %s, %s)""" # sql for inserting data into the table
          try:          
              cursor.execute(insert,data) # run sql
          except mysql.connector.Error as err:
                print(err.msg)
          else:
              cnx.commit()


# This function of making a table in database is insipred by and modified from Ilir Jusufi's Workshop codes
# Here's the link:  https://www.codepile.net/pile/aoyNe2kP
def create_table_shotCompany(cursor): # setting up the table, attributs and their datatypes
    creat_cars = "CREATE TABLE `shotcompany` (" \
                 "  `patientsid` varchar(225) NOT NULL ," \
                 "  `shot1info` varchar(255) NOT NULL," \
                 "  `shot2info` varchar(255) NOT NULL," \
                 "  `shot3info` varchar(225) NOT NULL," \
                 "  PRIMARY KEY (`patientsid`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table shotcompany: ")
        cursor.execute(creat_cars)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


# This way of inserting data into table is insipred by and modified from a Youtube tutorial video and here's the citation:
#The Code City (2020) Read data from CSV and insert into Database | Python Tutorial .https://www.youtube.com/watch?v=LNgg_YJ29OY&list=PLfx9Y__lmb-ky2cQLea0HSUvm2Pyz7yxs&index=5
def insert_shotCompany(cursor): # this function is about opening and reading file and inserting data into table planets
    with open('shotcompany.csv') as csvfile: # open file
        read_file = csv.reader(csvfile) # read file
        for tuple in read_file: # read through
          data=(tuple[0], tuple[1], tuple[2], tuple[3] ) # data based 9 indexes
          insert="""INSERT INTO shotcompany (patientsid, shot1info, shot2info, shot3info) 
                VALUES (%s, %s, %s, %s)""" # sql for inserting data into the table
          try:          
              cursor.execute(insert,data) # run sql
          except mysql.connector.Error as err:
                print(err.msg)
          else:
              cnx.commit()


# majority of the functions created are being runned here 
try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exist".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor, DB_NAME)
        print("Database {} created succesfully.".format(DB_NAME))
        cnx.database = DB_NAME
        create_table_patientinfo(cursor)
        insert_patientDATA(cursor)

        create_table_shotinfo(cursor)
        insert_shotinfoDATA(cursor)

        create_table_shotCompany(cursor)
        insert_shotCompany(cursor)

    else:
        print(err)



# setting up the menu for options here.
menu = """ 
1. Print the names of patient who have NOT taken ANY shots of vaccine
2. print patient names that is ordered by their id
3. print the patient names who's got 3 shots of vaccine
4. print the average age of patients who have taken 2 shots vaccine
5. print the number of Jonsson vaccine that has been taken by the patients
6. Print the contact info of patients who needs to take the second vaccine
Q. Quit
--------- """
print(menu) # printing the menu

option= input("Please choose one option: ")  # asking for user's input
#option ="1"
if option == "Q": # if the option is Q, then stop the program
    exit(1)



# Print the names of patient who have NOT taken ANY shots of vaccine
if option == "1": # this query uses create view, use view, and JOIN
    query = """ CREATE OR REPLACE VIEW firstQuery AS 
                SELECT firstname, lastname ,shotinfo.shot1
                FROM patientinfo
                join shotinfo on patientinfo.id = shotinfo.patientid """
    cursor.execute(query) # this sql is for creating the view on database
    
    query = """SELECT firstname, lastname
                FROM firstQuery
                WHERE shot1 = 'no' """ 
    cursor.execute(query)  # this sql is for using the view and display it in the teminal
    print("\nPrint the names of patient who have NOT taken ANY shots of vaccine ")
    print("|{:<11}|{:<11}|".format("First name","Last name"))
    for (i,j) in cursor:
        print("|{:<11}|{:<11}|".format(i, j)) # printing results


# print patient names that is ordered by their id
if option == "2":
    # query for GROUP BY here
    query2 = """SELECT id, firstname, lastname
                FROM patientinfo
                WHERE firstname != 'firstname'
                GROUP BY id """  # sql 
    cursor.execute(query2) # run sql
    print("\nPrint the patient name that is ordered by the id ")
    print("|{:<7}|{:<10}|{:<10}|".format("id","First name","Last name"))
    for (id, firstname,lastname) in cursor:
        print("|{:<7}|{:<10}|{:<10}|".format(id, firstname,lastname)) # printing results


# print the patient names who's got 3 shots of vaccine
if option == "3":
    # query for JOIN and GROUP BY here
    query3 = """SELECT firstname, lastname
                FROM patientinfo
                join shotinfo on patientinfo.id = shotinfo.patientid
                WHERE shotinfo.shot3 = 'yes' 
                GROUP BY id"""  # sql 
    cursor.execute(query3) # run sql
    print("\nPrint the names of patient who have taken ALL 3 shots of vaccines ")
    print("|{:<11}|{:<11}|".format("First name","Last name"))
    for (i,j) in cursor:
        print("|{:<11}|{:<11}|".format(i, j)) # printing results


# print the average age of patients who have taken 2 shots vaccine
if option == "4":
    # query for JOIN, Aggregation here
    query4 = """SELECT AVG(age)
                FROM patientinfo
                JOIN shotinfo on patientinfo.id = shotinfo.patientid
                WHERE shotinfo.shot2 = 'yes' """  # sql 
    cursor.execute(query4) # run sql
    for i in cursor:
        print("\nThe average age of patients who took 2 shots of vaccines is: {}\n".format(round(i[0]))) # printing results


# print the number of Jonsson vaccine that has been taken by the patients
if option == "5":
    # for counting company shots and it uses JOIN and aggreagation
    query = """SELECT COUNT(* )
                FROM patientinfo
                join shotcompany on patientinfo.id = shotcompany.patientsid
                WHERE shotcompany.shot1info = 'Jonsson' """  # sql 
    cursor.execute(query) # run sql
    for i in cursor:
        print() # printing results


    # for counting company shots 
    query = """SELECT COUNT(* )
                FROM patientinfo
                join shotcompany on patientinfo.id = shotcompany.patientsid
                WHERE shotcompany.shot2info = 'Jonsson' """  # sql 
    cursor.execute(query) # run sql
    for j in cursor:
        print() # printing results
        # for counting company shots 


    query = """SELECT COUNT(* )
                FROM patientinfo
                join shotcompany on patientinfo.id = shotcompany.patientsid
                WHERE shotcompany.shot3info = 'Jonsson' """  # sql 
    cursor.execute(query) # run sql
    for k in cursor:
        print() # printing results
    total = i[0] + j[0] + k[0]

    print("the number of Jonsson Vaccine that has been taken is:", total, "\n")



# print contact information of patients who needs to take the second vaccine shot
if option == "6":
    query = """ SELECT firstname, lastname, phonenumber,address
                FROM patientinfo
                join shotinfo on patientinfo.id = shotinfo.patientid
                WHERE shotinfo.shot1 = 'yes' AND shotinfo.shot2 = 'no' """ 
    cursor.execute(query) # run sql
    print("Print the contact info of patients who needs to take the second vaccine")
    print("|{:<11}|{:<11}|{:<11}|{:<19}|".format("First name","Last name", "phonenumber", "address"))
    for (firstname,lastname,phonenumber,address) in cursor:
        print("|{:<11}|{:<11}|{:<11}|{:<19}|".format(firstname,lastname,phonenumber,address))

