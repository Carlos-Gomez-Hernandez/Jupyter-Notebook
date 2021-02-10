import pandas as pd
import pyodbc
import os
import credentials #credentials.py located in this directory
import queries #queries.py located in this directory

#-------------------------------------------------------------------------
    #ODBC connection to TERADATA
#-------------------------------------------------------------------------

#query string imported from the query files
queryA = queries.data_limit_exceed_time_pure_average
queryB = queries.data_limit_exceed_time_days_dlist
queryC = queries.data_limit_exceed_time_average_by_plans

#connection string object.
conn = 'DRIVER={DRIVERNAME};DBCNAME={hostname};UID={uid};PWD={pwd};authentication={auth}'.format(
                            DRIVERNAME='Teradata Database ODBC Driver 16.20',
                            hostname='tdidwcop1.unix.gsm1900.org',
                            uid=credentials.tera_username,
                            pwd=credentials.tera_password,
                            auth='LDAP')

#Database connection object.
connection = pyodbc.connect(conn)

#-------------------------------------------------------------------------
    #Retrieve data to dataFrame
#-------------------------------------------------------------------------
with pyodbc.connect(conn) as connect:
    df0 = pd.read_sql(queryA,connect)
    df1 = pd.read_sql(queryB,connect)
    df2 = pd.read_sql(queryC,connect)

#-------------------------------------------------------------------------
    # Push Data to teradata tables
#-------------------------------------------------------------------------
#Enable autocommit for DDL commands
connection.autocommit = True;

#Cursor handler for the sql statements, this will be used to push data into the database.
cursor = connection.cursor() 

#Insert Values from Dataframe pure_average ((df1)) into teradata
cursor.executemany(f"""INSERT INTO USER_DW.data_limit_exceed_time_pure_average VALUES (CAST(? AS DATE FORMAT 'MM/DD/YYYY'),?,?,?,?)""",
                   df0.values.tolist())

#Insert Values from Dataframe days_dlist (df1) into teradata
cursor.executemany(f"""INSERT INTO USER_DW.data_limit_exceed_time_days_dlist VALUES (CAST(? AS DATE FORMAT 'MM/DD/YYYY'),?,?,?)""",
                   df1.values.tolist())

#Insert Values from Dataframe average_by_plans (df2) into teradata
cursor.executemany(f"""INSERT INTO USER_DW.data_limit_exceed_time_average_by_plans VALUES (CAST(? AS DATE FORMAT 'MM/DD/YYYY'),?,?,?)""",
                   df2.values.tolist())

#-------------------------------------------------------------------------

#close cursor to close connection
cursor.close()

#close connection so it does not take resources
connection.close()