
# coding: utf-8

# In[3]:

import pyodbc
import pandas 
import sys

def print_full(x):
    pandas.set_option('display.max_rows', len(x))
    print(x)
    pandas.reset_option('display.max_rows')
    
SQL_connection_string = """
Driver={SQL Server Native Client 11.0};Server=tcp:michaelmarks.database.windows.net,1433;Database=Twitter_Output;Uid=marks@michaelmarks;Pwd={$tr0ngP@ssword};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;
"""
SQL_Connection = pyodbc.connect(SQL_connection_string)
cursor = SQL_Connection.cursor()
print sys.argv
if len(sys.argv) >1:
    SQL_Script = """Select sum(WordCount) as WordCount from Word_Counts Where word like \'""" + str(sys.argv[1]) + """\' group by Word order by WordCount desc"""
    df = pandas.io.sql.read_sql(SQL_Script, SQL_Connection)
    SQL_Connection.close()
    print "Total Number of Occurences of \""+str(sys.argv[1]) + "\": "  + str(int(df['WordCount']))
else:
    SQL_Script = """Select Word, sum(WordCount) as WordCount from Word_Counts group by Word order by Word asc"""
    df = pandas.io.sql.read_sql(SQL_Script, SQL_Connection)
    SQL_Connection.close()       
    print print_full(df)







