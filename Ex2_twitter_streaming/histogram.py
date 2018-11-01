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


if len(sys.argv) > 1 and int(sys.argv[1]) < int(sys.argv[2]):
    SQL_Script = """Select Word, sum(WordCount) as WordCount from Word_Counts group by Word having sum(WordCount) >""" \
    + str(sys.argv[1]) +  """ and sum(WordCount) < """+ str(sys.argv[2]) 
    df = pandas.io.sql.read_sql(SQL_Script, SQL_Connection)
    SQL_Connection.close()
    print print_full(df)
else:      
    print "Error: Two arguments needed. Arg1 < Arg2"