
# coding: utf-8

# In[ ]:

# Import libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import re
import psycopg2
import sys

from IPython.display import display, HTML # used to print out pretty pandas dataframes\n

# querying oracle
import cx_Oracle

# prompt for password without echoing it
import getpass

def load_query(fn):
    # load SQL code which gets census info for patients
    with open(fn, 'r') as f:
        qry = f.read()


    # remove empty lines
    while '\n\n' in qry:
        qry = qry.replace('\n\n','\n')

    # remove semicolon at end
    qry = qry.rstrip('\n').rstrip(';')
    return qry


# In[ ]:

# configure the connections for both oracle and a local postgres database

# PostgreSQL config which works on pc70
sqluser = getpass.getuser()
dbname = 'mimic'
schema_name = 'mimiciii'


print('PostgreSQL username: ' + sqluser)
print('PostgreSQL database: ' + dbname)
print('PostgreSQL schema: ' + schema_name)

# Connect to local postgres version of mimic
con = psycopg2.connect(dbname=dbname, user=sqluser)

# Oracle which works on pc70 and connects to hera (need username/password)
dbservice = 'MIMIC2'
dbstring = 'localhost:3309/' + dbservice

# connect to oracle **requires a tunnel in the background** - pc70 is configured to use port 3309
# tunnel command: ssh -n -N -f -L 3309:localhost:1521 alistairewj@hera
print('Oracle Username: ' + sqluser)
db = cx_Oracle.connect(getpass.getuser() + '/' + getpass.getpass('Oracle Password: ') + '@' + dbstring)
print('Oracle v' + db.version)


# In[ ]:

qry = load_query('sql/0-phi-info.sql')

# execute query on database and get resultant rows
cur = db.cursor()
cur.execute(qry)
data = cur.fetchall()
        
# extract column names from description
colNames = cur.description
colNames = [x[0] for idx, x in enumerate(colNames)]
cur.close()

# save data in dataframe
df_phi = pd.DataFrame.from_records(data, index=None, exclude=None,
                               columns=colNames,
                               coerce_float=False, nrows=None)

# convert columns to lowercase - makes them consistent with postgresql
df_phi.columns = [x.lower() for x in df_phi.columns]


# In[ ]:

# run all the SQL scripts for PostgreSQL - these generate materialized views
cur = con.cursor()
cur.execute('SET search_path to ' + schema_name)
qry = load_query('sql/1-define-cohort.sql')
cur.execute(qry)
qry = load_query('sql/2-icu-variables.sql')
cur.execute(qry)
qry = load_query('sql/3-final-design-matrix.sql')
cur.execute(qry)
cur.execute('commit;')
cur.close()

# final query which actually pulls the data
qry = """
select 
    *
from dd_design_matrix
order by icustay_id
"""
df = pd.read_sql_query(qry,con)


# We now have two dataframes: `df` with most of the callout data (extracted from a local PostgreSQL instance) and `df_phi` with the census related data (extracted from an Oracle instance which connects to hera).

# In[ ]:

# write the data to file
df_phi.to_csv('callout-phi.csv',sep=',',na_rep='NA',header=True,index=False)
df.to_csv('callout.csv',sep=',',na_rep='NA',header=True,index=False)


# In[ ]:

# print some stats
print('Data size, normal: {}, PHI: {}'.format(df.shape[0],df_phi.shape[0]))
if df.shape[0]==df_phi.shape[0]:
    print('Overlap in ICUSTAY_ID: {:2.2f}'.format( len(np.union1d(df['icustay_id'],df_phi['icustay_id'])) / df.shape[0] * 100.0 ))

