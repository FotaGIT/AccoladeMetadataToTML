import os

import psycopg2
import pyodbc
from dotenv import load_dotenv

if env_var := load_dotenv(".env"):
    # ----------------mssql------------------
    server = os.environ.get('ms_server', None)
    database = os.environ.get('ms_database', None)
    username = os.environ.get('ms_username', None)
    password = os.environ.get('ms_password', None)

    # ----------------postgres------------------
    pg_user = os.environ.get('pg_user', None)
    pg_password = os.environ.get('pg_password', None)
    pg_host = os.environ.get('pg_host', None)
    pg_database = os.environ.get('pg_database', None)
    pg_port = os.environ.get('pg_port', None)

try:
    connection1 = psycopg2.connect(user=pg_user,password=pg_password, host=pg_host, port=pg_port, database=pg_database)
    connection2 = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

    connection1.autocommit = True

    cursor1 = connection1.cursor()
    cursor2 = connection2.cursor()
except Exception as error:
    print("Error --", error)

else:
    cursor1.execute('''SELECT * FROM api_ccpeolaco where is_transfer=False''')
    TML_UNIT_DATA = cursor1.fetchall()
    for i in TML_UNIT_DATA:
        print(i)
        try:
            # cursor2.execute(SQLCommand, i)
            i = list(i)
            i[11] = None
            values_ = """
                INSERT INTO [dbo].[accolade_data]
                       ([TCUModel]
                       ,[IMEI]
                       ,[ICCID]
                       ,[TMLPN]
                       ,[ProdDate]
                       ,[UIN]
                       ,[TCUTestStatus]
                       ,[TCUFirmwareVersion]
                       ,[BootstrapExpDate]
                       ,[SIMServiceOperator]
                       ,[is_transfer]
                       ,[CERT_TIMESTAMP]
                       ,[created_on]
                       ,[emission_type]
                       ,[modified_on]
                       ,[sim_vendor])
                VALUES
                       (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)

                """
            cursor2.execute(values_, i)

            cursor2.commit()
        except Exception as e:
            print(e)
        else:
            script_set = f"""UPDATE api_ccpeolaco SET is_transfer = True WHERE "UIN"='{i[5]}';"""
            cursor1.execute(script_set)
            print('data updated')


finally:
    if connection1:
        cursor1.close()
        connection1.close()
        print("PostgreSQL connection is closed")

    if connection2:
        cursor2.close()
        connection2.close()
        print("MS SQL connection is closed")
