import os

import psycopg2
import pyodbc
from dotenv import load_dotenv

load_dotenv()

env = os.environ

server = env.get('server')
database = env.get('database')
username = env.get('username_')
password = env.get('password')

rds_user_name = env.get('rds_user_name')
rds_password = env.get('rds_password')
rds_host = env.get('rds_host')
rds_port = env.get('rds_port')
rds_database = env.get('rds_database')


# print(rds_user_name, rds_password, rds_host, rds_port, rds_database)
# print(server, database, username, password)

try:
    try:
        postgres_db = psycopg2.connect(user=rds_user_name, password=rds_password, host=rds_host, port=rds_port, database=rds_database)

        mssql_db = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

        postgres_db.autocommit = True

        postgres_cursor = postgres_db.cursor()

        mssql_cursor = mssql_db.cursor()
    except Exception as error:
        print("Error --", error)

    else:
        postgres_cursor.execute('''
        SELECT "TCUModel",
             "IMEI",
             "ICCID",
             "TMLPN",
             "ProdDate",
             "UIN",
             "TCUTestStatus",
             "TCUFirmwareVersion",
             "BootstrapExpDate",
             "SIMServiceOperator",
             null,
             "emission_type",
             "sim_vendor" ,
             NOW()
        FROM api_ccpeolaco where is_transfer=False''')

        TML_UNIT_DATA = postgres_cursor.fetchall()
        print(TML_UNIT_DATA)
        for i in TML_UNIT_DATA:
            UIN = i[5]
            try:
                i = list(i)
                values_ = f"""
                    if not exists( select [UIN] from [dbo].[ACCOLADE_DEVICE_DATA] where UIN='{UIN}')
                    BEGIN
                        INSERT INTO [dbo].[ACCOLADE_DEVICE_DATA]
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
                               ,[CERT_TIMESTAMP]
                               ,[emission_type]
                               ,[sim_vendor]
                               ,[RecordCreatedDate]
                               )
                        VALUES
                               (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    END
                    """
                mssql_cursor.execute(values_, i)

                mssql_cursor.commit()
                print("commit done ", UIN)
            except Exception as e:
                print(e)
            else:
                script_set = f"""UPDATE api_ccpeolaco SET is_transfer = True WHERE "UIN"='{UIN}';"""
                postgres_cursor.execute(script_set)
                print('data updated')


    finally:
        if postgres_db:
            postgres_cursor.close()
            postgres_db.close()
            print("PostgreSQL connection is closed")

        if mssql_db:
            mssql_cursor.close()
            mssql_db.close()
            print("MS SQL connection is closed")
except Exception as e:
    error_values_quiery = """
        INSERT INTO Tablename (uin, error)
        VALUES
            (?,?);
        """
    mssql_cursor.execute(error_values_quiery, (UIN or None, str(e)))
    mssql_cursor.commit()

