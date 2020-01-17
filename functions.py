import re
import pandas as pd
import dotenv
import os
import psycopg2 as ps
import numpy as np

dotenv.load_dotenv()

# Mirar si es necesario cerrar la conexion a PS (el cursor)

def connectToPs():
    host=os.getenv('HOSTNAME')
    database=os.getenv('NAME')
    user=os.getenv('USER')
    password=os.getenv('PASSWORD')
    print(f'Connecting to Postgree...\n')
    conn = ps.connect(host=host, database=database, user=user, password=password)
    print(conn)
    return conn.cursor()

def getFamilyFrom(description):
    if re.search('MUSIC', description.upper()):
        return 'X Music'
    elif re.search('PRIVACY', description.upper()):
        return 'X Privacy'
    elif re.search('PROTECTION', description.upper()):
        return 'X Protection'
    elif re.search('FIBER', description.upper()):
        return 'X Fiber'
    elif re.search('SECURITY', description.upper()):
        return 'X Security'
    elif re.search('UCOM', description.upper()):
        return 'X UCom'

def getVelocityFrom(description):
    if re.findall('[0-9]{3}|[0-9]{2}', description):
        return re.findall('[0-9]{3}|[0-9]{2}', description)[0]+' Mbps'
    else:
        return ''

def isNeba(description):
    if re.search('NEBA', description.upper()):
        return 1
    else:
        return 0

def ventas(cur):
    cur.execute('''
                select segmento, assetid, product_name, purchase_date, canal_venta, cif
                from cartera_xbo
                where process_date = (
                select max(process_date) from cartera_xbo)
                and segmento in ('3-NOTRIAL', '4-MIGRADO')
                and upper(product_name) not like '%CENTRALITA%'
                and assetpadre_mig isnull
                ''')
    df = pd.DataFrame(cur.fetchall())
    col_names = []
    for e in range(len(cur.description)):
        col_names.append(cur.description[e][0])
    df.rename(columns = dict(zip(list(range(33)), col_names)), inplace = True)
    df['purchase_month'] = df.purchase_date.apply(lambda x: x.month)
    df['purchase_year'] = df.purchase_date.apply(lambda x: x.year)
    df['Family'] = df.product_name.apply(lambda x: getFamilyFrom(x))
    df['Velocity'] = df.product_name.apply(lambda x: getVelocityFrom(x))
    df['isNeba'] = df.product_name.apply(lambda x: isNeba(x))
    df['Total'] = 1
    return df


def altas(cur):
    cur.execute('''
                select segmento, assetid, product_name, rfb_date, canal_venta, cif
                from cartera_xbo
                where process_date = (
                select max(process_date) from cartera_xbo)
                and segmento in ('3-NOTRIAL', '4-MIGRADO')
                and upper(product_name) not like '%CENTRALITA%'
                and assetpadre_mig isnull
                ''')
    df = pd.DataFrame(cur.fetchall())
    col_names = []
    for e in range(len(cur.description)):
        col_names.append(cur.description[e][0])
    df.rename(columns = dict(zip(list(range(33)), col_names)), inplace = True)
    df['alta_month'] = df.rfb_date.apply(lambda x: x.month)
    df['alta_year'] = df.rfb_date.apply(lambda x: x.year)
    df['Family'] = df.product_name.apply(lambda x: getFamilyFrom(x))
    df['Velocity'] = df.product_name.apply(lambda x: getVelocityFrom(x))
    df['isNeba'] = df.product_name.apply(lambda x: isNeba(x))
    df['Total'] = 1
    df = df[np.isfinite(df['alta_month'])]
    df = df.astype({'alta_month': 'int64', 'alta_year':'int64'})
    return df   


def altas_cif(altas,tipo):
    if tipo == 'Family':
        df = altas[['Family','cif','rfb_date']]
        df = df.groupby(['Family','cif'])['rfb_date'].min().to_frame().reset_index()
        df['alta_month'] = df.rfb_date.apply(lambda x: x.month)
        df['alta_year'] = df.rfb_date.apply(lambda x: x.year)
        df.drop('rfb_date', axis=1, inplace=True)
        df = df.groupby(by=['Family','alta_year','alta_month']).count().reset_index()
        return df
    if tipo == 'Total':
        df = altas[['cif','rfb_date']]
        df = df.groupby(['cif'])['rfb_date'].min().to_frame().reset_index()
        df['alta_month'] = df.rfb_date.apply(lambda x: x.month)
        df['alta_year'] = df.rfb_date.apply(lambda x: x.year)
        df.drop('rfb_date', axis=1, inplace=True)
        df = df.groupby(by=['alta_year','alta_month']).count().reset_index()
        return df

def rfb(row):
    return row['rfb_migration'] if row['migrado'] == '1' else row['rfb_date']

def bajas(cur):
    cur.execute('''
            select segmento, assetid, product_name, migrado,rfb_date, rfb_migration, deactivation_date, canal_venta, cif
            from cartera_xbo
            where process_date = (
            select max(process_date) from cartera_xbo)
            and segmento in ('3-NOTRIAL')
            and upper(product_name) not like '%CENTRALITA%'
            and rfb_date notnull
            ''')
    df = pd.DataFrame(cur.fetchall())
    col_names = []
    for e in range(len(cur.description)):
        col_names.append(cur.description[e][0])
    df.rename(columns = dict(zip(list(range(33)), col_names)), inplace = True)
    df['rfb__c'] = df.apply(lambda row: rfb(row), axis = 1)
    df.drop(columns=['rfb_date','rfb_migration'], axis=1, inplace=True)
    df['Family'] = df.product_name.apply(lambda x: getFamilyFrom(x))
    df['Velocity'] = df.product_name.apply(lambda x: getVelocityFrom(x))
    df['isNeba'] = df.product_name.apply(lambda x: isNeba(x))
    df['Total'] = 1
    return df