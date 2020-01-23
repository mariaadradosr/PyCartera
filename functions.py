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
        return 'X MUSIC'
    elif re.search('PRIVACY', description.upper()):
        return 'X PRIVACY'
    elif re.search('PROTECTION', description.upper()):
        return 'X PROTECTION'
    elif re.search('FIBER', description.upper()):
        return 'X FIBER'
    elif re.search('SECURITY', description.upper()):
        return 'X SECURITY'
    elif re.search('UCOM', description.upper()):
        return 'X UCOM'
    elif re.search('MOBILE', description.upper()):
        return 'X MOBILE'

def getVelocityFrom(description):
    if re.findall('[0-9]{3}|[0-9]{2}', description):
        return re.findall('[0-9]{3}|[0-9]{2}', description)[0]
        # return re.findall('[0-9]{3}|[0-9]{2}', description)[0]+' Mbps'
    else:
        return ''

def isNeba(description):
    if re.search('FIBER', description.upper()) and (re.search('NEBA', description.upper()) or re.search('IP', description.upper())) :
        return 1
    else:
        return 0

def getProductName(row):
    if row['Velocity'] != '' and row['isNeba'] == 0 and row['Family'] != 'X FIBER':
        return row['Family'].title()+' '+row['Velocity']
    if row['Velocity'] != '' and row['isNeba'] == 0 and row['Family'] == 'X FIBER':
        return row['Family'].title()+' '+row['Velocity']+' - '+'own'
    if row['isNeba'] == 1:
        return row['Family'].title()+' '+row['Velocity']+' - '+'neba'
    if row['Family'] == 'X UCOM':
        return 'Teamwork+PBX'
    if row['Family'] == 'X MOBILE':
        return row['product_name'].title()
    else:
        return 'Assets'


def canal(canal_venta):
    if re.search('DHO', canal_venta.upper()):
        return 'DHO'
    elif re.search('INTEGRA', canal_venta.upper()):
        return 'IT integrator'
    elif re.search('WEB', canal_venta.upper()):
        return 'WEB'

def rfb(row):
    return row['rfb_migration'] if row['migrado'] == '1' else row['rfb_date']

def purchase(row):
    return row['purchase_migration'] if row['migrado'] == '1' else row['purchase_date']

def purchaseToAsset(row):
    return 0 if row['rfb__c'] is pd.NaT else 1

def baja__c(row):
    return 1 if row['deactivation_date'] is not pd.NaT else 0

def cancelledSales(row):
    if row['cancellation_date'] is pd.NaT and row['rfb__c'] != pd.NaT:
        return 0
    else:
        return 1

def deactivation(row):
    return pd.NaT if row['isActive'] >= 1 else row['deactivation_date']

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
    df['Bajas__c'] = df.apply(lambda row: baja__c(row), axis = 1)
    df['assets_act'] = df['Total'] + df['Bajas__c']
    return df

def altas_bajas(df):
    assets = df.Total.groupby(by=[df.cif,df.canal_venta]).sum().reset_index()
    assets_act = df.assets_act.groupby(by=[df.cif,df.canal_venta]).sum().reset_index()
    altas = df.rfb__c.groupby(by=[df.cif,df.canal_venta]).min().reset_index()
    bajas = df.deactivation_date.groupby(by=[df.cif,df.canal_venta]).max().reset_index()
    final = pd.merge(pd.merge(pd.merge(assets_act,altas,on=['cif','canal_venta']),bajas,on=['cif','canal_venta']),assets,on=['cif','canal_venta'])
    final['isActive'] = final.assets_act.apply(lambda x: 1 if x>=1 else 0)
    final['deactivation_date'] = final.apply(lambda row: deactivation(row), axis = 1)
    final['assets_dea'] = final['Total'] - final['assets_act']
    final['isBaja'] = -(final['isActive']-1)
    final['Client'] = 1
    final.rename(columns={'Total':'assets_tot',
                      'rfb__c':'rfb',
                      }, inplace = True)
    return final

def altas_bajas_mes(altas_bajas_df):
    altas = altas_bajas_df[['Client']].groupby(by=[altas_bajas_df.rfb]).sum()
    bajas = altas_bajas_df[['isBaja']].groupby(by=[altas_bajas_df.deactivation_date]).sum()
    bajas.index.name = 'rfb'
    altas_bajas = altas.join(bajas, how='outer').fillna(0.0)
    mes = altas_bajas.groupby(by=[altas_bajas.index.year, altas_bajas.index.month]).sum()
    mes.index.names = ['year', 'month']
    mes.reset_index(inplace=True)
    return mes

def altas_bajas_mes_canal(altas_bajas_df):
    altas = altas_bajas_df[['Client']].groupby(by=[altas_bajas_df.rfb, altas_bajas_df.canal_venta]).sum()
    bajas = altas_bajas_df[['isBaja']].groupby(by=[altas_bajas_df.deactivation_date,altas_bajas_df.canal_venta]).sum()
    bajas.index.names = ['rfb','canal_venta']
    altas_bajas = altas.join(bajas, how='outer').fillna(0.0)
    mes_canal = altas_bajas.groupby(by=[altas_bajas.index.get_level_values('rfb').year, altas_bajas.index.get_level_values('rfb').month, altas_bajas.index.get_level_values('canal_venta')]).sum()
    mes_canal.index.names = ['year', 'month', 'canal']
    mes_canal.reset_index(inplace=True)
    return mes_canal


def ventas_2(cur):
    cur.execute('''
            select segmento, assetid, cif, canal_venta, product_name, 
                    migrado,rfb_date, rfb_migration, deactivation_date,
                    purchase_date, purchase_migration, deactivation_request_date
            from cartera_xbo
            where process_date = (
            select max(process_date) from cartera_xbo)
            and segmento in ('3-NOTRIAL')
            and upper(product_name) not like '%CENTRALITA%'
            ''')
    df = pd.DataFrame(cur.fetchall())
    col_names = []
    for e in range(len(cur.description)):
        col_names.append(cur.description[e][0])
    df.rename(columns = dict(zip(list(range(33)), col_names)), inplace = True)
    df['rfb__c'] = df.apply(lambda row: rfb(row), axis = 1)
    df.drop(columns=['rfb_date','rfb_migration'], axis=1, inplace=True)
    df['purchase_date__c'] = df.apply(lambda row: purchase(row), axis = 1)
    df.drop(columns=['purchase_migration','purchase_date'], axis=1, inplace=True)
    df['Family'] = df.product_name.apply(lambda x: getFamilyFrom(x))
    df['Velocity'] = df.product_name.apply(lambda x: getVelocityFrom(x))
    df['isNeba'] = df.product_name.apply(lambda x: isNeba(x))
    df['isSold'] = 1
    df['isAsset'] =  df.apply(lambda row: purchaseToAsset(row), axis = 1)
    df['isBaja'] = df.apply(lambda row: baja__c(row), axis = 1)
    df['isActive'] = df['isAsset'] + df['isBaja']
    to_drop = df[(df['rfb__c'].isnull())&((df['deactivation_date'].notnull())|(df['deactivation_request_date'].notnull()))].index
    df.drop(to_drop , inplace=True)
    df.drop(columns=['deactivation_request_date'], axis=1, inplace=True)
    return df



# -------------------------------------------------------------------

def detalle(cur):
    cur.execute('''
            select segmento, product_name, companyname, sitename, created_as_trial, purchase_date,
            cancellation_date, service_date, rfb_date, deactivation_date, canal_venta, partnername,
            price, cif, assetid, asset_status, migrado, assetpadre_mig, assethijo_mig, rfb_migration,
            date_migration, purchase_migration
            from cartera_xbo
            where process_date = (
            select max(process_date) from cartera_xbo)
            and upper(product_name) not like '%CENTRALITA%'
            ''')
    df = pd.DataFrame(cur.fetchall())
    col_names = []
    for e in range(len(cur.description)):
        col_names.append(cur.description[e][0])
    df.rename(columns = dict(zip(list(range(33)), col_names)), inplace = True)
    df['Family'] = df.product_name.apply(lambda x: getFamilyFrom(x))
    df['Velocity'] = df.product_name.apply(lambda x: getVelocityFrom(x))
    df['isNeba'] = df.product_name.apply(lambda x: isNeba(x))
    df['product_name_ok'] = df.apply(lambda row: getProductName(row), axis = 1)
    df['cod_postal'] = ''
    df['precio_dto'] = ''
    df['cod_postal'] = ''
    df['%_dto'] = ''
    df['Segmento_Empleados'] = ''
    cols = ['segmento','Family', 'product_name', 'companyname', 'sitename', 'cod_postal',
        'created_as_trial', 'purchase_date', 'cancellation_date', 
        'service_date', 'rfb_date', 'deactivation_date', 'canal_venta', 
        'partnername', 'price','precio_dto', '%_dto', 'cif', 'Segmento_Empleados','assetid', 'asset_status', 'migrado', 
        'assetpadre_mig', 'assethijo_mig', 'rfb_migration', 'date_migration', 'purchase_migration','product_name_ok']
    df = df[cols]
    return df


def base(cur):
    cur.execute('''
            select segmento, assetid, cif, canal_venta, product_name, 
                    migrado,rfb_date, rfb_migration, deactivation_date, cancellation_date,
                    purchase_date, purchase_migration, deactivation_request_date
            from cartera_xbo
            where process_date = (
            select max(process_date) from cartera_xbo)
            and segmento in ('3-NOTRIAL')
            and upper(product_name) not like '%CENTRALITA%'
            ''')
    df = pd.DataFrame(cur.fetchall())
    col_names = []
    for e in range(len(cur.description)):
        col_names.append(cur.description[e][0])
    df.rename(columns = dict(zip(list(range(33)), col_names)), inplace = True)
    df['canal_venta'] = df.canal_venta.apply(lambda x: canal(x))
    df['rfb__c'] = df.apply(lambda row: rfb(row), axis = 1)
    df.drop(columns=['rfb_date','rfb_migration'], axis=1, inplace=True)
    df['purchase_date__c'] = df.apply(lambda row: purchase(row), axis = 1)
    df.drop(columns=['purchase_migration','purchase_date'], axis=1, inplace=True)
    df['Family'] = df.product_name.apply(lambda x: getFamilyFrom(x))
    df['Velocity'] = df.product_name.apply(lambda x: getVelocityFrom(x))
    df['isNeba'] = df.product_name.apply(lambda x: isNeba(x))
    df['product_name'] = df.apply(lambda row: getProductName(row), axis = 1)
    df['isSold'] = 1
    df['isAsset'] =  df.apply(lambda row: purchaseToAsset(row), axis = 1)
    df['isBaja'] = df.apply(lambda row: baja__c(row), axis = 1)
    df['isActive'] = df['isAsset'] - df['isBaja']
    df['isCancelled'] = df.apply(lambda row: cancelledSales(row), axis = 1)
    to_drop = df[(df['rfb__c'].isnull())&((df['deactivation_date'].notnull())|(df['deactivation_request_date'].notnull()))].index
    df.drop(to_drop , inplace=True)
    df.drop(columns=['deactivation_request_date'], axis=1, inplace=True)
    return df

# -------------- VENTAS --------------

# VENTA ASSETS
def ass_vta_fam_mes(df,canal=0):
    if canal == 1:
        ass_vta_canal_fam_mes = df[['isSold']].groupby(by=[df.purchase_date__c.dt.year,df.purchase_date__c.dt.month,df.canal_venta,df.Family]).sum()
        ass_vta_canal_fam_mes.index.names = ['purchase_year', 'purchase_month', 'canal_venta','Family']
        ass_vta_canal_fam_mes.reset_index(inplace=True)
        return ass_vta_canal_fam_mes.pivot_table('isSold',['canal_venta','Family'],['purchase_year','purchase_month'],fill_value=0,margins=True,aggfunc=sum)
    elif canal == 0:
        ass_vta_fam_mes = df[['isSold']].groupby(by=[df.purchase_date__c.dt.year,df.purchase_date__c.dt.month,df.Family]).sum()
        ass_vta_fam_mes.index.names = ['purchase_year', 'purchase_month','Family']
        ass_vta_fam_mes.reset_index(inplace=True)
        return ass_vta_fam_mes.pivot_table('isSold','Family',['purchase_year','purchase_month'],fill_value=0,margins=True,aggfunc=sum)
    else:
        print('Canal: 0 or 1')

def ass_vta_prod_mes(df,canal=0):
    if canal == 1:
        ass_vta_canal_prod_mes = df[['isSold']].groupby(by=[df.purchase_date__c.dt.year,df.purchase_date__c.dt.month,df.canal_venta,df.Family,df.product_name]).sum()
        ass_vta_canal_prod_mes.index.names = ['purchase_year', 'purchase_month','canal_venta','Family','product_name']
        ass_vta_canal_prod_mes.reset_index(inplace=True)
        return ass_vta_canal_prod_mes.pivot_table('isSold',['canal_venta','Family','product_name'],['purchase_year','purchase_month'],fill_value=0,margins=True,aggfunc=sum)
    elif canal == 0:
        ass_vta_canal_prod_mes = df[['isSold']].groupby(by=[df.purchase_date__c.dt.year,df.purchase_date__c.dt.month,df.Family,df.product_name]).sum()
        ass_vta_canal_prod_mes.index.names = ['purchase_year', 'purchase_month','Family','product_name']
        ass_vta_canal_prod_mes.reset_index(inplace=True)
        return ass_vta_canal_prod_mes.pivot_table('isSold',['Family','product_name'],['purchase_year','purchase_month'],fill_value=0,margins=True,aggfunc=sum)
    else:
        print('Canal: 0 or 1')
        
# VENTAS CLIENTES


def cli_vta_fam_mes(df,canal=0):
    if canal == 0:
        cli_vta_canal_mes = df[['cif']].groupby(by=[df.purchase_date__c.dt.year,df.purchase_date__c.dt.month,df.canal_venta]).nunique()
        cli_vta_canal_mes.index.names = ['purchase_year', 'purchase_month', 'canal_venta']
        cli_vta_canal_mes.reset_index(inplace=True)
        return cli_vta_canal_mes.pivot_table('cif',['canal_venta'],['purchase_year','purchase_month'],fill_value=0,margins=True,aggfunc=sum)
    elif canal == 1:
        cli_vta_canal_fam_mes = df[['cif']].groupby(by=[df.purchase_date__c.dt.year,df.purchase_date__c.dt.month,df.canal_venta,df.Family]).nunique()
        cli_vta_canal_fam_mes.index.names = ['purchase_year', 'purchase_month', 'canal_venta','Family']
        cli_vta_canal_fam_mes.reset_index(inplace=True)
        return cli_vta_canal_fam_mes.pivot_table('cif',['canal_venta','Family'],['purchase_year','purchase_month'],fill_value=0)
    else:
        print('Canal: 0 or 1')