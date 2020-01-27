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

def getMigrationDate(row):
    return row['date_migration'] if row['tipo_migra'] == 'IN' else row['deactivation_date']

# Correcciones manuales -------------------

def DRDcorrecciones(row):
    return pd.NaT if row['assetid'] in ['02i0N00000KqDCzQAN','02i0N00000I2qZ2QAJ','02i0N00000I2qZ3QAJ'] else row['deactivation_request_date']

def DDcorrecciones(row):
    # return pd.NaT if row['assetid'] in ['02i0N00000L2rIBQAZ','02i0N00000L2rICQAZ','02i0N00000LGrVPQA1','02i0N00000LGrVQQA1'] else row['deactivation_date']
    if row['assetid'] in ['02i0N00000KqDCzQAN','02i0N00000L2rIBQAZ','02i0N00000L2rICQAZ','02i0N00000LGrVPQA1','02i0N00000LGrVQQA1','02i0N00000LJWycQAH','02i0N00000HfjEZQAZ','02i0N00000HfjEYQAZ']:
        return pd.NaT
    else:
        return row['deactivation_date']

def CDcorrecciones(row):
    return row['deactivation_date'] if row['assetid'] in ['02i0N00000KqDCzQAN','02i0N00000LJWycQAH','02i0N00000HfjEZQAZ','02i0N00000HfjEYQAZ'] else row['cancellation_date']


def CanalCorrecciones(row):
    return 'DHO' if row['cif'] == 'P1817800D' else row['canal_venta']

# ----------------------------------- DETALLE --------------------------------

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
    df['canal_venta'] = df.apply(lambda row: CanalCorrecciones(row), axis =1)
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

# prueba.to_excel('./detalle_cartera_20200123_excel_3.xlsx',encoding='CP1252',index=False)


# ----------------------------------- BASE --------------------------------
def base(cur):
    cur.execute('''
            select segmento, assetid, cif, canal_venta, product_name, 
                    migrado,rfb_date, rfb_migration, deactivation_date, cancellation_date,
                    purchase_date, purchase_migration, deactivation_request_date, date_migration
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
    df['cancellation_date'] = df.apply(lambda row: CDcorrecciones(row), axis = 1)
    df['deactivation_request_date'] = df.apply(lambda row: DRDcorrecciones(row), axis = 1)
    df['deactivation_date'] = df.apply(lambda row: DDcorrecciones(row), axis = 1)
    df['canal_venta'] = df.apply(lambda row: CanalCorrecciones(row), axis =1)
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
    # to_drop = df[(df['rfb__c'].isnull())&((df['deactivation_date'].notnull())|(df['deactivation_request_date'].notnull()))].index
    # df.drop(to_drop , inplace=True)
    # df.drop(columns=['deactivation_request_date'], axis=1, inplace=True)
    cols = ['assetid', 'product_name', 'Family' ,'cif', 'canal_venta',
            'purchase_date__c','cancellation_date','rfb__c', 'deactivation_date',
            'date_migration','migrado', 
            'isSold', 'isCancelled','isAsset', 'isActive','isBaja']
    df = df[cols]
    df.rename(columns={'date_migration':'mig_in_date'}, inplace = True)
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

# -------------- ALTAS & BAJAS --------------

def base_altas(df):
    df = df[df['rfb__c'].notnull()]  
    assets = df.isAsset.groupby(by=[df.cif,df.canal_venta]).sum().reset_index()
    assets.rename(columns={'isAsset':'assets'}, inplace=True)
    assets_act = df.isActive.groupby(by=[df.cif,df.canal_venta]).sum().reset_index()
    assets_act.rename(columns={'isActive':'assets_act'}, inplace=True)
    altas = df.rfb__c.groupby(by=[df.cif,df.canal_venta]).min().reset_index()
    bajas = df.deactivation_date.groupby(by=[df.cif,df.canal_venta]).max().reset_index()
    df_altas = pd.merge(pd.merge(pd.merge(assets_act,altas,on=['cif','canal_venta']),bajas,on=['cif','canal_venta']),assets,on=['cif','canal_venta'])
    df_altas['isClient'] = 1
    df_altas['isActive'] = df_altas.assets_act.apply(lambda x: 1 if x>=1 else 0)
    df_altas['deactivation_date'] = df_altas.apply(lambda row: deactivation(row), axis = 1)
    df_altas['assets_dea'] = df_altas['assets'] - df_altas['assets_act']
    df_altas['isBaja'] = -(df_altas['isActive']-1)
    df_altas.rename(columns={'assets':'assets_tot',
                      'rfb__c':'rfb',
                      }, inplace = True)
    return df_altas

# ALTAS/BAJAS CLIENTES

def cli_altas_bajas_mes(df_altas,canal=0):
    if canal == 0:
        altas = df_altas[['isClient']].groupby(by=[df_altas.rfb]).sum()
        bajas = df_altas[['isBaja']].groupby(by=[df_altas.deactivation_date]).sum()
        bajas.index.name = 'rfb'
        altas_bajas = altas.join(bajas, how='outer').fillna(0)
        mes = altas_bajas.groupby(by=[altas_bajas.index.year, altas_bajas.index.month]).sum()
        mes.index.names = ['year', 'month']
        return mes.T.astype('int64')
    elif canal == 1:
        altas = df_altas[['isClient']].groupby(by=[df_altas.rfb,df_altas.canal_venta]).sum()
        bajas = df_altas[['isBaja']].groupby(by=[df_altas.deactivation_date, df_altas.canal_venta]).sum()
        bajas.index.names = ['rfb','canal_venta']
        altas_bajas = altas.join(bajas, how='outer').fillna(0)
        mes = altas_bajas.groupby(by=[altas_bajas.index.get_level_values('rfb').year, altas_bajas.index.get_level_values('rfb').month, altas_bajas.index.get_level_values('canal_venta')]).sum()
        mes.index.names = ['year', 'month', 'canal']
#         mes.reorder_levels([1,0], axis=0)
        return mes.unstack().T.fillna(0).astype('int64')

# ALTAS/BAJAS PRODUCTO

def ass_altas_bajas_fam_mes(df, nivel ='fam',canal = 0):
    if nivel.upper() == 'FAM':
        if canal == 1:
            ass_alta_canal_prod_mes = df[['isAsset']].groupby(by=[df.rfb__c,df.canal_venta,df.Family,df.product_name]).sum()
            ass_baja_canal_prod_mes = df[['isBaja']].groupby(by=[df.deactivation_date,df.canal_venta,df.Family,df.product_name]).sum()
            ass_baja_canal_prod_mes.index.names =  ['rfb__c','canal_venta','Family','product_name']
            altas_bajas = ass_alta_canal_prod_mes.join(ass_baja_canal_prod_mes, how='outer').fillna(0)
            mes = altas_bajas.groupby(by=[altas_bajas.index.get_level_values('rfb__c').year, altas_bajas.index.get_level_values('rfb__c').month, altas_bajas.index.get_level_values('canal_venta'),altas_bajas.index.get_level_values('Family')]).sum()
            mes.index.names = ['year', 'month', 'canal_venta', 'Family']
            isAsset = mes[['isAsset']]
            isAsset.reset_index(inplace=True)
            pivot_isAsset = isAsset.pivot_table(index = ['canal_venta','Family'],columns=['year','month'], values='isAsset',fill_value=0,margins=True,aggfunc=sum).astype('int64')
            isBaja = mes[['isBaja']]
            isBaja.reset_index(inplace=True)
            pivot_isBaja = isBaja.pivot_table(index = ['canal_venta','Family'],columns=['year','month'], values='isBaja',fill_value=0,margins=True,aggfunc=sum).astype('int64')
            return pivot_isAsset, pivot_isBaja
        elif canal == 0:
            ass_alta_canal_prod_mes = df[['isAsset']].groupby(by=[df.rfb__c,df.Family,df.product_name]).sum()
            ass_baja_canal_prod_mes = df[['isBaja']].groupby(by=[df.deactivation_date,df.Family,df.product_name]).sum()
            ass_baja_canal_prod_mes.index.names =  ['rfb__c','Family','product_name']
            altas_bajas = ass_alta_canal_prod_mes.join(ass_baja_canal_prod_mes, how='outer').fillna(0)
            mes = altas_bajas.groupby(by=[altas_bajas.index.get_level_values('rfb__c').year, altas_bajas.index.get_level_values('rfb__c').month,altas_bajas.index.get_level_values('Family')]).sum()
            mes.index.names = ['year', 'month', 'Family']
            isAsset = mes[['isAsset']]
            isAsset.reset_index(inplace=True)
            pivot_isAsset = isAsset.pivot_table(index ='Family',columns=['year','month'], values='isAsset',fill_value=0,margins=True,aggfunc=sum).astype('int64')
            isBaja = mes[['isBaja']]
            isBaja.reset_index(inplace=True)
            pivot_isBaja = isBaja.pivot_table(index ='Family',columns=['year','month'], values='isBaja',fill_value=0,margins=True,aggfunc=sum).astype('int64')
            return pivot_isAsset, pivot_isBaja
    elif nivel.upper() =='PROD':
        if canal == 1:
            ass_alta_canal_prod_mes = df[['isAsset']].groupby(by=[df.rfb__c,df.canal_venta,df.Family,df.product_name]).sum()
            ass_baja_canal_prod_mes = df[['isBaja']].groupby(by=[df.deactivation_date,df.canal_venta,df.Family,df.product_name]).sum()
            ass_baja_canal_prod_mes.index.names =  ['rfb__c','canal_venta','Family','product_name']
            altas_bajas = ass_alta_canal_prod_mes.join(ass_baja_canal_prod_mes, how='outer').fillna(0)
            mes = altas_bajas.groupby(by=[altas_bajas.index.get_level_values('rfb__c').year, altas_bajas.index.get_level_values('rfb__c').month, altas_bajas.index.get_level_values('canal_venta'),altas_bajas.index.get_level_values('Family'),altas_bajas.index.get_level_values('product_name')]).sum()
            mes.index.names = ['year', 'month', 'canal_venta', 'Family','product_name']
            isAsset = mes[['isAsset']]
            isAsset.reset_index(inplace=True)
            pivot_isAsset = isAsset.pivot_table(index = ['canal_venta','Family','product_name'],columns=['year','month'], values='isAsset',fill_value=0,margins=True,aggfunc=sum).astype('int64')
            isBaja = mes[['isBaja']]
            isBaja.reset_index(inplace=True)
            pivot_isBaja = isBaja.pivot_table(index = ['canal_venta','Family','product_name'],columns=['year','month'], values='isBaja',fill_value=0,margins=True,aggfunc=sum).astype('int64')
            return pivot_isAsset, pivot_isBaja
        elif canal == 0:
            ass_alta_canal_prod_mes = df[['isAsset']].groupby(by=[df.rfb__c,df.Family,df.product_name]).sum()
            ass_baja_canal_prod_mes = df[['isBaja']].groupby(by=[df.deactivation_date,df.Family,df.product_name]).sum()
            ass_baja_canal_prod_mes.index.names =  ['rfb__c','Family','product_name']
            altas_bajas = ass_alta_canal_prod_mes.join(ass_baja_canal_prod_mes, how='outer').fillna(0)
            mes = altas_bajas.groupby(by=[altas_bajas.index.get_level_values('rfb__c').year, altas_bajas.index.get_level_values('rfb__c').month,altas_bajas.index.get_level_values('Family'),altas_bajas.index.get_level_values('product_name')]).sum()
            mes.index.names = ['year', 'month', 'Family','product_name']
            isAsset = mes[['isAsset']]
            isAsset.reset_index(inplace=True)
            pivot_isAsset = isAsset.pivot_table(index =['Family','product_name'],columns=['year','month'], values='isAsset',fill_value=0,margins=True,aggfunc=sum).astype('int64')
            isBaja = mes[['isBaja']]
            isBaja.reset_index(inplace=True)
            pivot_isBaja = isBaja.pivot_table(index =['Family','product_name'],columns=['year','month'], values='isBaja',fill_value=0,margins=True,aggfunc=sum).astype('int64')
            return pivot_isAsset, pivot_isBaja


# -------------- MIGRAS --------------

def migOut(cur):
    cur.execute('''
            select segmento, assetid, product_name,assethijo_mig, cif, canal_venta,deactivation_date,
            date_migration
            from cartera_xbo
            where process_date = (
            select max(process_date) from cartera_xbo)
            and segmento in ('4-MIGRADO','3-NOTRIAL')
            and migrado = '1'
            and upper(product_name) not like '%CENTRALITA%'
            ''')
    df = pd.DataFrame(cur.fetchall())
    col_names = []
    for e in range(len(cur.description)):
        col_names.append(cur.description[e][0])
    df.rename(columns = dict(zip(list(range(33)), col_names)), inplace = True)
    df['canal_venta'] = df.apply(lambda row: CanalCorrecciones(row), axis =1)
    df['canal_venta'] = df.canal_venta.apply(lambda x: canal(x))
    df['Family'] = df.product_name.apply(lambda x: getFamilyFrom(x))
    df['Velocity'] = df.product_name.apply(lambda x: getVelocityFrom(x))
    df['isNeba'] = df.product_name.apply(lambda x: isNeba(x))
    df['product_name'] = df.apply(lambda row: getProductName(row), axis = 1)
    df['tipo_migra'] = df.segmento.apply(lambda x: 'IN' if x == '3-NOTRIAL' else 'OUT')
    df['date_migration'] = df.apply(lambda row: getMigrationDate(row), axis=1 )
    cols = ['assetid','assethijo_mig','product_name', 'Family' ,'cif', 'canal_venta',
            'date_migration','tipo_migra']
    df = df[cols]
    df.rename(columns={'date_migration':'mig_in_date'}, inplace = True)
    return df