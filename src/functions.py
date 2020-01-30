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

def getProcessDate(cur):
    cur.execute(''' select max(process_date) from cartera_xbo''')
    return pd.DataFrame(cur.fetchall()).iloc[0][0]


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
    if re.search('4G', description):
        return '4G'
    elif re.findall('[0-9]{3}|[0-9]{2}|[0-9]{1}', description):
        if len(re.findall('[0-9]{3}|[0-9]{2}|[0-9]{1}', description)[0]) == 1:
            return re.findall('[0-9]{3}|[0-9]{2}|[0-9]{1}', description)[0] + 'Gb'
        elif re.findall('[0-9]{3}|[0-9]{2}|[0-9]{1}', description)[0] == '20':
            return 'Portable 20'
        else:
            return re.findall('[0-9]{3}|[0-9]{2}|[0-9]{1}', description)[0]
    else:
        return ''

def isNeba(description):
    if re.search('FIBER', description.upper()) and (re.search('NEBA', description.upper()) or re.search('IP', description.upper())) :
        return 1
    else:
        return 0

def isMonosede(description):
    if re.search('MONOSEDE', description.upper()) :
        return 1
    else:
        return 0

def getProductName(row):
    if row['Family'] == 'X MUSIC':
        return 'X Music'
    elif row['isMonosede'] ==1:
        return row['Family'].title()+' '+'Monosede '+row['Velocity']
    elif row['Velocity'] != '' and row['isNeba'] == 0 and row['Family'] != 'X FIBER':
        return row['Family'].title()+' '+row['Velocity']
    elif row['Velocity'] != '' and row['isNeba'] == 0 and row['Family'] == 'X FIBER':
        return row['Family'].title()+' '+row['Velocity']+' - '+'own'
    elif row['isNeba'] == 1:
        return row['Family'].title()+' '+row['Velocity']+' - '+'neba'
    elif row['Family'] == 'X UCOM':
        return 'Teamwork+PBX'
    elif row['Family'] == 'X MOBILE':
        return row['product_name'].title()
    else:
        return row['Family'].title()


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

# ----------------------------------- CORRECCIONES MANUALES -------------------

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
    if row['cif'] in ['P1817800D','B71332548']:
        return 'DHO'
    if row['cif'] in ['B88280359']:
        return 'IT integrator'
    else:
        return row['canal_venta']

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
    df['canal_venta'] = df.apply(lambda row: canal(row), axis =1)
    df['Family'] = df.product_name.apply(lambda x: getFamilyFrom(x))
    df['Velocity'] = df.product_name.apply(lambda x: getVelocityFrom(x))
    df['isNeba'] = df.product_name.apply(lambda x: isNeba(x))
    df['isMonosede']= df.product_name.apply(lambda x: isMonosede(x))
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
    df['isMonosede']= df.product_name.apply(lambda x: isMonosede(x))
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

def ass_vta_mes(df,tipo=0):
    idx = pd.period_range('2018-10-01',df.purchase_date__c.max(),freq='M')
    if tipo == 0:
        data = df[['isSold']].groupby(by=[df.purchase_date__c.dt.to_period("M")]).sum()
        data.reset_index(inplace=True)
        data.set_index('purchase_date__c', inplace=True)
        pivot = data.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 1:
        data = df[['isSold']].groupby(by=[df.purchase_date__c.dt.to_period("M"),df.Family]).sum()
        data.reset_index(inplace=True)
        data.set_index('purchase_date__c', inplace=True)
        pivot =  data.pivot_table(index='purchase_date__c', columns='Family', values='isSold',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 2:
        data = df[['isSold']].groupby(by=[df.purchase_date__c.dt.to_period("M"),df.canal_venta]).sum()
        data.reset_index(inplace=True)
        data.set_index('purchase_date__c', inplace=True)
        pivot = data.pivot_table(index='purchase_date__c', columns='canal_venta', values='isSold',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 3:
        data = df[['isSold']].groupby(by=[df.purchase_date__c.dt.to_period("M"),df.canal_venta,df.Family]).sum()
        data.reset_index(inplace=True)
        data.set_index('purchase_date__c', inplace=True)
        pivot = data.pivot_table(index='purchase_date__c', columns=['canal_venta','Family'], values='isSold',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 4:
        data = df[['isSold']].groupby(by=[df.purchase_date__c.dt.to_period("M"),df.Family,df.product_name]).sum()
        data.reset_index(inplace=True)
        data.set_index('purchase_date__c', inplace=True)
        pivot = data.pivot_table(index='purchase_date__c', columns=['Family','product_name'], values='isSold',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 5:
        data = df[['isSold']].groupby(by=[df.purchase_date__c.dt.to_period("M"),df.canal_venta,df.Family,df.product_name]).sum()
        data.reset_index(inplace=True)
        data.set_index('purchase_date__c', inplace=True)
        pivot = data.pivot_table(index='purchase_date__c', columns=['canal_venta','Family','product_name'], values='isSold',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
 
        
# VENTAS CLIENTES

def cli_vta_mes(df,tipo=0):
    idx = pd.period_range('2018-10-01',df.purchase_date__c.max(),freq='M')
    if tipo == 0:
        data = df[['cif']].groupby(by=[df.purchase_date__c.dt.to_period("M")]).nunique()
        data.reset_index(inplace=True)
        data.set_index('purchase_date__c', inplace=True)
        pivot = data.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 1:
        data = df[['cif']].groupby(by=[df.purchase_date__c.dt.to_period("M"),df.Family]).nunique()
        data.reset_index(inplace=True)
        data.set_index('purchase_date__c', inplace=True)
        pivot =  data.pivot_table(index='purchase_date__c', columns='Family', values='cif',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 2:
        data = df[['cif']].groupby(by=[df.purchase_date__c.dt.to_period("M"),df.canal_venta]).nunique()
        data.reset_index(inplace=True)
        data.set_index('purchase_date__c', inplace=True)
        pivot = data.pivot_table(index='purchase_date__c', columns='canal_venta', values='cif',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 3:
        data = df[['cif']].groupby(by=[df.purchase_date__c.dt.to_period("M"),df.canal_venta,df.Family]).nunique()
        data.reset_index(inplace=True)
        data.set_index('purchase_date__c', inplace=True)
        pivot = data.pivot_table(index='purchase_date__c', columns=['canal_venta','Family'], values='cif',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 4:
        data = df[['cif']].groupby(by=[df.purchase_date__c.dt.to_period("M"),df.Family,df.product_name]).nunique()
        data.reset_index(inplace=True)
        data.set_index('purchase_date__c', inplace=True)
        pivot = data.pivot_table(index='purchase_date__c', columns=['Family','product_name'], values='cif',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 5:
        data = df[['cif']].groupby(by=[df.purchase_date__c.dt.to_period("M"),df.canal_venta,df.Family,df.product_name]).nunique()
        data.reset_index(inplace=True)
        data.set_index('purchase_date__c', inplace=True)
        pivot = data.pivot_table(index='purchase_date__c', columns=['canal_venta','Family','product_name'], values='cif',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot

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

def cli_altas_mes_agg(df_altas,tipo=0):
    idx = pd.period_range('2018-10-01',df_altas.rfb.max(),freq='M')
    if tipo == 0:
        data = df_altas[['isClient']].groupby(by=[df_altas.rfb.dt.to_period("M")]).sum()
        data.reset_index(inplace=True)
        data.set_index('rfb', inplace=True)
        pivot = data.reindex(idx).fillna(0).T
        return pivot
    if tipo == 1:
        data = df_altas[['isClient']].groupby(by=[df_altas.rfb.dt.to_period("M"),df_altas.canal_venta]).sum()
        data.reset_index(inplace=True)
        pivot =  data.pivot_table(index='rfb', columns='canal_venta', values='isClient',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
def cli_bajas_mes_agg(df_altas,tipo=0):
    idx = pd.period_range('2018-10-01',df_altas.rfb.max(),freq='M')
    if tipo == 0:
        data = df_altas[['isBaja']].groupby(by=[df_altas.deactivation_date.dt.to_period("M")]).sum()
        data.reset_index(inplace=True)
        data.set_index('deactivation_date', inplace=True)
        pivot = data.reindex(idx).fillna(0).T.astype('int64')
        return pivot
    if tipo == 1:
        data = df_altas[['isBaja']].groupby(by=[df_altas.deactivation_date.dt.to_period("M"),df_altas.canal_venta]).sum()
        data.reset_index(inplace=True)
        pivot = data.pivot_table(index='deactivation_date', columns='canal_venta', values='isBaja',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T.astype('int64')
        return pivot

def cli_altas_mes(df,tipo=3):
    df2 = df[df['isAsset']==1]
    idx = pd.period_range('2018-10-01',df2.purchase_date__c.max(),freq='M')
    # if tipo == 0:
    #     data = df2[['cif']].groupby(by=[df2.rfb__c.dt.to_period("M")]).nunique()
    #     data.reset_index(inplace=True)
    #     data.set_index('rfb__c', inplace=True)
    #     pivot = data.reindex(idx).fillna(0).T
    #     return pivot
    # elif tipo == 1:
    #     data = df2[['cif']].groupby(by=[df2.rfb__c.dt.to_period("M"),df2.Family]).nunique()
    #     data.reset_index(inplace=True)
    #     data.set_index('rfb__c', inplace=True)
    #     pivot =  data.pivot_table(index='rfb__c', columns='Family', values='cif',fill_value=0)
    #     pivot = pivot.reindex(idx).fillna(0).T
    #     return pivot
    # elif tipo == 2:
    #     data = df2[['cif']].groupby(by=[df2.rfb__c.dt.to_period("M"),df2.canal_venta]).nunique()
    #     data.reset_index(inplace=True)
    #     data.set_index('rfb__c', inplace=True)
    #     pivot = data.pivot_table(index='rfb__c', columns='canal_venta', values='cif',fill_value=0)
    #     pivot = pivot.reindex(idx).fillna(0).T
    #     return pivot
    if tipo == 3:
        data = df2[['cif']].groupby(by=[df2.rfb__c.dt.to_period("M"),df2.canal_venta,df2.Family]).nunique()
        data.reset_index(inplace=True)
        data.set_index('rfb__c', inplace=True)
        pivot = data.pivot_table(index='rfb__c', columns=['canal_venta','Family'], values='cif',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    # elif tipo == 4:
    #     data = df2[['cif']].groupby(by=[df2.rfb__c.dt.to_period("M"),df2.Family,df2.product_name]).nunique()
    #     data.reset_index(inplace=True)
    #     data.set_index('rfb__c', inplace=True)
    #     pivot = data.pivot_table(index='rfb__c', columns=['Family','product_name'], values='cif',fill_value=0)
    #     pivot = pivot.reindex(idx).fillna(0).T
    #     return pivot
    # elif tipo == 5:
    #     data = df2[['cif']].groupby(by=[df2.rfb__c.dt.to_period("M"),df2.canal_venta,df2.Family,df2.product_name]).nunique()
    #     data.reset_index(inplace=True)
    #     data.set_index('rfb__c', inplace=True)
    #     pivot = data.pivot_table(index='rfb__c', columns=['canal_venta','Family','product_name'], values='cif',fill_value=0)
    #     pivot = pivot.reindex(idx).fillna(0).T
        # return pivot

def cli_bajas_mes(df,tipo=3):
    df2 = df[df['isBaja']==1]
    idx = pd.period_range('2018-10-01',df2.purchase_date__c.max(),freq='M')
    # if tipo == 0:
    #     data = df2[['cif']].groupby(by=[df2.deactivation_date.dt.to_period("M")]).nunique()
    #     data.reset_index(inplace=True)
    #     data.set_index('deactivation_date', inplace=True)
    #     pivot = data.reindex(idx).fillna(0).T.astype('int64')
    #     return pivot
    # elif tipo == 1:
    #     data = df2[['cif']].groupby(by=[df2.deactivation_date.dt.to_period("M"),df2.Family]).nunique()
    #     data.reset_index(inplace=True)
    #     data.set_index('deactivation_date', inplace=True)
    #     pivot =  data.pivot_table(index='deactivation_date', columns='Family', values='cif',fill_value=0)
    #     pivot = pivot.reindex(idx).fillna(0).T.astype('int64')
    #     return pivot
    # elif tipo == 2:
    #     data = df2[['cif']].groupby(by=[df2.deactivation_date.dt.to_period("M"),df2.canal_venta]).nunique()
    #     data.reset_index(inplace=True)
    #     data.set_index('deactivation_date', inplace=True)
    #     pivot = data.pivot_table(index='deactivation_date', columns='canal_venta', values='cif',fill_value=0)
    #     pivot = pivot.reindex(idx).fillna(0).T
    #     return pivot
    if tipo == 3:
        data = df2[['cif']].groupby(by=[df2.deactivation_date.dt.to_period("M"),df2.canal_venta,df2.Family]).nunique()
        data.reset_index(inplace=True)
        data.set_index('deactivation_date', inplace=True)
        pivot = data.pivot_table(index='deactivation_date', columns=['canal_venta','Family'], values='cif',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T.astype('int64')
        return pivot
    # elif tipo == 4:
    #     data = df2[['cif']].groupby(by=[df2.deactivation_date.dt.to_period("M"),df2.Family,df2.product_name]).nunique()
    #     data.reset_index(inplace=True)
    #     data.set_index('deactivation_date', inplace=True)
    #     pivot = data.pivot_table(index='deactivation_date', columns=['Family','product_name'], values='cif',fill_value=0)
    #     pivot = pivot.reindex(idx).fillna(0).T.astype('int64')
    #     return pivot
    # elif tipo == 5:
    #     data = df2[['cif']].groupby(by=[df2.deactivation_date.dt.to_period("M"),df2.canal_venta,df2.Family,df2.product_name]).nunique()
    #     data.reset_index(inplace=True)
    #     data.set_index('deactivation_date', inplace=True)
    #     pivot = data.pivot_table(index='deactivation_date', columns=['canal_venta','Family','product_name'], values='cif',fill_value=0)
    #     pivot = pivot.reindex(idx).fillna(0).T.astype('int64')
    #     return pivot


# ALTAS/BAJAS PRODUCTOS ---------------------------------------

def ass_altas_mes(df,tipo=0):
    idx = pd.period_range('2018-10-01',df.purchase_date__c.max(),freq='M')
    if tipo == 0:
        data = df[['isAsset']].groupby(by=[df.rfb__c.dt.to_period("M")]).sum()
        data.reset_index(inplace=True)
        data.set_index('rfb__c', inplace=True)
        pivot = data.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 1:
        data = df[['isAsset']].groupby(by=[df.rfb__c.dt.to_period("M"),df.Family]).sum()
        data.reset_index(inplace=True)
        data.set_index('rfb__c', inplace=True)
        pivot =  data.pivot_table(index='rfb__c', columns='Family', values='isAsset',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
#     elif tipo == 2:
#         data = df[['isAsset']].groupby(by=[df.rfb__c.dt.to_period("M"),df.canal_venta]).sum()
#         data.reset_index(inplace=True)
#         data.set_index('rfb__c', inplace=True)
#         pivot = data.pivot_table(index='rfb__c', columns='canal_venta', values='isAsset',fill_value=0)
#         pivot = pivot.reindex(idx).fillna(0).T
#         return pivot
    elif tipo == 3:
        data = df[['isAsset']].groupby(by=[df.rfb__c.dt.to_period("M"),df.canal_venta,df.Family]).sum()
        data.reset_index(inplace=True)
        data.set_index('rfb__c', inplace=True)
        pivot = data.pivot_table(index='rfb__c', columns=['canal_venta','Family'], values='isAsset',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 4:
        data = df[['isAsset']].groupby(by=[df.rfb__c.dt.to_period("M"),df.Family,df.product_name]).sum()
        data.reset_index(inplace=True)
        data.set_index('rfb__c', inplace=True)
        pivot = data.pivot_table(index='rfb__c', columns=['Family','product_name'], values='isAsset',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 5:
        data = df[['isAsset']].groupby(by=[df.rfb__c.dt.to_period("M"),df.canal_venta,df.Family,df.product_name]).sum()
        data.reset_index(inplace=True)
        data.set_index('rfb__c', inplace=True)
        pivot = data.pivot_table(index='rfb__c', columns=['canal_venta','Family','product_name'], values='isAsset',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot

def ass_bajas_mes(df,tipo=0):
    idx = pd.period_range('2018-10-01',df.purchase_date__c.max(),freq='M')
    if tipo == 0:
        data = df[['isBaja']].groupby(by=[df.deactivation_date.dt.to_period("M")]).sum()
        data.reset_index(inplace=True)
        data.set_index('deactivation_date', inplace=True)
        pivot = data.reindex(idx).fillna(0).T.astype('int64')
        return pivot
    elif tipo == 1:
        data = df[['isBaja']].groupby(by=[df.deactivation_date.dt.to_period("M"),df.Family]).sum()
        data.reset_index(inplace=True)
        data.set_index('deactivation_date', inplace=True)
        pivot =  data.pivot_table(index='deactivation_date', columns='Family', values='isBaja',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T.astype('int64')
        return pivot
#     elif tipo == 2:
#         data = df[['isBaja']].groupby(by=[df.deactivation_date.dt.to_period("M"),df.canal_venta]).sum()
#         data.reset_index(inplace=True)
#         data.set_index('deactivation_date', inplace=True)
#         pivot = data.pivot_table(index='deactivation_date', columns='canal_venta', values='isBaja',fill_value=0)
#         pivot = pivot.reindex(idx).fillna(0).T
#         return pivot
    elif tipo == 3:
        data = df[['isBaja']].groupby(by=[df.deactivation_date.dt.to_period("M"),df.canal_venta,df.Family]).sum()
        data.reset_index(inplace=True)
        data.set_index('deactivation_date', inplace=True)
        pivot = data.pivot_table(index='deactivation_date', columns=['canal_venta','Family'], values='isBaja',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T.astype('int64')
        return pivot
    elif tipo == 4:
        data = df[['isBaja']].groupby(by=[df.deactivation_date.dt.to_period("M"),df.Family,df.product_name]).sum()
        data.reset_index(inplace=True)
        data.set_index('deactivation_date', inplace=True)
        pivot = data.pivot_table(index='deactivation_date', columns=['Family','product_name'], values='isBaja',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T.astype('int64')
        return pivot
    elif tipo == 5:
        data = df[['isBaja']].groupby(by=[df.deactivation_date.dt.to_period("M"),df.canal_venta,df.Family,df.product_name]).sum()
        data.reset_index(inplace=True)
        data.set_index('deactivation_date', inplace=True)
        pivot = data.pivot_table(index='deactivation_date', columns=['canal_venta','Family','product_name'], values='isBaja',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T.astype('int64')
        return pivot


# -------------- MIGRAS --------------

def migBase(cur):
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
    df['isMonosede']= df.product_name.apply(lambda x: isMonosede(x))
    df['product_name'] = df.apply(lambda row: getProductName(row), axis = 1)
    df['tipo_migra'] = df.segmento.apply(lambda x: 'IN' if x == '3-NOTRIAL' else 'OUT')
    df['date_migration'] = df.apply(lambda row: getMigrationDate(row), axis=1 )
    cols = ['assetid','assethijo_mig','product_name', 'Family' ,'cif', 'canal_venta',
            'date_migration','tipo_migra']
    df = df[cols]
    df.rename(columns={'date_migration':'mig_date'}, inplace = True)
    return df


def mig_producto_mes(mig_df):
    idx = pd.period_range('2018-10-01',mig_df.mig_date.max(),freq='M')
    group = mig_df[['assetid']].groupby(by=[mig_df.tipo_migra,mig_df.mig_date.dt.to_period("M"),mig_df.canal_venta,mig_df.Family,mig_df.product_name]).count()
    group.reset_index(inplace=True)
    group.set_index('mig_date', inplace=True)
    migin = group[group['tipo_migra']=='IN']
    pivot_migin = migin.pivot_table(index='mig_date', columns=['canal_venta','Family','product_name'], values='assetid')
    pivot_migin = pivot_migin.reindex(idx).fillna(0).T.astype('int64')
    migout = group[group['tipo_migra']=='OUT']
    pivot_migout = migout.pivot_table(index='mig_date', columns=['canal_venta','Family','product_name'], values='assetid')
    pivot_migout = pivot_migout.reindex(idx).fillna(0).T.astype('int64')
    return pivot_migin,pivot_migout