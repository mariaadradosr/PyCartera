# def ventas(cur):
#     cur.execute('''
#                 select segmento, assetid, product_name, purchase_date, canal_venta, cif
#                 from cartera_xbo
#                 where process_date = (
#                 select max(process_date) from cartera_xbo)
#                 and segmento in ('3-NOTRIAL', '4-MIGRADO')
#                 and upper(product_name) not like '%CENTRALITA%'
#                 and assetpadre_mig isnull
#                 ''')
#     df = pd.DataFrame(cur.fetchall())
#     col_names = []
#     for e in range(len(cur.description)):
#         col_names.append(cur.description[e][0])
#     df.rename(columns = dict(zip(list(range(33)), col_names)), inplace = True)
#     df['purchase_month'] = df.purchase_date.apply(lambda x: x.month)
#     df['purchase_year'] = df.purchase_date.apply(lambda x: x.year)
#     df['Family'] = df.product_name.apply(lambda x: getFamilyFrom(x))
#     df['Velocity'] = df.product_name.apply(lambda x: getVelocityFrom(x))
#     df['isNeba'] = df.product_name.apply(lambda x: isNeba(x))
#     df['Total'] = 1
#     return df


# def altas(cur):
#     cur.execute('''
#                 select segmento, assetid, product_name, rfb_date, canal_venta, cif
#                 from cartera_xbo
#                 where process_date = (
#                 select max(process_date) from cartera_xbo)
#                 and segmento in ('3-NOTRIAL', '4-MIGRADO')
#                 and upper(product_name) not like '%CENTRALITA%'
#                 and assetpadre_mig isnull
#                 ''')
#     df = pd.DataFrame(cur.fetchall())
#     col_names = []
#     for e in range(len(cur.description)):
#         col_names.append(cur.description[e][0])
#     df.rename(columns = dict(zip(list(range(33)), col_names)), inplace = True)
#     df['alta_month'] = df.rfb_date.apply(lambda x: x.month)
#     df['alta_year'] = df.rfb_date.apply(lambda x: x.year)
#     df['Family'] = df.product_name.apply(lambda x: getFamilyFrom(x))
#     df['Velocity'] = df.product_name.apply(lambda x: getVelocityFrom(x))
#     df['isNeba'] = df.product_name.apply(lambda x: isNeba(x))
#     df['Total'] = 1
#     df = df[np.isfinite(df['alta_month'])]
#     df = df.astype({'alta_month': 'int64', 'alta_year':'int64'})
#     return df   


# def altas_cif(altas,tipo):
#     if tipo == 'Family':
#         df = altas[['Family','cif','rfb_date']]
#         df = df.groupby(['Family','cif'])['rfb_date'].min().to_frame().reset_index()
#         df['alta_month'] = df.rfb_date.apply(lambda x: x.month)
#         df['alta_year'] = df.rfb_date.apply(lambda x: x.year)
#         df.drop('rfb_date', axis=1, inplace=True)
#         df = df.groupby(by=['Family','alta_year','alta_month']).count().reset_index()
#         return df
#     if tipo == 'Total':
#         df = altas[['cif','rfb_date']]
#         df = df.groupby(['cif'])['rfb_date'].min().to_frame().reset_index()
#         df['alta_month'] = df.rfb_date.apply(lambda x: x.month)
#         df['alta_year'] = df.rfb_date.apply(lambda x: x.year)
#         df.drop('rfb_date', axis=1, inplace=True)
#         df = df.groupby(by=['alta_year','alta_month']).count().reset_index()
#         return df

# def bajas(cur):
#     cur.execute('''
#             select segmento, assetid, product_name, migrado,rfb_date, rfb_migration, deactivation_date, canal_venta, cif
#             from cartera_xbo
#             where process_date = (
#             select max(process_date) from cartera_xbo)
#             and segmento in ('3-NOTRIAL')
#             and upper(product_name) not like '%CENTRALITA%'
#             and rfb_date notnull
#             ''')
#     df = pd.DataFrame(cur.fetchall())
#     col_names = []
#     for e in range(len(cur.description)):
#         col_names.append(cur.description[e][0])
#     df.rename(columns = dict(zip(list(range(33)), col_names)), inplace = True)
#     df['rfb__c'] = df.apply(lambda row: rfb(row), axis = 1)
#     df.drop(columns=['rfb_date','rfb_migration'], axis=1, inplace=True)
#     df['Family'] = df.product_name.apply(lambda x: getFamilyFrom(x))
#     df['Velocity'] = df.product_name.apply(lambda x: getVelocityFrom(x))
#     df['isNeba'] = df.product_name.apply(lambda x: isNeba(x))
#     df['Total'] = 1
#     df['Bajas__c'] = df.apply(lambda row: baja__c(row), axis = 1)
#     df['assets_act'] = df['Total'] + df['Bajas__c']
#     return df

# def altas_bajas(df):
#     assets = df.Total.groupby(by=[df.cif,df.canal_venta]).sum().reset_index()
#     assets_act = df.assets_act.groupby(by=[df.cif,df.canal_venta]).sum().reset_index()
#     altas = df.rfb__c.groupby(by=[df.cif,df.canal_venta]).min().reset_index()
#     bajas = df.deactivation_date.groupby(by=[df.cif,df.canal_venta]).max().reset_index()
#     df_altas = pd.merge(pd.merge(pd.merge(assets_act,altas,on=['cif','canal_venta']),bajas,on=['cif','canal_venta']),assets,on=['cif','canal_venta'])
#     df_altas['isActive'] = df_altas.assets_act.apply(lambda x: 1 if x>=1 else 0)
#     df_altas['deactivation_date'] = df_altas.apply(lambda row: deactivation(row), axis = 1)
#     df_altas['assets_dea'] = df_altas['Total'] - df_altas['assets_act']
#     df_altas['isBaja'] = -(df_altas['isActive']-1)
#     df_altas['Client'] = 1
#     df_altas.rename(columns={'Total':'assets_tot',
#                       'rfb__c':'rfb',
#                       }, inplace = True)
#     return df_altas

# def altas_bajas_mes(altas_bajas_df):
#     altas = altas_bajas_df[['Client']].groupby(by=[altas_bajas_df.rfb]).sum()
#     bajas = altas_bajas_df[['isBaja']].groupby(by=[altas_bajas_df.deactivation_date]).sum()
#     bajas.index.name = 'rfb'
#     altas_bajas = altas.join(bajas, how='outer').fillna(0.0)
#     mes = altas_bajas.groupby(by=[altas_bajas.index.year, altas_bajas.index.month]).sum()
#     mes.index.names = ['year', 'month']
#     mes.reset_index(inplace=True)
#     return mes

# def altas_bajas_mes_canal(altas_bajas_df):
#     altas = altas_bajas_df[['Client']].groupby(by=[altas_bajas_df.rfb, altas_bajas_df.canal_venta]).sum()
#     bajas = altas_bajas_df[['isBaja']].groupby(by=[altas_bajas_df.deactivation_date,altas_bajas_df.canal_venta]).sum()
#     bajas.index.names = ['rfb','canal_venta']
#     altas_bajas = altas.join(bajas, how='outer').fillna(0.0)
#     mes_canal = altas_bajas.groupby(by=[altas_bajas.index.get_level_values('rfb').year, altas_bajas.index.get_level_values('rfb').month, altas_bajas.index.get_level_values('canal_venta')]).sum()
#     mes_canal.index.names = ['year', 'month', 'canal']
#     mes_canal.reset_index(inplace=True)
#     return mes_canal


# def ventas_2(cur):
#     cur.execute('''
#             select segmento, assetid, cif, canal_venta, product_name, 
#                     migrado,rfb_date, rfb_migration, deactivation_date,
#                     purchase_date, purchase_migration, deactivation_request_date
#             from cartera_xbo
#             where process_date = (
#             select max(process_date) from cartera_xbo)
#             and segmento in ('3-NOTRIAL')
#             and upper(product_name) not like '%CENTRALITA%'
#             ''')
#     df = pd.DataFrame(cur.fetchall())
#     col_names = []
#     for e in range(len(cur.description)):
#         col_names.append(cur.description[e][0])
#     df.rename(columns = dict(zip(list(range(33)), col_names)), inplace = True)
#     df['rfb__c'] = df.apply(lambda row: rfb(row), axis = 1)
#     df.drop(columns=['rfb_date','rfb_migration'], axis=1, inplace=True)
#     df['purchase_date__c'] = df.apply(lambda row: purchase(row), axis = 1)
#     df.drop(columns=['purchase_migration','purchase_date'], axis=1, inplace=True)
#     df['Family'] = df.product_name.apply(lambda x: getFamilyFrom(x))
#     df['Velocity'] = df.product_name.apply(lambda x: getVelocityFrom(x))
#     df['isNeba'] = df.product_name.apply(lambda x: isNeba(x))
#     df['isSold'] = 1
#     df['isAsset'] =  df.apply(lambda row: purchaseToAsset(row), axis = 1)
#     df['isBaja'] = df.apply(lambda row: baja__c(row), axis = 1)
#     df['isActive'] = df['isAsset'] + df['isBaja']
#     to_drop = df[(df['rfb__c'].isnull())&((df['deactivation_date'].notnull())|(df['deactivation_request_date'].notnull()))].index
#     df.drop(to_drop , inplace=True)
#     df.drop(columns=['deactivation_request_date'], axis=1, inplace=True)
#     return df



# def getTimeline(df):
#     return [date.strftime('%d-%m-%Y') for date in pd.date_range(df.purchase_date__c.min(), df.purchase_date__c.max()+ datetime.timedelta(days=30), freq='M')] 

# VENTA ASSETS

# def ass_vta_fam_mes(df,canal=0):
#     if canal == 1:
#         ass_vta_canal_fam_mes = df[['isSold']].groupby(by=[df.purchase_date__c.dt.year,df.purchase_date__c.dt.month,df.canal_venta,df.Family]).sum()
#         ass_vta_canal_fam_mes.index.names = ['purchase_year', 'purchase_month', 'canal_venta','Family']
#         ass_vta_canal_fam_mes.reset_index(inplace=True)
#         return ass_vta_canal_fam_mes.pivot_table('isSold',['canal_venta','Family'],['purchase_year','purchase_month'],fill_value=0,margins=True,aggfunc=sum)
#     elif canal == 0:
#         ass_vta_fam_mes = df[['isSold']].groupby(by=[df.purchase_date__c.dt.year,df.purchase_date__c.dt.month,df.Family]).sum()
#         ass_vta_fam_mes.index.names = ['purchase_year', 'purchase_month','Family']
#         ass_vta_fam_mes.reset_index(inplace=True)
#         return ass_vta_fam_mes.pivot_table('isSold','Family',['purchase_year','purchase_month'],fill_value=0,margins=True,aggfunc=sum)
#     else:
#         print('Canal: 0 or 1')



# def ass_vta_prod_mes(df,canal=0):
#     if canal == 1:
#         ass_vta_canal_prod_mes = df[['isSold']].groupby(by=[df.purchase_date__c.dt.year,df.purchase_date__c.dt.month,df.canal_venta,df.Family,df.product_name]).sum()
#         ass_vta_canal_prod_mes.index.names = ['purchase_year', 'purchase_month','canal_venta','Family','product_name']
#         ass_vta_canal_prod_mes.reset_index(inplace=True)
#         return ass_vta_canal_prod_mes.pivot_table('isSold',['canal_venta','Family','product_name'],['purchase_year','purchase_month'],fill_value=0,margins=True,aggfunc=sum)
#     elif canal == 0:
#         ass_vta_canal_prod_mes = df[['isSold']].groupby(by=[df.purchase_date__c.dt.year,df.purchase_date__c.dt.month,df.Family,df.product_name]).sum()
#         ass_vta_canal_prod_mes.index.names = ['purchase_year', 'purchase_month','Family','product_name']
#         ass_vta_canal_prod_mes.reset_index(inplace=True)
#         return ass_vta_canal_prod_mes.pivot_table('isSold',['Family','product_name'],['purchase_year','purchase_month'],fill_value=0,margins=True,aggfunc=sum)
#     else:
#         print('Canal: 0 or 1')



# VENTAS CLIENTES

# def cli_vta_fam_mes(df,canal=0):
#     if canal == 0:
#         cli_vta_canal_mes = df[['cif']].groupby(by=[df.purchase_date__c.dt.year,df.purchase_date__c.dt.month,df.canal_venta]).nunique()
#         cli_vta_canal_mes.index.names = ['purchase_year', 'purchase_month', 'canal_venta']
#         cli_vta_canal_mes.reset_index(inplace=True)
#         return cli_vta_canal_mes.pivot_table('cif',['canal_venta'],['purchase_year','purchase_month'],fill_value=0,margins=True,aggfunc=sum)
#     elif canal == 1:
#         cli_vta_canal_fam_mes = df[['cif']].groupby(by=[df.purchase_date__c.dt.year,df.purchase_date__c.dt.month,df.canal_venta,df.Family]).nunique()
#         cli_vta_canal_fam_mes.index.names = ['purchase_year', 'purchase_month', 'canal_venta','Family']
#         cli_vta_canal_fam_mes.reset_index(inplace=True)
#         return cli_vta_canal_fam_mes.pivot_table('cif',['canal_venta','Family'],['purchase_year','purchase_month'],fill_value=0)
#     else:
#         print('Canal: 0 or 1')


# ALTAS/BAJAS CLIENTES

# def cli_altas_bajas_mes(df_altas,canal=0):
#     if canal == 0:
#         altas = df_altas[['isClient']].groupby(by=[df_altas.rfb]).sum()
#         bajas = df_altas[['isBaja']].groupby(by=[df_altas.deactivation_date]).sum()
#         bajas.index.name = 'rfb'
#         altas_bajas = altas.join(bajas, how='outer').fillna(0)
#         mes = altas_bajas.groupby(by=[altas_bajas.index.year, altas_bajas.index.month]).sum()
#         mes.index.names = ['year', 'month']
#         return mes.T.astype('int64')
#     elif canal == 1:
#         altas = df_altas[['isClient']].groupby(by=[df_altas.rfb,df_altas.canal_venta]).sum()
#         bajas = df_altas[['isBaja']].groupby(by=[df_altas.deactivation_date, df_altas.canal_venta]).sum()
#         bajas.index.names = ['rfb','canal_venta']
#         altas_bajas = altas.join(bajas, how='outer').fillna(0)
#         mes = altas_bajas.groupby(by=[altas_bajas.index.get_level_values('rfb').year, altas_bajas.index.get_level_values('rfb').month, altas_bajas.index.get_level_values('canal_venta')]).sum()
#         mes.index.names = ['year', 'month', 'canal']
# #         mes.reorder_levels([1,0], axis=0)
#         return mes.unstack().T.fillna(0).astype('int64')

# ALTAS/BAJAS PRODUCTO

# def ass_altas_bajas_fam_mes(df, nivel ='fam',canal = 0):
#     if nivel.upper() == 'FAM':
#         if canal == 1:
#             ass_alta_canal_prod_mes = df[['isAsset']].groupby(by=[df.rfb__c,df.canal_venta,df.Family,df.product_name]).sum()
#             ass_baja_canal_prod_mes = df[['isBaja']].groupby(by=[df.deactivation_date,df.canal_venta,df.Family,df.product_name]).sum()
#             ass_baja_canal_prod_mes.index.names =  ['rfb__c','canal_venta','Family','product_name']
#             altas_bajas = ass_alta_canal_prod_mes.join(ass_baja_canal_prod_mes, how='outer').fillna(0)
#             mes = altas_bajas.groupby(by=[altas_bajas.index.get_level_values('rfb__c').year, altas_bajas.index.get_level_values('rfb__c').month, altas_bajas.index.get_level_values('canal_venta'),altas_bajas.index.get_level_values('Family')]).sum()
#             mes.index.names = ['year', 'month', 'canal_venta', 'Family']
#             isAsset = mes[['isAsset']]
#             isAsset.reset_index(inplace=True)
#             pivot_isAsset = isAsset.pivot_table(index = ['canal_venta','Family'],columns=['year','month'], values='isAsset',fill_value=0,margins=True,aggfunc=sum).astype('int64')
#             isBaja = mes[['isBaja']]
#             isBaja.reset_index(inplace=True)
#             pivot_isBaja = isBaja.pivot_table(index = ['canal_venta','Family'],columns=['year','month'], values='isBaja',fill_value=0,margins=True,aggfunc=sum).astype('int64')
#             return pivot_isAsset, pivot_isBaja
#         elif canal == 0:
#             ass_alta_canal_prod_mes = df[['isAsset']].groupby(by=[df.rfb__c,df.Family,df.product_name]).sum()
#             ass_baja_canal_prod_mes = df[['isBaja']].groupby(by=[df.deactivation_date,df.Family,df.product_name]).sum()
#             ass_baja_canal_prod_mes.index.names =  ['rfb__c','Family','product_name']
#             altas_bajas = ass_alta_canal_prod_mes.join(ass_baja_canal_prod_mes, how='outer').fillna(0)
#             mes = altas_bajas.groupby(by=[altas_bajas.index.get_level_values('rfb__c').year, altas_bajas.index.get_level_values('rfb__c').month,altas_bajas.index.get_level_values('Family')]).sum()
#             mes.index.names = ['year', 'month', 'Family']
#             isAsset = mes[['isAsset']]
#             isAsset.reset_index(inplace=True)
#             pivot_isAsset = isAsset.pivot_table(index ='Family',columns=['year','month'], values='isAsset',fill_value=0,margins=True,aggfunc=sum).astype('int64')
#             isBaja = mes[['isBaja']]
#             isBaja.reset_index(inplace=True)
#             pivot_isBaja = isBaja.pivot_table(index ='Family',columns=['year','month'], values='isBaja',fill_value=0,margins=True,aggfunc=sum).astype('int64')
#             return pivot_isAsset, pivot_isBaja
#     elif nivel.upper() =='PROD':
#         if canal == 1:
#             ass_alta_canal_prod_mes = df[['isAsset']].groupby(by=[df.rfb__c,df.canal_venta,df.Family,df.product_name]).sum()
#             ass_baja_canal_prod_mes = df[['isBaja']].groupby(by=[df.deactivation_date,df.canal_venta,df.Family,df.product_name]).sum()
#             ass_baja_canal_prod_mes.index.names =  ['rfb__c','canal_venta','Family','product_name']
#             altas_bajas = ass_alta_canal_prod_mes.join(ass_baja_canal_prod_mes, how='outer').fillna(0)
#             mes = altas_bajas.groupby(by=[altas_bajas.index.get_level_values('rfb__c').year, altas_bajas.index.get_level_values('rfb__c').month, altas_bajas.index.get_level_values('canal_venta'),altas_bajas.index.get_level_values('Family'),altas_bajas.index.get_level_values('product_name')]).sum()
#             mes.index.names = ['year', 'month', 'canal_venta', 'Family','product_name']
#             isAsset = mes[['isAsset']]
#             isAsset.reset_index(inplace=True)
#             pivot_isAsset = isAsset.pivot_table(index = ['canal_venta','Family','product_name'],columns=['year','month'], values='isAsset',fill_value=0,margins=True,aggfunc=sum).astype('int64')
#             isBaja = mes[['isBaja']]
#             isBaja.reset_index(inplace=True)
#             pivot_isBaja = isBaja.pivot_table(index = ['canal_venta','Family','product_name'],columns=['year','month'], values='isBaja',fill_value=0,margins=True,aggfunc=sum).astype('int64')
#             return pivot_isAsset, pivot_isBaja
#         elif canal == 0:
#             ass_alta_canal_prod_mes = df[['isAsset']].groupby(by=[df.rfb__c,df.Family,df.product_name]).sum()
#             ass_baja_canal_prod_mes = df[['isBaja']].groupby(by=[df.deactivation_date,df.Family,df.product_name]).sum()
#             ass_baja_canal_prod_mes.index.names =  ['rfb__c','Family','product_name']
#             altas_bajas = ass_alta_canal_prod_mes.join(ass_baja_canal_prod_mes, how='outer').fillna(0)
#             mes = altas_bajas.groupby(by=[altas_bajas.index.get_level_values('rfb__c').year, altas_bajas.index.get_level_values('rfb__c').month,altas_bajas.index.get_level_values('Family'),altas_bajas.index.get_level_values('product_name')]).sum()
#             mes.index.names = ['year', 'month', 'Family','product_name']
#             isAsset = mes[['isAsset']]
#             isAsset.reset_index(inplace=True)
#             pivot_isAsset = isAsset.pivot_table(index =['Family','product_name'],columns=['year','month'], values='isAsset',fill_value=0,margins=True,aggfunc=sum).astype('int64')
#             isBaja = mes[['isBaja']]
#             isBaja.reset_index(inplace=True)
#             pivot_isBaja = isBaja.pivot_table(index =['Family','product_name'],columns=['year','month'], values='isBaja',fill_value=0,margins=True,aggfunc=sum).astype('int64')
#             return pivot_isAsset, pivot_isBaja