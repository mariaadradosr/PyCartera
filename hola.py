def ass_bajas_mes(df,tipo=0):
    idx = pd.period_range('2018-10-01',df.purchase_date__c.max(),freq='M')
    if tipo == 0:
        data = df[['isBaja']].groupby(by=[df.deactivation_date.dt.to_period("M")]).sum()
        data.reset_index(inplace=True)
        data.set_index('deactivation_date', inplace=True)
        pivot = data.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 1:
        data = df[['isBaja']].groupby(by=[df.deactivation_date.dt.to_period("M"),df.Family]).sum()
        data.reset_index(inplace=True)
        data.set_index('deactivation_date', inplace=True)
        pivot =  data.pivot_table(index='deactivation_date', columns='Family', values='isBaja',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
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
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 4:
        data = df[['isBaja']].groupby(by=[df.deactivation_date.dt.to_period("M"),df.Family,df.product_name]).sum()
        data.reset_index(inplace=True)
        data.set_index('deactivation_date', inplace=True)
        pivot = data.pivot_table(index='deactivation_date', columns=['Family','product_name'], values='isBaja',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot
    elif tipo == 5:
        data = df[['isBaja']].groupby(by=[df.deactivation_date.dt.to_period("M"),df.canal_venta,df.Family,df.product_name]).sum()
        data.reset_index(inplace=True)
        data.set_index('deactivation_date', inplace=True)
        pivot = data.pivot_table(index='deactivation_date', columns=['canal_venta','Family','product_name'], values='isBaja',fill_value=0)
        pivot = pivot.reindex(idx).fillna(0).T
        return pivot