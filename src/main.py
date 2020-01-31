import functions
# import argparse

# def recibeConfig():
#     parser = argparse.ArgumentParser(description='Cartera process')
#     parser.add_argument('--detalle')



def main():
    cur = functions.connectToPs()
    df = functions.base(cur)
    df_altas = functions.base_altas(df)
    mig_df=functions.migBase(cur)
    detalle = functions.detalle(cur)

    print('\n')
    print('Latest PROCESS DATE: ',(functions.getProcessDate(cur)))
    print('\n')
    print('Filas Cartera:', len(df))
    print('Total ventas:', len(df[df['isSold']==1]))
    print('Total cancelaciones:', len(df[df['isCancelled']==1]))
    print('Total assets:', len(df[df['isAsset']==1]))
    print('Total bajas:', len(df[df['isBaja']==1]))
    print('Total assets activos:', len(df[df['isActive']==1]))
    print('Total clientes:', len(df_altas))
    print('Total clientes activos:', len(df_altas[df_altas['isActive']==1]))

    print('\n')
    # VENTAS
    ventas_tabla = functions.ventas(df)
    ventas_tabla.to_csv('../output/ventas_tabla.csv ',encoding='CP1252')
    print('Tabla ventas ........ OK')

    # ALTAS
    altas_tabla = functions.altas(df,df_altas)
    altas_tabla.to_csv('../output/altas_tabla.csv ',encoding='CP1252')
    print('Tabla altas ........ OK')

    # BAJAS
    bajas_tabla = functions.bajas(df,df_altas)
    bajas_tabla.to_csv('../output/bajas_tabla.csv ',encoding='CP1252')
    print('Tabla bajas ........ OK')
    # MIGRAS

    migin, migout = functions.mig_producto_mes(mig_df)
    migin.reset_index(inplace=True)
    migin['index'] = migin['canal_venta']+'_'+migin['product_name']
    migin.drop(columns=['canal_venta','Family','product_name'],inplace=True)
    migin.set_index(['index'],inplace=True)
    migin.to_csv('../output/migin.csv ',encoding='CP1252')
    print('Migras in mensuales ........ OK')
    migout.reset_index(inplace=True)
    migout['index'] = migout['canal_venta']+'_'+migout['product_name']
    migout.drop(columns=['canal_venta','Family','product_name'],inplace=True)
    migout.set_index(['index'],inplace=True)
    migout.to_csv('../output/migout.csv ',encoding='CP1252')
    print('Migras out mensuales ........ OK')

    # DETALLE CARTERA
    detalle.to_csv('../output/detalle_cartera.csv',encoding='CP1252',index=False)
    print('Detalle cartera ........ OK')





if __name__=="__main__":
    main()