import functions

ruta = "../output/"


def main():
    cur = functions.connectToPs()
    df = functions.base(cur)
    df_altas = functions.base_altas(df)
    mig_df = functions.migBase(cur)
    detalle = functions.detalle(cur)

    print('\n')
    print('Latest PROCESS DATE: ', (functions.getProcessDate(cur)))
    print('\n')
    print('Ventas:', len(df[df['isSold'] == 1]))
    print('Cancelaciones:', len(df[df['isCancelled'] == 1]))
    print('\n')
    print('Ventas sin activar:',len(df[df['isSold'] == 1])-len(df[df['isCancelled'] == 1])-len(df[df['isAsset'] == 1]))
    print('\n')
    print('Assets:', len(df[df['isAsset'] == 1]))
    print('Assets activos:', len(df[df['isActive'] == 1]))
    print('Assets bajas:', len(df[df['isBaja'] == 1]))
    print('\n')
    print('Clientes:', len(df_altas))
    print('Clientes activos:', len(df_altas[df_altas['isActive'] == 1]))
    print('Clientes baja:', len(df_altas[df_altas['isActive'] == 0]))
    print('\n')
    print('Ventas mensuales')
    print(functions.ass_vta_mes(df,0))
    print('\n')
    print(functions.ass_vta_mes(df,2))
    print('\n')
    print(functions.ass_vta_mes(df,1))
    print('\n')
    print('Altas cliente mensuales')
    print(functions.cli_altas_mes_agg(df_altas,tipo=0))
    print('\n')
    print('Bajas cliente mensuales')
    print(functions.cli_bajas_mes_agg(df_altas,tipo=0))
    print('\n')
    # VENTAS
    ventas_tabla = functions.ventas(df)
    ventas_tabla.to_csv(f'{ruta}ventas_tabla.csv ', encoding='CP1252')
    print('Tabla ventas ........ OK')

    # ALTAS
    altas_tabla = functions.altas(df, df_altas)
    altas_tabla.to_csv(f'{ruta}altas_tabla.csv ', encoding='CP1252')
    print('Tabla altas ........ OK')

    # BAJAS
    bajas_tabla = functions.bajas(df, df_altas)
    bajas_tabla.to_csv(f'{ruta}bajas_tabla.csv ', encoding='CP1252')
    print('Tabla bajas ........ OK')
    # MIGRAS

    migin, migout = functions.mig_producto_mes(mig_df, df)
    migin.reset_index(inplace=True)
    migin['index'] = migin['canal_venta']+'_'+migin['product_name']
    migin.drop(columns=['canal_venta', 'Family', 'product_name'], inplace=True)
    migin.set_index(['index'], inplace=True)
    migin.to_csv('../output/migin.csv ', encoding='CP1252')
    print('Migras in mensuales ........ OK')
    migout.reset_index(inplace=True)
    migout['index'] = migout['canal_venta']+'_'+migout['product_name']
    migout.drop(columns=['canal_venta', 'Family',
                         'product_name'], inplace=True)
    migout.set_index(['index'], inplace=True)
    migout.to_csv(f'{ruta}migout.csv ', encoding='CP1252')
    print('Migras out mensuales ........ OK')

    # DETALLE CARTERA
    detalle.to_csv(f'{ruta}detalle_cartera.csv',
                   encoding='CP1252', index=False)
    df_altas.to_csv(f'{ruta}df_altas.csv',
                encoding='CP1252', index=False)
    print('Detalle cartera ........ OK')


if __name__ == "__main__":
    main()
