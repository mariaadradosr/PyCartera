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
    cli_vta_mes_0 = functions.cli_vta_mes(df,0).reset_index()
    cli_vta_mes_0.to_csv('../output/vta_cli_mes_0.csv ',encoding='CP1252',index=False)
    print('Ventas por cliente mensuales TOTAL ........ OK')
    
    cli_vta_mes_2 = functions.cli_vta_mes(df,2).reset_index()
    cli_vta_mes_2.to_csv('../output/vta_cli_mes_2.csv ',encoding='CP1252',index=False)
    print('Ventas por cliente mensuales CANAL ........ OK')

    cli_vta_mes_3 = functions.cli_vta_mes(df,3).reset_index()
    cli_vta_mes_3.to_csv('../output/vta_cli_mes_3.csv ',encoding='CP1252',index=False)
    print('Ventas por cliente mensuales CANAL + FAM ........ OK')

    ass_vta_mes_5 = functions.ass_vta_mes(df,5).reset_index()
    ass_vta_mes_5.to_csv('../output/vta_ass_mes_5.csv ',encoding='CP1252',index=False)
    print('Ventas assets mensuales CANAL + FAM + PRODUCTO ........ OK')

    # ALTAS
    cli_altas_mes_0 = functions.cli_altas_mes_agg(df_altas,tipo=0).reset_index()
    cli_altas_mes_0.to_csv('../output/altas_cli_mes_0.csv ',encoding='CP1252',index=False)
    print('Altas por cliente mensuales TOTAL ........ OK')
    
    cli_altas_mes_1 = functions.cli_altas_mes_agg(df_altas,1).reset_index()
    cli_altas_mes_1.to_csv('../output/altas_cli_mes_1.csv ',encoding='CP1252',index=False)
    print('Altas por cliente mensuales CANAL ........ OK')

    cli_altas_mes_3 = functions.cli_altas_mes(df,3).reset_index()
    cli_altas_mes_3.to_csv('../output/altas_cli_mes_3.csv ',encoding='CP1252',index=False)
    print('Altas por cliente mensuales CANAL + FAM ........ OK')

    ass_altas_mes_5 = functions.ass_altas_mes(df,5).reset_index()
    ass_altas_mes_5.to_csv('../output/altas_ass_mes_5.csv ',encoding='CP1252',index=False)
    print('Altas assets mensuales CANAL + FAM + PRODUCTO ........ OK')

    # BAJAS
    cli_bajas_mes_0 = functions.cli_bajas_mes_agg(df_altas,tipo=0).reset_index()
    cli_bajas_mes_0.to_csv('../output/bajas_cli_mes_0.csv ',encoding='CP1252',index=False)
    print('Bajas por cliente mensuales TOTAL ........ OK')
    
    cli_bajas_mes_1 = functions.cli_bajas_mes_agg(df_altas,1).reset_index()
    cli_bajas_mes_1.to_csv('../output/bajas_cli_mes_1.csv ',encoding='CP1252',index=False)
    print('Bajas por cliente mensuales CANAL ........ OK')

    cli_bajas_mes_3 = functions.cli_bajas_mes(df,3).reset_index()
    cli_bajas_mes_3.to_csv('../output/bajas_cli_mes_3.csv ',encoding='CP1252',index=False)
    print('Bajas por cliente mensuales CANAL + FAM ........ OK')

    ass_bajas_mes_5 = functions.ass_bajas_mes(df,5).reset_index()
    ass_bajas_mes_5.to_csv('../output/bajas_ass_mes_5.csv ',encoding='CP1252',index=False)
    print('Bajas assets mensuales CANAL + FAM + PRODUCTO ........ OK')

    # MIGRAS

    migin, migout = functions.mig_producto_mes(mig_df)
    migin.to_csv('../output/migin.csv ',encoding='CP1252',index=False)
    print('Migras in mensuales ........ OK')
    migout.to_csv('../output/migout.csv ',encoding='CP1252',index=False)
    print('Migras out mensuales ........ OK')

    # DETALLE CARTERA
    detalle.to_excel('../output/detalle_cartera.xlsx',encoding='CP1252',index=False)
    print('Detalle cartera ........ OK')





if __name__=="__main__":
    main()