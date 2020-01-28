import functions


def main():
    cur = functions.connectToPs()
    df = functions.base(cur)
    df_altas = functions.base_altas(df)
    mig_df=functions.migBase(cur)
    
    # VENTAS
    cli_vta_mes_0 = functions.cli_vta_mes(df,0).reset_index()
    cli_vta_mes_0.to_csv('.output/cli_vta_mes_0.csv',encoding='CP1252',index=False)
    print('Ventas por cliente mensuales TOTAL ........ EXPORTADO')
    
    cli_vta_mes_2 = functions.cli_vta_mes(df,2).reset_index()
    cli_vta_mes_2.to_csv('.output/cli_vta_mes_2.csv',encoding='CP1252',index=False)
    print('Ventas por cliente mensuales CANAL ........ EXPORTADO')

    cli_vta_mes_3 = functions.cli_vta_mes(df,3).reset_index()
    cli_vta_mes_3.to_csv('.output/cli_vta_mes_3.csv',encoding='CP1252',index=False)
    print('Ventas por cliente mensuales CANAL + FAM ........ EXPORTADO')

    ass_vta_mes_5 = functions.cli_vta_mes(df,5).reset_index()
    ass_vta_mes_5.to_csv('.output/ass_vta_mes_5.csv',encoding='CP1252',index=False)
    print('Ventas assets mensuales CANAL + FAM + PRODUCTO ........ EXPORTADO')

    # ALTAS
    cli_altas_mes_0 = functions.cli_altas_mes(df_altas,tipo=0).reset_index()
    cli_altas_mes_0.to_csv('.output/cli_altas_mes_0.csv',encoding='CP1252',index=False)
    print('Altas por cliente mensuales TOTAL ........ EXPORTADO')
    
    cli_altas_mes_1 = functions.cli_altas_mes(df_altas,1).reset_index()
    cli_altas_mes_1.to_csv('.output/cli_altas_mes_1.csv',encoding='CP1252',index=False)
    print('Altas por cliente mensuales CANAL ........ EXPORTADO')

    cli_altas_mes_3 = functions.cli_altas_mes(df,3).reset_index()
    cli_altas_mes_3.to_csv('.output/cli_altas_mes_3.csv',encoding='CP1252',index=False)
    print('Altas por cliente mensuales CANAL + FAM ........ EXPORTADO')

    ass_altas_mes_5 = functions.ass_altas_mes(df,5).reset_index()
    ass_altas_mes_5.to_csv('.output/ass_vta_mes_5.csv',encoding='CP1252',index=False)
    print('Altas assets mensuales CANAL + FAM + PRODUCTO ........ EXPORTADO')


    # BAJAS
    cli_bajas_mes_0 = functions.cli_bajas_mes(df_altas,tipo=0).reset_index()
    cli_bajas_mes_0.to_csv('.output/cli_bajas_mes_0.csv',encoding='CP1252',index=False)
    print('Bajas por cliente mensuales TOTAL ........ EXPORTADO')
    
    cli_bajas_mes_1 = functions.cli_bajas_mes(df_altas,1).reset_index()
    cli_bajas_mes_1.to_csv('.output/cli_bajas_mes_1.csv',encoding='CP1252',index=False)
    print('Bajas por cliente mensuales CANAL ........ EXPORTADO')

    cli_bajas_mes_3 = functions.cli_bajas_mes(df,3).reset_index()
    cli_bajas_mes_3.to_csv('.output/cli_bajas_mes_3.csv',encoding='CP1252',index=False)
    print('Bajas por cliente mensuales CANAL + FAM ........ EXPORTADO')

    ass_bajas_mes_5 = functions.ass_bajas_mes(df,5).reset_index()
    ass_bajas_mes_5.to_csv('.output/ass_vta_mes_5.csv',encoding='CP1252',index=False)
    print('Bajas assets mensuales CANAL + FAM + PRODUCTO ........ EXPORTADO')

    # MIGRAS






if __name__=="__main__":
    main()