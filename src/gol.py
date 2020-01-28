    cli_bajas_mes_0 = functions.cli_bajas_mes(df_bajas,tipo=0).reset_index()
    cli_bajas_mes_0.to_csv('.output/cli_bajas_mes_0.csv',encoding='CP1252',index=False)
    print('Bajas por cliente mensuales TOTAL ........ EXPORTADO')
    
    cli_bajas_mes_1 = functions.cli_bajas_mes(df_bajas,1).reset_index()
    cli_bajas_mes_1.to_csv('.output/cli_bajas_mes_1.csv',encoding='CP1252',index=False)
    print('Bajas por cliente mensuales CANAL ........ EXPORTADO')

    cli_bajas_mes_3 = functions.cli_bajas_mes(df,3).reset_index()
    cli_bajas_mes_3.to_csv('.output/cli_bajas_mes_3.csv',encoding='CP1252',index=False)
    print('Bajas por cliente mensuales CANAL + FAM ........ EXPORTADO')

    ass_bajas_mes_5 = functions.ass_bajas_mes(df,5).reset_index()
    ass_bajas_mes_5.to_csv('.output/ass_vta_mes_5.csv',encoding='CP1252',index=False)
    print('Bajas assets mensuales CANAL + FAM + PRODUCTO ........ EXPORTADO')