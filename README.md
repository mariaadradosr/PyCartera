# PyCartera

- añadir tarifa
- precio
- calcular dto


- sofia barat hay que hacer el cambio del assetid que se está pintando billing_details y el real 
- 


### Main
ruta = carperta destino


### Librerias:
    - pandas
    - numpy
    - re
    - dotenv
    - os
    - psycopg2


### To do's:
    - Ver cómo corregir el hecho de que un cif haya sido 'cliente' de dos partners
    - Meter reglas de negocio
    - segmento_empleados: posibilidad de crear una vista de la tabla vm_service_razonsocial por su tamaño
    - x fiber y x security con direcciones IP en la descripción
    - mirar el tema del sort=True en la parte de bajas concat


## Excel: Conexiones
Añadido de columnas manual:
Con las conexiones establecidadas y consultas hechas (tablas)
> Datos. Obtener datos. Iniciar editor de Power Query. Editor avanzado (sobre la consulta)

>`let
    Origen = Csv.Document(File.Contents("C:\Users\MariaAdradosReig\Desktop\XByOrange\2_Controlling\PyCartera\output\altas_tabla.csv"),[Delimiter=",", Columns=86, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Filas superiores quitadas" = Table.Skip(Origen,1),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Filas superiores quitadas",{{"Column2", "2018-10"}, {"Column3", "2018-11"}, {"Column4", "2018-12"}, {"Column5", "2019-01"}, {"Column6", "2019-02"}, {"Column7", "2019-03"}, {"Column8", "2019-04"}, {"Column9", "2019-05"}, {"Column10", "2019-06"}, {"Column11", "2019-07"}, {"Column12", "2019-08"}, {"Column13", "2019-09"}, {"Column14", "2019-10"}, {"Column15", "2019-11"}, {"Column16", "2019-12"}, {"Column17", "2020-01"}, {"Column18", "2020-02"}, {"Column19", "2020-03"}, {"Column20", "2020-04"}, {"Column21", "2020-05"}, {"Column22", "2020-06"}, {"Column23", "2020-07"}, {"Column24", "2020-08"}, {"Column25", "2020-09"}, {"Column26", "2020-10"}, {"Column27", "2020-11"}, {"Column28", "2020-12"}, {"Column29", "2021-01"}, {"Column30", "2021-02"}, {"Column31", "2021-03"}, {"Column32", "2021-04"}, {"Column33", "2021-05"}, {"Column34", "2021-06"}, {"Column35", "2021-07"}, {"Column36", "2021-08"}, {"Column37", "2021-09"}, {"Column38", "2021-10"}, {"Column39", "2021-11"}, {"Column40", "2021-12"}, {"Column41", "2022-01"}, {"Column42", "2022-02"}, {"Column43", "2022-03"}, {"Column44", "2022-04"}, {"Column45", "2022-05"}, {"Column46", "2022-06"}, {"Column47", "2022-07"}, {"Column48", "2022-08"}, {"Column49", "2022-09"}, {"Column50", "2022-10"}, {"Column51", "2022-11"}, {"Column52", "2022-12"}, {"Column53", "2023-03"}, {"Column54", "2023-04"}, {"Column55", "2023-05"}, {"Column56", "2023-06"}, {"Column57", "2023-07"}, {"Column58", "2023-08"}, {"Column59", "2023-09"}, {"Column60", "2023-10"}, {"Column61", "2023-11"}, {"Column62", "2023-12"}, {"Column63", "2024-01"}, {"Column64", "2024-02"}, {"Column65", "2024-03"}, {"Column66", "2024-04"}, {"Column67", "2024-05"}, {"Column68", "2024-06"}, {"Column69", "2024-07"}, {"Column70", "2024-08"}, {"Column71", "2024-09"}, {"Column72", "2024-10"}, {"Column73", "2024-11"}, {"Column74", "2024-12"}, {"Column75", "2025-01"}, {"Column76", "2025-02"}, {"Column77", "2025-03"}, {"Column78", "2025-04"}, {"Column79", "2025-05"}, {"Column80", "2025-06"}, {"Column81", "2025-07"}, {"Column82", "2025-08"}, {"Column83", "2025-09"}, {"Column84", "2025-10"}, {"Column85", "2025-11"}, {"Column86", "2025-12"}}),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Columnas con nombre cambiado",{{"2018-10", Int64.Type}, {"2018-11", Int64.Type}, {"2018-12", Int64.Type}, {"2019-01", Int64.Type}, {"2019-02", Int64.Type}, {"2019-03", Int64.Type}, {"2019-04", Int64.Type}, {"2019-05", Int64.Type}, {"2019-06", Int64.Type}, {"2019-07", Int64.Type}, {"2019-08", Int64.Type}, {"2019-09", Int64.Type}, {"2019-10", Int64.Type}, {"2019-11", Int64.Type}, {"2019-12", Int64.Type}, {"2020-01", Int64.Type}, {"2020-02", Int64.Type}, {"2020-03", Int64.Type}, {"2020-04", Int64.Type}, {"2020-05", Int64.Type}, {"2020-06", Int64.Type}, {"2020-07", Int64.Type}, {"2020-08", Int64.Type}, {"2020-09", Int64.Type}, {"2020-10", Int64.Type}, {"2020-11", Int64.Type}, {"2020-12", Int64.Type}, {"2021-01", Int64.Type}, {"2021-02", Int64.Type}, {"2021-03", Int64.Type}, {"2021-04", Int64.Type}, {"2021-05", Int64.Type}, {"2021-06", Int64.Type}, {"2021-07", Int64.Type}, {"2021-08", Int64.Type}, {"2021-09", Int64.Type}, {"2021-10", Int64.Type}, {"2021-11", Int64.Type}, {"2021-12", Int64.Type}, {"2022-01", Int64.Type}, {"2022-02", Int64.Type}, {"2022-03", Int64.Type}, {"2022-04", Int64.Type}, {"2022-05", Int64.Type}, {"2022-06", Int64.Type}, {"2022-07", Int64.Type}, {"2022-08", Int64.Type}, {"2022-09", Int64.Type}, {"2022-10", Int64.Type}, {"2022-11", Int64.Type}, {"2022-12", Int64.Type}, {"2023-03", Int64.Type}, {"2023-04", Int64.Type}, {"2023-05", Int64.Type}, {"2023-06", Int64.Type}, {"2023-07", Int64.Type}, {"2023-08", Int64.Type}, {"2023-09", Int64.Type}, {"2023-10", Int64.Type}, {"2023-11", Int64.Type}, {"2023-12", Int64.Type}, {"2024-01", Int64.Type}, {"2024-02", Int64.Type}, {"2024-03", Int64.Type}, {"2024-04", Int64.Type}, {"2024-05", Int64.Type}, {"2024-06", Int64.Type}, {"2024-07", Int64.Type}, {"2024-08", Int64.Type}, {"2024-09", Int64.Type}, {"2024-10", Int64.Type}, {"2024-11", Int64.Type}, {"2024-12", Int64.Type}, {"2025-01", Int64.Type}, {"2025-02", Int64.Type}, {"2025-03", Int64.Type}, {"2025-04", Int64.Type}, {"2025-05", Int64.Type}, {"2025-06", Int64.Type}, {"2025-07", Int64.Type}, {"2025-08", Int64.Type}, {"2025-09", Int64.Type}, {"2025-10", Int64.Type}, {"2025-11", Int64.Type}, {"2025-12", Int64.Type}})
in
    #"Tipo cambiado"`