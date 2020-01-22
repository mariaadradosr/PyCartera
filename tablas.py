import functions

cur = functions.connectToPs()

df = functions.ventas(cur)
altas = functions.altas(cur)

###### VENTAS MENSUALES

# Faltaría la tabla por asset pero tengo que mirar cómo homogeneizar los nombres de los assets

#Tabla VENTAS - FAMILIA - ASSET

vta_fam_ass = df[['Total']].groupby(by=[df.purchase_year,df.purchase_month,df.Family]).sum().reset_index()
# vta_fam_ass.reset_index(inplace=True)
# vta_fam_ass.drop(['index'], axis=1, inplace=True)
vta_fam_ass.pivot_table('Total','Family',['purchase_year','purchase_month'], fill_value= 0, margins=True, aggfunc= sum)

#Tabla VENTAS - FAMILIA - CLIENTE

vta_fam_cif = df[['cif']].groupby(by=[df.purchase_year,df.purchase_month,df.Family]).nunique().reset_index()
vta_fam_cif.pivot_table('cif','Family',['purchase_year','purchase_month'], fill_value= 0, margins=True, aggfunc= sum)

#Tabla VENTAS - CLIENTE

vta_cif =  df[['cif']].groupby(by=[df.purchase_year,df.purchase_month]).nunique().reset_index().T


#Tabla VENTAS - ASSET


###### ALTAS MENSUALES
# Faltaría la tabla por asset pero tengo que mirar cómo homogeneizar los nombres de los assets

#Tabla ALTAS - FAMILIA - ASSET (Tienen sentido los totales)

alta_fam_ass = altas[['Total']].groupby(by=[altas.alta_year,altas.alta_month,altas.Family]).sum().reset_index()
alta_fam_ass.pivot_table('Total','Family',['alta_year','alta_month'], fill_value= 0, margins=True, aggfunc= sum)

#Tabla ALTAS - FAMILIA - CLIENTE (No tienen sentido los totales)
# Para las altas por cliente se cuenta como alta la primera vez que contrata:
# ------ 1) un servicio en el caso alta_fam_cif
# ------ 2) 
alta_fam_cif = functions.altas_cif(altas,'Family').pivot_table('cif','Family',['alta_year','alta_month'], fill_value= 0)

#Tabla ALTAS - CLIENTE

alta_cif =  functions.altas_cif(altas,'Total').T


###### BAJAS MENSUALES