"""
Universidad de Sonora
Maestría en Ciencia de Datos

Ingeniería de Características
Dr. Julio Waissman Vilanova
Melissa Reyes Paz

Noviembre de 2022

El presente script de python forma parte del proyecto final del curso de Ingeniería de 
características de la Maestría en Ciencia de Datos. 
El objeto del proyecto es, por medio de datos, describir si una victima de feminicidio fue reportada como desaparecida con anterioridad.

Se utilizaron dos fuentes principales para este proyecto. Los Datos abiertos de Incidencia delictiva (SESNSP) y los Datos Abiertos del Gobierno, donde se presentan las Estadísticas de Personas Desaparecidas no localizadas (EPDNL).

El script se encarga de descargar los archivos de la fuente y armonizar las variables.

El script concluye con la creación de un solo archivo csv donde se incluye toda esta información. 

Tota la información referente al proyecto está disponible en un repositorio de Github: https://github.com/melrepa/Proyecto-Integrador-Ing-Car y la respectiva página https://melrepa.github.io/Proyecto-Integrador-Ing-Car/dashboard/

"""

import pandas as pd
import numpy as np
import datetime
import urllib.request
import os
import requests
import csv


#Si desea cambiar donde se descargaran los datos, modificar la siguiente variable.

subdir = "./data/"

#---------------Descarga de datos desde su fuente-------------------#
print("#---------------Descarga de datos desde su fuente-------------------#")

#URL datos de SESNSP
SESNSP_1522='https://drive.google.com/u/0/uc?id=1i2Zts5aDcd8cfixtA1Jn-JGlKIgYTZWN&export=download'
Diccionario_SESNSP_1522='https://drive.google.com/u/0/uc?id=1rfvgcAcEzLR1Q44wwjZhjBvBFjtGSmX3&export=download'

#Ubicacion para datos de la SESNSP
SESNSP_1522_file='IDEFC_NM_ago22.csv'
Diccionario_SESNSP_1522_file='DD_sesNSP.xlsx'

#URL datos de EPDNL
EPDNL_18 = 'http://datosabiertospgr.blob.core.windows.net/desaparecidas-pgr/Estadisticade_Personas_Desaparecidas_No_Localizadas.csv'
Diccionario_EPDNL_18 = 'http://datosabiertospgr.blob.core.windows.net/desaparecidas-pgr/Diccionario_Datos_Personas_Desaparecidas_NoLocalizadas.csv'

#Ubicación para datos de EPDNL
EPDNL_18_file = 'Estadistica_Personas_Desaparecidas_No_Localizadas.csv'
Diccionario_EPDNL_18_file = 'Diccionario_Datos_Personas_Desaparecidas_NoLocalizadas.csv'


SESNSP = { SESNSP_1522:SESNSP_1522_file, Diccionario_SESNSP_1522:Diccionario_SESNSP_1522_file }
EPDNL = { EPDNL_18:EPDNL_18_file, Diccionario_EPDNL_18:Diccionario_EPDNL_18_file}


for url, archivo in SESNSP.items(): 
    if not os.path.exists(archivo):
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        urllib.request.urlretrieve(url, subdir + archivo)  
print("Descarga de SESNSP terminada.")

for url, archivo in EPDNL.items(): 
    if not os.path.exists(archivo):
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        urllib.request.urlretrieve(url, subdir + archivo)
print("Descarga de EPDNL terminada.")


#---------------Log de la descarga-------------------#
with open(subdir + "info.txt", 'w') as f:
    f.write("Archivos con datos sobre feminicidios \n")
    info = """
    Los datos fueron descargados de los portales del Gobierno Federal. 
    Los datos presentan información referente a delitos registrados por entidad federativa en cada año, 
    asi como tambien específicamente de la cantidad de feminicidios de 2015 a septiembre de 2022 y la estadística de las personas desaparecidas no localizadas hasta 2018. 

    """ 
    f.write(info + '\n')
    f.write("Descargado el " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")
    f.write("|-------------------------Metadatos de la descarga de SESNSP-------------------------|"+'\n')
    for url, archivo in SESNSP.items():        
        f.write("Desde: " + url + "\n")
        f.write("Nombre: " + archivo + "\n")
    f.write("|-------------------------Metadatos de la descarga de EPDNL-------------------------|"+'\n')
    for url, archivo in EPDNL.items():        
        f.write("Desde: " + url + "\n")
        f.write("Nombre: " + archivo + "\n")  


#---------------Integración de datos de cada fuente en dos fuentes -------------------#
print("#---------------Integración de datos de cada fuente en dos fuentes-------------------#")

sesnsp=pd.read_csv("data//"+SESNSP_1522_file, encoding='latin-1')

sesnsp_columns=['Año', 
                'Clave_Ent', 
                'Entidad', 
                'Bien jurídico afectado',
                'Tipo de delito',
                'Subtipo de delito',
                'Modalidad',
                'Enero',  
                'Febrero',
                'Marzo',
                'Abril',
                'Mayo',
                'Junio',
                'Julio',
                'Agosto',
                'Septiembre',
                'Octubre',
                'Noviembre',
                'Diciembre']


SESNSP_tidy=sesnsp[sesnsp_columns].copy()

#Creando lista de columna para descartar Año y Clave_Ent para el momento de la sumatoria. 
col_listSESNSP= list(SESNSP_tidy)
col_listSESNSP.remove('Año')
col_listSESNSP.remove('Clave_Ent')
#Suma de columnas numéricas (solo los datos registrados en meses para obtener el total anual)
SESNSP_tidy['Total'] = SESNSP_tidy[col_listSESNSP].sum(axis=1)

#Descartando columnas que no se usarán
SESNSP_tidy.drop(['Clave_Ent', 
                'Bien jurídico afectado', 
                'Subtipo de delito', 
                'Modalidad',
                'Enero',  
                'Febrero',
                'Marzo',
                'Abril',
                'Mayo',
                'Junio',
                'Julio',
                'Agosto',
                'Septiembre',
                'Octubre',
                'Noviembre',
                'Diciembre'], axis = 'columns', inplace=True)

#Descartando filas que no pertenecen al delito Feminicidio
SESNSP_tidy = SESNSP_tidy.drop(SESNSP_tidy[SESNSP_tidy['Tipo de delito']!='Feminicidio'].index)

#Descartando filas que no pertenecen a los años 2015-2018
SESNSP_tidy = SESNSP_tidy.drop(SESNSP_tidy[SESNSP_tidy['Año']> 2018].index)

#Renombrando columna
SESNSP_tidy = SESNSP_tidy.rename(columns ={'Tipo de delito':'Delito'})

#Convirtiendo valores float a int
convert_dict = {'Total': int}
SESNSP_tidy = SESNSP_tidy.astype(convert_dict)

#Uniendo filas pertenecientes al mismo año y entidad.
def unir_feminicidios(SESNSP_tidy):
  f1 = ", ".join(f"{fem:}" for fem in set(SESNSP_tidy.Delito.dropna()))
  t = np.sum(SESNSP_tidy.Total.dropna())
  return pd.DataFrame({"Delito":[f1], "Total":[t]})

SESNSP_tidy = SESNSP_tidy.groupby(["Año", "Entidad"]).apply(unir_feminicidios).droplevel(-1).reset_index()

#Ordenando por estado
SESNSP_tidySt = SESNSP_tidy.sort_values("Entidad")



epdnl=pd.read_csv("data//"+EPDNL_18_file, encoding='latin-1')


epdnl_columns=['AÑO',
               'MES',
               'Entidad donde se recibió la Denuncia',
                'RD_H', 
                'RD_M', 
                'RD_215_H', 
                'RD_215_M',
                'EVDI_H',
                'EVDI_M',
                'EVDV_H',
                'EVDV_M',  
                'EV_215_I_H',
                'EV_215_I_M',
                'EV_215_V_H',
                'EV_215_V_M',
                'DESV_H',
                'DESV_M',
                'DESV_215_H',
                'DESV_215_M',
                'CDNB_H',
                'CDNB_M',
                'CDNB_215_H',
                'CDNB_215_M']


#Descartando columnas que no se usarán
epdnl.drop(['MES',
            'RD_H', 
            'RD_215_H', 
            'EVDI_H',
            'EVDV_H',
            'EV_215_I_H',
            'EV_215_V_H',
            'DESV_H',
            'DESV_215_H',
            'CDNB_H',
            'CDNB_215_H'], axis = 'columns', inplace=True)
EPDNL_tidy = epdnl


#Descartando filas que no pertenecen a los años 2015-2018
EPDNL_tidy = EPDNL_tidy.drop(EPDNL_tidy[EPDNL_tidy['AÑO']< 2015].index)

#Renombrando columna
EPDNL_tidy = EPDNL_tidy.rename(columns ={'Entidad donde se recibió la Denuncia':'Estado'})


#Uniendo filas pertenecientes al mismo año y entidad.
def unir_desap(EPDNL_tidy):
  R = np.sum(EPDNL_tidy.RD_M.dropna())
  R215 = np.sum(EPDNL_tidy.RD_215_M.dropna())
  EVDI = np.sum(EPDNL_tidy.EVDI_M.dropna())
  EVDV = np.sum(EPDNL_tidy.EVDV_M.dropna())
  EV = np.sum(EPDNL_tidy.EV_215_I_M.dropna())
  EVV = np.sum(EPDNL_tidy.EV_215_V_M.dropna())
  DESV = np.sum(EPDNL_tidy.DESV_M.dropna())
  DESV215 = np.sum(EPDNL_tidy.DESV_215_M.dropna())
  CDNB = np.sum(EPDNL_tidy.CDNB_M.dropna())
  CDNB215 = np.sum(EPDNL_tidy.CDNB_215_M.dropna())
  return pd.DataFrame({"RD_M":[R], "RD_215_M":[R215], "EVDI_M":[EVDI], "EVDV_M":[EVDV], "EV_215_I_M":[EV], "EV_215_V_M":[EVV],"DESV_M":[DESV], "DESV_215_M":[DESV215], "CDNB_M":[CDNB], "CDNB_215_M":[CDNB215]})

EPDNL_tidy = EPDNL_tidy.groupby(["AÑO", "Estado"]).apply(unir_desap).droplevel(-1).reset_index()

#Ordenando por estado
EPDNL_tidySt = EPDNL_tidy.sort_values("Estado")



#--------------------Manejo de valores perdidos-----------------#
print("#--------------------Manejo de valores perdidos-----------------#")



#------------------------Armonización de variables-------------------------#
print("#------------------------Armonización de variables-------------------------#")

Feminicidios = SESNSP_tidy
Desaparecidos = EPDNL_tidy

Feminicidios['Año'] = Feminicidios['Año'].astype('str')
Feminicidios['Año'] = Feminicidios['Año'].astype('datetime64')

Desaparecidos['AÑO'] = Desaparecidos['AÑO'].astype('str')
Desaparecidos['AÑO'] = Desaparecidos['AÑO'].astype('datetime64')

femin= Feminicidios.groupby("Año").sum()["Total"].reset_index()
print("El número total de feminicidios denunciados por año es de "+str()+":")
femin=pd.DataFrame(femin) 
print(femin)


RDcolumns=['RD_M', 
           'RD_215_M']
EVDIcol=['EVDI_M',
         'EV_215_I_M']
EVDVcol = ['EVDV_M', 
           'EV_215_V_M']
DESVcol=['DESV_M',
         'DESV_215_M']
CDNBcol=['CDNB_M',
         'CDNB_215_M']

#Suma de columnas numéricas (solo los datos  que pertenecen y no al articulo 215)
Desaparecidos['RD'] = Desaparecidos[RDcolumns].sum(axis=1)
Desaparecidos['EVDI'] = Desaparecidos[EVDIcol].sum(axis=1)
Desaparecidos['EVDV'] = Desaparecidos[EVDVcol].sum(axis=1)
Desaparecidos['DESV'] = Desaparecidos[DESVcol].sum(axis=1)
Desaparecidos['CDNB'] = Desaparecidos[CDNBcol].sum(axis=1)

#Descartando columnas que no se usarán
Desaparecidos.drop(['RD_M', 'RD_215_M', 'EVDI_M', 'EV_215_I_M', 'EVDV_M', 'EV_215_V_M',
                    'DESV_M', 'DESV_215_M', 'CDNB_M', 'CDNB_215_M'], axis = 'columns', inplace=True)



desap= Desaparecidos.groupby("AÑO").sum()["RD"].reset_index()
print("El número total de mujeres reportadas como desaparecidas por año es de "+str()+":")
desap=pd.DataFrame(desap)
print(desap)


FinalData = pd.concat([Feminicidios, Desaparecidos], axis=1)
#Descartando columnas que no se usarán
FinalData.drop(['Delito',
            'AÑO', 
            'Estado'], axis = 'columns', inplace=True)
#Renombrando columna
FinalData = FinalData.rename(columns ={'Total':'TotalFeminicidios'})
FinalDatast = FinalData.groupby(by = ['Entidad']).sum().reset_index()


FinalData = FinalData.replace('Coahuila de Zaragoza','Coahuila')
FinalData = FinalData.replace('Michoacán de Ocampo','Michoacán')
FinalData = FinalData.replace('Veracruz de Ignacio de la Llave','Veracruz')


FinalDatast = FinalDatast.replace('Coahuila de Zaragoza','Coahuila')
FinalDatast = FinalDatast.replace('Michoacán de Ocampo','Michoacán')
FinalDatast = FinalDatast.replace('Veracruz de Ignacio de la Llave','Veracruz')


print("#-----------------------Proceso terminado, se crea archivo csv-----------------------#")

FinalData.to_csv("data//"+"femin-desap.csv")
FinalDatast.to_csv("data//"+"femin-desapst.csv")