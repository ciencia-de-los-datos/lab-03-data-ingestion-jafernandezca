"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import numpy as np
pd.set_option('display.max_colwidth', None)


def leer_linea(numero_linea, nombre_archivo):
    linea = ""
    with open(nombre_archivo, "r") as archivo:
        for num, contenido_linea in enumerate(archivo, 1):
            if num == numero_linea:
                linea = contenido_linea
                break
    return linea

def procesar_linea(linea):
    linea_procesada = linea.lower()
    palabras = linea_procesada.split()
    indice_inicial = 0
    posicion2 = 0
    
    for i in range(len(palabras)):
        palabra = palabras[i]
        posicion1 = linea_procesada.find(palabra, indice_inicial)
        if i != 0:
            diferencia = posicion1 - posicion2
            if diferencia == 1:
                linea_procesada = (linea_procesada[:posicion2] + "_" + 
                                   linea_procesada[posicion2 + 1:])
        posicion2 = posicion1 + len(palabra)
        indice_inicial = posicion1 + len(palabra)
    return linea_procesada


lista_intervalos = []
def procesar_linea2(linea):
    linea_procesada = linea.lower()
    palabras = linea_procesada.split()
    indice_inicial = 0
    indice = 0
    resultados = []
    for i in range(len(palabras)):
        palabra = palabras[i]
        posicion1 = linea_procesada.find(palabra, indice_inicial)
              
        longitud = len(palabra)
        resultado = {
            "columna": i+1,
            "palabra": palabra
        }
        
        if len(lista_intervalos) < 4:
            lista_intervalos.append(posicion1)
                
        resultados.append(resultado)    
        
        indice_inicial = posicion1 + len(palabra)
    
    return resultados


def asignar_columna(linea):
    linea = linea.rstrip()
    longitud_linea = len(linea)
    lista_intervalos = [0, 9, 25, 41]
    lista_intervalos.append(longitud_linea) 
    columnas = {i: '' for i in range(4)}
    for i in range(len(lista_intervalos) - 1):
        inicio_intervalo = lista_intervalos[i]
        fin_intervalo = lista_intervalos[i + 1]
        if fin_intervalo <= longitud_linea:
            columnas[i] = linea[inicio_intervalo:fin_intervalo]
        else:
            columnas[i] = linea[inicio_intervalo:]
    return columnas


def asignar_columna(linea):
    linea = linea.rstrip()
    longitud_linea = len(linea)
    lista_intervalos = [0, 9, 25, 41]
    lista_intervalos.append(longitud_linea) 
    columnas = {i: '' for i in range(4)}
    for i in range(len(lista_intervalos) - 1):
        inicio_intervalo = lista_intervalos[i]
        fin_intervalo = lista_intervalos[i + 1]
        if fin_intervalo <= longitud_linea:
            columnas[i] = linea[inicio_intervalo:fin_intervalo]
        else:
            columnas[i] = linea[inicio_intervalo:]
    return columnas


    
def unir_diccionarios(lista_diccionarios):
    diccionario_unido = {}
    for diccionario in lista_diccionarios:
        for key, value in diccionario.items():
            diccionario_unido.setdefault(key, []).append(value)
    return diccionario_unido


def eliminar_espacios(celda):
    return celda.strip() if isinstance(celda, str) else celda

# Función para eliminar espacios en blanco de las primeras tres columnas y devolver un nuevo DataFrame
def eliminar_espacios_df(df_original):
    df_nuevo = df_original.copy()  # Copiar el DataFrame original para no modificarlo
    for columna in df_nuevo.columns[:3]:
        df_nuevo[columna] = df_nuevo[columna].apply(eliminar_espacios)
    return df_nuevo



def ingest_data():

    numero_linea_deseada1 = 1
    numero_linea_deseada2 = 2
    nombre_archivo = "clusters_report.txt"  
    linea_seleccionada1 = leer_linea(numero_linea_deseada1, nombre_archivo)
    linea_seleccionada2 = leer_linea(numero_linea_deseada2, nombre_archivo)

    # Procesamos la línea seleccionada
    primera_linea_procesada = procesar_linea(linea_seleccionada1)
    segunda_linea_procesada = procesar_linea(linea_seleccionada2)

    columna_asignada1 = asignar_columna(primera_linea_procesada)
    columna_asignada2 = asignar_columna(segunda_linea_procesada)
    
    lista_diccionarios = [columna_asignada1, columna_asignada2]
    
    # Unir los diccionarios en uno solo
    diccionario_unido = unir_diccionarios(lista_diccionarios)

    # Crear el DataFrame a partir del diccionario unido
    df = pd.DataFrame(diccionario_unido)

    df_t = eliminar_espacios_df(df)
    df_t1 = df_t.replace('', pd.NA)
    grupost = df_t1[0].notnull().cumsum()
    df_combinadot = df_t1.groupby(grupost).agg(lambda x: ' '.join(x.dropna()))
    df_combi = df_combinadot.applymap(lambda x: x.replace(' ', '_') if isinstance(x, str) else x)
    df_combi

    with open(nombre_archivo, 'r') as archivo:
        next(archivo)  # Saltar la primera línea
        next(archivo)  # Saltar la segunda línea
        next(archivo)
        next(archivo)
        lista_resultados = []
        for linea in archivo:
            lista_resultados.append(asignar_columna(linea))

    df1 = pd.DataFrame(lista_resultados)
    df_2 = df1.replace('', np.nan)
    df_pro = df_2.dropna()
    
    df_procesado = eliminar_espacios_df(df_pro)
    df_3 = df_procesado.replace('', pd.NA)
    grupos = df_3[0].notnull().cumsum()
    df_combinado = df_3.groupby(grupos).agg(lambda x: ' '.join(x.dropna()))
    
    while True:
        celda_cambiada = False
        for i, fila in df_combinado.iterrows():
            if '  ' in fila[3]:
                df_combinado.at[i, 3] = fila[3].replace('  ', ' ')
                celda_cambiada = True
        if not celda_cambiada:
            break
    
    df_union = pd.concat([df_combi, df_combinado], ignore_index=True)
    df_union.columns = df_union.iloc[0]
    df_union1 = df_union[1:]
    df_union2 = df_union1.applymap(lambda x: x.replace(' %', '').replace(',', '.') if isinstance(x, str) else x)
    df_union2['cluster'] = df_union2['cluster'].astype(int)
    df_union2['cantidad_de_palabras_clave'] = df_union2['cantidad_de_palabras_clave'].astype(int)
    df_union2['porcentaje_de_palabras_clave'] = df_union2['porcentaje_de_palabras_clave'].astype(float)
    df_union2['principales_palabras_clave'] = df_union2['principales_palabras_clave'].str.replace('.', ',', regex=False).str.rstrip(',')


    return df_union2
