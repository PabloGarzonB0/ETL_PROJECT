# PROGRAMA DE OPERACIONES ETL PARA EXTRACCION DE INFORMACION PIB DE PAISES

# importacion de librerias
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
import numpy as np
import sqlite3

def extract(url, table_attribs):
    ''' Esta funcion extrae la infromacion requerida de la pagina web y la guarda en un
    dataframe de pandas'''
    
    html_page = requests.get(url).text
    soup_object = BeautifulSoup(html_page, 'html.parser')
    # Creacion de dataframe vacio
    df = pd.DataFrame(columns=table_attribs)
    table = soup_object.find_all('table')
    tbody_table = table[2].tbody 
    rows_table = tbody_table.find_all('tr')

    for i, row in enumerate(rows_table):
        col = row.find_all('td') # Trae columna a columna de una fila
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col [2]:
                country =  col[0].a.text
                gdp = col[2].text.strip()
                data_dict = {'Country': country, \
                          'GDP_USD_millions':gdp}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df):
    ''' Esta funcion convierte la funcion del dataframe en formato floar pasando de millones a 
    billones, redondeando a 2 decimales y retornando el dataframe transformado'''
    # df["GDP_USD_millions"] = df["GDP_USD_millions"].str.replace(',','').astype('float')
    # df["GDP_USD_billions"] = df["GDP_USD_millions"]/1000
    # df["GDP_USD_billions"] = (df["GDP_USD_billions"]).round(2)
    # df.drop(columns=["GDP_USD_billions"], inplace=True)
    
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000, decimals=2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df = df.rename(columns={"GDP_USD_millions":"GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
    ''' Esta funcion guarda el dataframe final en un archivo .csv considerando la ruta de 
    almacenamiento'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    '''Esta funcion guarga el dataframe final en una tabla dentro de la base de datos'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statment, sql_connection):
    '''Esta funcion ejecuta una consulta dentro de la base de datos'''
    print(f"Consulta Realizada: \n {query_statment}")
    query_output = pd.read_sql(query_statment, sql_connection)
    print(query_output)


def log_progress(message):
    '''Esta funcion genera un archivo log donde se guarda los registro de operaciones 
    de ejecucion realizados'''
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("log_file.txt", "+a") as file:
        file.write("[{}] {} \n".format(timestamp,message))
    



if __name__ == '__main__':
    # Inicializacion de varibles principales
    URL = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    table_attribs = ["Country", "GDP_USD_millions"]
    db_name = 'World_Economies.db'
    table_name = 'Countries_by_GDP'
    csv_path = 'Countries_by_GDP.csv'

    log_progress("ETL JOB STARTED")
    
    log_progress("1. Web_Page Data Extraction ")
    df = extract(URL, table_attribs)
    log_progress("Data extraction complete. Initiating Transformation process")
    
    df_prueba = df.copy()
    
    log_progress("2. Transform Job Started")
    df_transfrom = transform(df_prueba)
    log_progress("Data transformation complete. initiating loading process")
    
    log_progress("3. Load  Job Started")
    load_to_csv(df_transfrom, csv_path)
    sql_connection = sqlite3.connect(db_name)  
    load_to_db(df_transfrom, sql_connection, table_name)
    log_progress("Data Saved")
    
    #Consulta en base de datos
    log_progress("4. Quering the database ")
    query_statment = f"SELECT Country, GDP_USD_billions as GDP FROM  {table_name} WHERE GDP >= 100"
    run_query(query_statment, sql_connection)
    log_progress("Process Complete")
    
    
    
    log_progress("Finish ETL Process...")  
    
    sql_connection.close()
    