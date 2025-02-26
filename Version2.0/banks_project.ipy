from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
import numpy as np

def extract(url, table_attribs):
    '''Funcion para extraer los datos dentro de la URL y almacenarlos en 
        un dataframe con la infroamcion del nombre y ranking mundial '''
    html_page = requests.get(url).text
    soup_object = BeautifulSoup(html_page, 'html.parser')
    # Creacion de dataframe donde se almacenan los valores
    df = pd.DataFrame(columns=table_attribs)
    table = soup_object.find_all('table')
    table_2 = table[0]
    tbody_table = table_2.tbody
    rows_table = tbody_table.find_all('tr')
    
    for row in rows_table:
        col = row.find_all("td")
        if len(col) != 0:
            if col[1].find('a') is not None and ' ' not in col[2]:
                bank_name = col[1].find_all('a')[1]['title']
                market_cap = col[2].text.strip()
                # Eliminar caracteres no numéricos y convertir a float
                market_cap = market_cap.replace(",", "").replace(".", "").replace("\n","")
                # Ingresar valores extraidos de web a nuevo dataframe
                try:
                    market_cap = float(market_cap)
                    # Almacenando informacion en diccionario
                    data_dict = {'bank_name':  bank_name,
                                'MC_USD_Billion': market_cap}
                    new_dataframe = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, new_dataframe], ignore_index=True)
                except ValueError:
                    print(f"Error al convertir {market_cap} a float")
    return df

def transfrorm(df, csv_path):
    

def load_to_csv(df, output_path):
    pass

def load_lo_db(df, sql_connection, table_name):
    pass

def run_query(query_statement, sql_connection):
    pass


def log_progress(message):
    '''Funcion para registrar los mensajes respecto a las operaciones
    dentro del ETL'''
    timestamp_format = "%Y-%m-%dT%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as file:
        file.write("[{}] {} \n".format(timestamp,message))
        
if __name__ == '__main__':
    
    # variables initialized   
    URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    table_attributes = ["bank_name","MC_USD_Billion"]
    csv_path = "./Largest_banks_data.csv"
    db_name = "Banks.db"
    table_name = "Largest_banks"

    df = extract(URL, table_attributes)
    
    df.to_csv("archivo.csv")
    '''
    Log message used later
    log_progress("Preliminaries complete. Initiating ETL process")
    log_progress("Data extraction complete. Initiating transformation process"
    log_progress("Data tranformation complete. Initiation Loging process")
    log_progress("Data saved to CSV file")
    log_progress("SQL connection initiated")
    log_progress("Data loaded to Database as a table, Executing queries")
    log_progress("Process complete")
    log_progress("Server Connection closed")
    
    
    '''
