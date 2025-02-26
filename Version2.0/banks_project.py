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
                market_cap = market_cap.replace("\n","")
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

def transform(df, csv_path):
    '''Funcion para acceder a archivo de conversion de moneta y adicionar
    3 columnas al dataset con la moneda actual para cada caso'''
    df_ex = pd.read_csv(csv_path)
    conversion_dict = df_ex.set_index('Currency').to_dict()['Rate']
    
    # Creacion de nuevas columnas MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion
    df['MC_GBP_Billion'] = [np.round(x*conversion_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*conversion_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*conversion_dict['INR'], 2) for x in df['MC_USD_Billion']]
    print(df)
    return df

def load_to_csv(df, output_path):
    ''' Funcion para salvar el resultado final como un archivo CSV en una ruta sugerida
    no se retorna nada'''
    df.to_csv(output_path)
    pass

def load_to_db(df, sql_connection, table_name):
    '''Esta funciona guarda el dataframe final como un archivo de tipo base de datos con un nombre y una tabla'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    '''Funcion que corre una consulta que se asigna y se imprime desde la terminal'''
    print(f"Consulta realizada: \n{query_statement}")
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


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
    csv_exchange_rate = "./exchange_rate.csv"
    db_name = "Banks.db"
    table_name = "Largest_banks"
    
    # Step 1.
    log_progress("Preliminaries complete. Initiating ETL process")
    df = extract(URL, table_attributes)
    log_progress("Data extraction complete. Initiating transformation process")
    # Step 2.
    log_progress("Data tranformation complete. Initiation Loging process")
    df_transform = transform(df, csv_exchange_rate)
    #Step 3.
    log_progress("Data saved to CSV file")
    load_to_csv(df,csv_path)
    #Step 4.
    log_progress("SQL connection initiated")
    sql_connection = sqlite3.connect(db_name)
    load_to_db(df_transform,sql_connection, table_name)
    #Step 5.
    log_progress("Data loaded to Database as a table, Executing queries")
    query_statement1 = f"SELECT * FROM {table_name}"
    query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
    query_statement3 = f"SELECT bank_name FROM {table_name} LIMIT 5 "
    run_query(query_statement1,sql_connection)
    run_query(query_statement2,sql_connection)
    run_query(query_statement3,sql_connection)
    log_progress("Process complete")

    #Step 6.
    sql_connection.close()
    log_progress("Server Connection closed")
