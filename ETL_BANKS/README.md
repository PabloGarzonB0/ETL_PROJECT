# Proyecto ETL de Bancos

## Escenario del Proyecto
Una gran compania financiera ha solicitado crear un script que compile una lista de los 10 bancos más grandes del mundo, 
clasificados por capitalización de mercado en miles de millones de USD. Los datos deben transformarse a GBP, EUR e INR utilizando información de tasas de cambio disponible en un archivo CSV.
La información procesada se almacenará localmente en un archivo CSV y en una base de datos.

Este sistema automatizado debe ejecutarse cada trimestre financiero para generar informes actualizados.

## Detalles del Proyecto

| Parámetro                  | Valor |
|---------------------------|--------------------------------------------------------|
| **Nombre del código**     | banks_project.py |
| **URL de los datos**      | [Wikipedia - Bancos más grandes](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks) |
| **Ruta del CSV de tasas de cambio** | [Exchange Rate CSV](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv) |
| **Atributos de la tabla (extracción)** | Name, MC_USD_Billion |
| **Atributos de la tabla (final)** | Name, MC_USD_Billion, MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion |
| **Ruta del CSV de salida** | `./Largest_banks_data.csv` |
| **Nombre de la base de datos** | `Banks.db` |
| **Nombre de la tabla**     | `Largest_banks` |
| **Archivo de registro**    | `code_log.txt` |

## Descripción de la Implementación

El proyecto consta de varias etapas:
- Extraer datos de capitalización de mercado de los bancos desde una página de Wikipedia.
- Transformar los datos extraídos convirtiendo los valores a GBP, EUR e INR utilizando las tasas de cambio proporcionadas.
- Almacenar los datos procesados en un archivo CSV y en una base de datos SQLite para facilitar su acceso y análisis futuro.
- Registrar el progreso de la ejecución en un archivo de log para asegurar la trazabilidad.

## Estructura de Carpetas
```
Banks_ETL_Project/
│-- banks_project.py       # Script principal
│-- data/
│   │-- exchange_rate.csv  # CSV con tasas de cambio
│   │-- Largest_banks_data.csv  # Datos procesados
│-- database/
│   │-- Banks.db           # Base de datos SQLite
│-- logs/
│   │-- code_log.txt       # Archivo de registro
│-- README.md              # Documentación del proyecto
```

## Instrucciones de Ejecución
1. Instalar las dependencias necesarias:
   ```bash
   pip install pandas requests sqlite3
   ```
2. Ejecutar el script:
   ```bash
   python banks_project.py
   ```
3. Verificar los resultados:
   - Revisar `data/Largest_banks_data.csv`.
   - Consultar `database/Banks.db` para acceder a los datos almacenados.
   - Revisar `logs/code_log.txt` para verificar los registros de ejecución.

## Resultado Esperado
- El script extraerá, transformará y cargará los datos financieros en salidas estructuradas.
- Los datos estarán disponibles en formatos CSV y SQL para su posterior análisis.

