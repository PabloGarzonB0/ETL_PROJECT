# **Proyecto de Ingenieria de Datos: Extraccion y almacenamiento del PIB Mundial**
## **Descripcion del proyecto**
Una empresa internacional busca expandir su negocio en diferentes paises y requiere automatizar la extraccion de informacion económica. En este proyecto, se desarrollará un proceso de ETL
para obtener la lista de paises con un PIB superior, redondeado a dos decimales, según los datos proporcionados por el Fondo Monetario Internacional (FMI)

## **Fuente de Datos**
Los datos se eztraen de la siguiente fuente:\
🔗 [Lista de países por PIB (nominal)](https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29)
## **Objetivo del proyecto**
1. Extraer la informacion de PIB de los paises a partir de la fuente mencionada
2. Transformar los datos al formato solicitado
3. Cargar la informacion en tres formatos accesibles:
   - Json
   - DataFrame en memoria
   - Base de datos SQLite (`World_Economies.db`) con la creacion de la tabla **Economies** y los atributos
        - `Country` (Nombre del país).
        - `GDP_USD_billion` (PIB en miles de millones de USD).
4. Realizar una consulta a la base de datos para filtrar y mostrar solo los paises con un PIB superior a 100 mil millones de USD
5. Registar todas las operaciones y archivos generados en un archivo de log (`etl_project_log.txt`)

## **Estructura del proyecto**

``` bash
📂 proyecto_etl
│── 📄 etl_script.py              # Script principal de extracción, transformación y carga
│── 📄 requirements.txt           # Dependencias del proyecto
│── 📄 README.md                  # Documentación del proyecto
│── 📂 data
│   ├── 🌍 world_economies.json    # Archivo JSON con los datos del PIB
│   ├── 🗄️ World_Economies.db      # Base de datos SQLite
│── 📂 logs
│   ├── 📝 etl_project_log.txt     # Archivo de registro de operaciones

```

## **Requisitos Previos**
Antes de ejecutar el proyecto, se asegura tener instaladas las siguientes dependencias
``` bash
  pip install pandas requests beautifulsoup4 sqlite3
```
## **Ejecucion del Proyecto**
Para ejecutar el pipeline de ETL, simplemente se corre el siguiente comando de la terminal
``` bash
 python etl_script.py
```
