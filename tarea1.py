import pyodbc
import pandas as pd
import requests
from bs4 import BeautifulSoup

# URL base de Datos Abiertos Bogotá
base_url = 'https://datosabiertos.bogota.gov.co'

# 1. Solicitar la página principal
response = requests.get(base_url)

# 2. Parsear el contenido HTML
soup = BeautifulSoup(response.content, 'html.parser')

# 3. Encontrar el enlace al botón "Dataset"
dataset_link = soup.find('a', id='datasets')

if dataset_link:
    dataset_href = dataset_link['href']
    full_dataset_url = base_url + dataset_href  # URL completa para acceder a los datasets
    print(f"Enlace al botón Dataset encontrado: {full_dataset_url}")
else:
    print("No se encontró el botón Dataset.")
    exit()

# 4. Hacer la solicitud a la página de "Dataset"
dataset_response = requests.get(full_dataset_url)
dataset_soup = BeautifulSoup(dataset_response.content, 'html.parser')

# 5. Realizar la búsqueda de 'mpox' en el campo de búsqueda
search_url = full_dataset_url + '?q=mpox'
search_response = requests.get(search_url)
search_soup = BeautifulSoup(search_response.content, 'html.parser')

# 6. Buscar el enlace específico "Mpox en Bogotá D.C."
mpox_dataset_link = search_soup.find('a', href=True, string=lambda text: text and 'mpox en bogotá d.c.' in text.lower())

if mpox_dataset_link:
    mpox_dataset_href = mpox_dataset_link['href']
    full_mpox_dataset_url = base_url + mpox_dataset_href  # URL completa del dataset de "mpox"
    print(f"Dataset relacionado con MPOX encontrado: {full_mpox_dataset_url}")
else:
    print("No se encontraron datasets relacionados con 'mpox en bogotá d.c.'.")
    exit()

# 7. Hacer la solicitud a la página del dataset de MPOX
mpox_dataset_response = requests.get(full_mpox_dataset_url)
mpox_dataset_soup = BeautifulSoup(mpox_dataset_response.content, 'html.parser')

# 8. Buscar el enlace de descarga del archivo CSV
download_links = mpox_dataset_soup.find_all('a', href=True)

# Filtrar los enlaces que contienen 'download' para encontrar los archivos descargables
for link in download_links:
    href = link['href']
    if 'download' in href and '.csv' in href:  # Buscamos específicamente archivos CSV
        full_download_url = href if href.startswith('http') else base_url + href
        print(f"Enlace de descarga del archivo CSV encontrado: {full_download_url}")

        # 9. Descargar el archivo CSV
        csv_response = requests.get(full_download_url)

        # Guardar el archivo si la solicitud fue exitosa
        if csv_response.status_code == 200:
            csv_file_path = 'archivo_mpox.csv'
            with open(csv_file_path, 'wb') as file:
                file.write(csv_response.content)
            print(f'Archivo CSV descargado y guardado como {csv_file_path}')
        else:
            print(f'Error al descargar el archivo. Código de estado: {csv_response.status_code}')
        break  # Salir del bucle una vez que se descarga el archivo
else:
    print("No se encontraron archivos CSV para descargar en el dataset.")

# 10. Cargar el archivo CSV en un DataFrame de pandas, especificando la codificación correcta y el delimitador
df = pd.read_csv(csv_file_path, encoding='latin1', delimiter=';')

# 11. Verificar las columnas originales
print("Columnas originales del archivo:", df.columns)

# 12. Renombrar las columnas si es necesario
df.columns = ['Fecha_notificacion', 'Fecha_inicio_Sintomas', 'Localidad', 'Edad', 'Sexo', 'Estado_del_Caso']

# 13. Verificar si las columnas fueron renombradas correctamente
print("Columnas después de limpiar:", df.columns)

# 14. Convertir columnas de fecha a datetime
df['Fecha_notificacion'] = pd.to_datetime(df['Fecha_notificacion'], errors='coerce')
df['Fecha_inicio_Sintomas'] = pd.to_datetime(df['Fecha_inicio_Sintomas'], errors='coerce')

# 15. Verificar si hay valores NaT (Not a Time) que podrían causar problemas al insertar
if df['Fecha_notificacion'].isnull().any() or df['Fecha_inicio_Sintomas'].isnull().any():
    print("Advertencia: Se encontraron fechas no válidas que se han convertido a NaT.")
    print(df[df['Fecha_notificacion'].isnull() | df['Fecha_inicio_Sintomas'].isnull()])

# 16. Conectar a SQL Server usando la base de datos 'master'
server = '.'  # o 'MIGUEL'
database_name = 'DatosMPOX'
conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=' + server + ';'
    'DATABASE=master;'  # Conectar a la base de datos master
    'Trusted_Connection=yes;'
)

# Crear conexión a la base de datos master
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# 17. Crear la base de datos si no existe
try:
    cursor.execute(f'''
        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{database_name}')
        BEGIN
            COMMIT TRANSACTION;  -- Finalizar cualquier transacción activa
            CREATE DATABASE {database_name}
        END
    ''')
    conn.commit()
    print(f'Base de datos "{database_name}" creada o ya existe.')
except Exception as e:
    print(f'Error al crear la base de datos: {e}')
finally:
    cursor.close()
    conn.close()  # Cerrar conexión a la base de datos master

# 18. Cambiar a la nueva base de datos
try:
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=' + server + ';'
        'DATABASE=' + database_name + ';'  # Cambiar a la nueva base de datos
        'Trusted_Connection=yes;'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # 19. Crear la tabla en SQL Server (si no existe)
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='mpox_cases' AND xtype='U')
        CREATE TABLE mpox_cases (
            Fecha_notificacion DATE,
            Fecha_inicio_sintomas DATE,
            Localidad NVARCHAR(255),
            Edad INT,
            Sexo NVARCHAR(10),
            Estado_del_Caso NVARCHAR(50)
        )
    ''')
    conn.commit()

    # 20. Insertar los datos del DataFrame en la tabla SQL, manejando errores de conversión
    for index, row in df.iterrows():
        try:
            cursor.execute('''
                INSERT INTO mpox_cases (Fecha_notificacion, Fecha_inicio_sintomas, Localidad, Edad, Sexo, Estado_del_Caso)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', row['Fecha_notificacion'], row['Fecha_inicio_Sintomas'], row['Localidad'], row['Edad'], row['Sexo'], row['Estado_del_Caso'])
        except Exception as e:
            print(f'Error al insertar la fila {index}: {e}')

    conn.commit()
    print("Datos insertados en la base de datos correctamente.")

except Exception as e:
    print(f'Error al cambiar a la base de datos o crear la tabla: {e}')
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
