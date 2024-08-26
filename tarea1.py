import requests
import pandas as pd

# Reemplaza esto con el enlace directo al archivo CSV de la página de datos abiertos
url = "https://datosabiertos.bogota.gov.co/dataset/584111e1-202c-4f17-8ad5-3ea890e5113c/resource/9ef92d4f-3d86-40cf-a1f3-220839dc2b9d/download/osb_enftransm_mpox23082024.csv"

# Realizar la solicitud GET para obtener los datos
response = requests.get(url)

if response.status_code == 200:
    # Guardar el contenido de la respuesta en un archivo CSV temporal
    with open("mpox_bogota.csv", "wb") as file:
        file.write(response.content)

    # Leer los datos en un DataFrame de pandas desde el archivo temporal con codificación latin1
    data = pd.read_csv("mpox_bogota.csv", encoding='latin1')

    # Guardar los datos en un archivo Excel
    data.to_excel("mpox_en_bogota.xlsx", index=False)

    print("Datos guardados exitosamente en 'mpox_en_bogota.xlsx'")
else:
    print(f"Error al obtener los datos: {response.status_code}")
