import requests
from bs4 import BeautifulSoup
import os

# URL de la página principal
url = 'https://datosabiertos.bogota.gov.co/dataset/mpox-en-bogota-d-c'
response = requests.get(url)

# Parsear el contenido HTML
soup = BeautifulSoup(response.content, 'html.parser')

# Encuentra todos los enlaces que cumplen con las características específicas
links = soup.find_all('a', attrs={'class': 'btn btn-default background-blue block-right-width hidden-xs'})

# Buscar el enlace que contiene la URL del archivo CSV
for link in links:
    if 'Previsualización' in link.text:
        href = link.get('href')
        full_url = 'https://datosabiertos.bogota.gov.co' + href
        print('Enlace encontrado:', full_url)
        
        # Realizar la solicitud GET para descargar el archivo
        file_response = requests.get('https://datosabiertos.bogota.gov.co/dataset/584111e1-202c-4f17-8ad5-3ea890e5113c/resource/9ef92d4f-3d86-40cf-a1f3-220839dc2b9d/download/osb_enftransm_mpox_06092024.csv')

        # Comprobar que la solicitud fue exitosa
        if file_response.status_code == 200:
            # Guardar el archivo en el disco
            with open('osb_enftransm_mpox_06092024.csv', 'wb') as file:
                file.write(file_response.content)
            print('Archivo descargado y guardado como osb_enftransm_mpox_06092024.csv')
        else:
            print('Error al descargar el archivo. Código de estado:', file_response.status_code)
