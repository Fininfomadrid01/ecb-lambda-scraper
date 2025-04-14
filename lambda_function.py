import os
import csv
import io
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import boto3

# Obtener nombres desde variables de entorno (se configurarán en la Lambda)
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME')

# Crear clientes de AWS fuera del handler para reutilizar conexiones si es posible
s3_client = boto3.client('s3')
dynamodb_resource = boto3.resource('dynamodb')

def scrape_ecb_rates():
    """
    Scrapea la tabla de tipos de cambio del sitio web del BCE.
    Retorna una lista de diccionarios, cada uno representando una fila de la tabla.
    """
    url = "https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() # Lanza excepción para errores HTTP
    except requests.RequestException as e:
        print(f"Error al obtener la página del BCE: {e}")
        raise

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', class_='forextable')

    if not table:
        print("No se encontró la tabla 'forextable' en la página.")
        return None, None

    # Extraer fecha de la cabecera (asumiendo que está en un h3 cerca de la tabla)
    # Esto puede necesitar ajuste si la estructura de la página cambia
    date_str = None
    header = soup.find('h3', id='fx-day')
    if header and header.string:
         # Ejemplo: "08 July 2024" -> Intentar parsear
         try:
             # El formato exacto puede variar, ajustar si es necesario
             extracted_date = datetime.strptime(header.string.strip(), '%d %B %Y')
             date_str = extracted_date.strftime('%Y-%m-%d')
         except ValueError as e:
             print(f"No se pudo parsear la fecha: {header.string.strip()}. Error: {e}")
             # Usar fecha actual como fallback o manejar el error de otra forma
             date_str = datetime.utcnow().strftime('%Y-%m-%d')
    else:
        print("No se encontró la cabecera h3 con id 'fx-day' para la fecha. Usando fecha actual.")
        date_str = datetime.utcnow().strftime('%Y-%m-%d')


    rates_data = []
    rows = table.find_all('tr')

    # Asumir que la primera fila son cabeceras (Moneda, Tasa)
    # y la última fila es un pie de tabla (ignorar)
    for row in rows[1:-1]: # Omitir cabecera y posible pie de tabla
        cells = row.find_all('td')
        if len(cells) >= 3: # Asegurar que hay suficientes celdas
            currency_cell = cells[0].find('a', class_='currency')
            rate_cell = cells[2].find('span', class_='rate')

            if currency_cell and rate_cell:
                currency = currency_cell.text.strip()
                rate_str = rate_cell.text.strip()
                try:
                    rate = float(rate_str)
                    rates_data.append({
                        'Currency': currency,
                        'Rate': rate
                    })
                except ValueError:
                    print(f"No se pudo convertir la tasa a float para {currency}: '{rate_str}'")
            else:
                 print(f"No se encontraron celdas de moneda o tasa en la fila: {row}")
        else:
            print(f"Fila ignorada por no tener suficientes celdas: {row}")


    print(f"Scrapeados {len(rates_data)} tipos de cambio para la fecha: {date_str}")
    return date_str, rates_data

def save_to_s3(date_str, rates_data):
    """
    Guarda los datos scrapeados en un archivo CSV en S3.
    El nombre del archivo incluirá la fecha.
    """
    if not S3_BUCKET_NAME:
        print("Error: Variable de entorno S3_BUCKET_NAME no definida.")
        return
    if not rates_data:
        print("No hay datos para guardar en S3.")
        return

    # Crear contenido CSV en memoria
    csv_buffer = io.StringIO()
    fieldnames = ['Currency', 'Rate']
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(rates_data)

    # Nombre del archivo en S3
    file_key = f"ecb_rates/{date_str}.csv"

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print(f"Datos guardados exitosamente en S3: s3://{S3_BUCKET_NAME}/{file_key}")
    except Exception as e:
        print(f"Error al guardar en S3: {e}")
        raise

def save_to_dynamodb(date_str, rates_data):
    """
    Guarda los datos scrapeados en la tabla DynamoDB.
    Cada tipo de cambio será un ítem separado con la fecha.
    """
    if not DYNAMODB_TABLE_NAME:
        print("Error: Variable de entorno DYNAMODB_TABLE_NAME no definida.")
        return
    if not rates_data:
        print("No hay datos para guardar en DynamoDB.")
        return

    table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)

    try:
        # Usar batch_writer para eficiencia al escribir múltiples ítems
        with table.batch_writer() as batch:
            for item in rates_data:
                # Asegurar que Rate es un tipo compatible con DynamoDB (Decimal o String)
                # Usamos String aquí por simplicidad, pero Decimal es mejor para cálculos
                rate_str = str(item['Rate'])
                batch.put_item(
                    Item={
                        'Date': date_str,         # Clave de Partición (o Ordenación)
                        'Currency': item['Currency'], # Clave de Ordenación (o Partición)
                        'Rate': rate_str          # Atributo
                    }
                )
        print(f"Datos guardados exitosamente en DynamoDB tabla: {DYNAMODB_TABLE_NAME}")
    except Exception as e:
        print(f"Error al guardar en DynamoDB: {e}")
        raise

def lambda_handler(event, context):
    """
    Punto de entrada de la función Lambda.
    """
    print("Iniciando scraping de tipos de cambio del BCE...")

    try:
        date_str, rates_data = scrape_ecb_rates()

        if date_str and rates_data:
            print(f"Se obtuvieron {len(rates_data)} registros para la fecha {date_str}.")
            # Guardar en S3
            save_to_s3(date_str, rates_data)
            # Guardar en DynamoDB
            save_to_dynamodb(date_str, rates_data)
        else:
            print("No se obtuvieron datos válidos del scraping.")
            return {
                'statusCode': 500,
                'body': 'Error durante el scraping, no se obtuvieron datos.'
            }

        print("Proceso completado exitosamente.")
        return {
            'statusCode': 200,
            'body': f'Datos de tipos de cambio para {date_str} guardados exitosamente en S3 y DynamoDB.'
        }

    except Exception as e:
        print(f"Error general en lambda_handler: {e}")
        # Considera enviar notificaciones aquí (e.g., SNS) si falla
        return {
            'statusCode': 500,
            'body': f'Error en la ejecución de la Lambda: {e}'
        }

# --- Para pruebas locales (opcional) ---
# if __name__ == '__main__':
#     # Simular variables de entorno para prueba local
#     os.environ['S3_BUCKET_NAME'] = 'tu-bucket-de-pruebas' # Reemplaza con un bucket real si pruebas S3
#     os.environ['DYNAMODB_TABLE_NAME'] = 'tu-tabla-de-pruebas' # Reemplaza con una tabla real si pruebas DynamoDB
#     print("Ejecutando prueba local...")
#     result = lambda_handler(None, None)
#     print(result)
