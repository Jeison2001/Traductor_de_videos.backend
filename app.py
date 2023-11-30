import time
from flask import Flask, request
import boto3
import psycopg2
from werkzeug.utils import secure_filename
from flask_cors import CORS
from decouple import config
import json

app = Flask(__name__)
CORS(app)  # Esto habilitará CORS para todas las rutas

# Configuración de AWS S3
s3 = boto3.client(
    's3',
    aws_access_key_id=config('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
)

# Configuración de PostgreSQL
conn = psycopg2.connect(
    host='databaseproyectoaws.cckcb0zakvdq.us-east-1.rds.amazonaws.com',
    database='databaseproyectoaws',
    user='postgres',
    password='databaseproyectoaws',
    port=5432  # El puerto por defecto de PostgreSQL
)

cur = conn.cursor()

# Función para generar el archivo SRT a partir del json de la transcripcion traducida

def process_translation_file(traduccion_file_name):
    s3 = boto3.client('s3', region_name='us-east-1')

    # Obtener el contenido del archivo JSON desde S3
    response = s3.get_object(Bucket='buckedproyectocloud', Key='traducciones/' + traduccion_file_name)
    translation_data = response['Body'].read().decode('utf-8')

    # Procesar el JSON para extraer la transcripción y su información
    translation_info = json.loads(translation_data)
    transcriptions = translation_info['results']['transcripciones']

    # Inicializar variables para los subtítulos
    subtitles = ""
    subtitle_counter = 1

    # Procesar cada elemento de la transcripción
    for transcription in transcriptions:
        text = transcription['transcripción']
        items = transcription['items']

        # Obtener los tiempos de inicio y finalización del texto
        start_time = float(items[0]['start_time'])
        end_time = float(items[-1]['end_time'])

        # Formatear tiempos en horas, minutos, segundos y milisegundos
        start_hours = int(start_time // 3600)
        start_minutes = int((start_time % 3600) // 60)
        start_seconds = int(start_time % 60)
        start_milliseconds = int((start_time - int(start_time)) * 1000)

        end_hours = int(end_time // 3600)
        end_minutes = int((end_time % 3600) // 60)
        end_seconds = int(end_time % 60)
        end_milliseconds = int((end_time - int(end_time)) * 1000)

        # Formatear texto para el subtítulo
        formatted_text = f"{text}\n"

        # Formatear tiempos para el subtítulo en el formato SRT
        formatted_time = f"{subtitle_counter}\n{start_hours:02d}:{start_minutes:02d}:{start_seconds:02d},{start_milliseconds:03d} --> {end_hours:02d}:{end_minutes:02d}:{end_seconds:02d},{end_milliseconds:03d}\n"

        # Agregar texto y tiempos al archivo de subtítulos
        subtitles += formatted_time + formatted_text + "\n"

        # Incrementar el contador de subtítulos
        subtitle_counter += 1

    
    # Guardar el archivo de subtítulos SRT en S3
    s3.put_object(Body=subtitles.encode('utf-8'), Bucket='buckedproyectocloud', Key='subtitulos/'+ traduccion_file_name[:-4]+'.srt')

    return 'https://buckedproyectocloud.s3.amazonaws.com/subtitulos/'+ traduccion_file_name[:-4]+'.srt'

# Función para transcribir el video
def transcribe_video(filename):
    transcribe = boto3.client('transcribe', region_name='us-east-1')

    job_name = f"transcripcion-job-{int(time.time())}"  # Nombre único con marca de tiempo
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={
            'MediaFileUri': 's3://buckedproyectocloud/uploads/' + filename
        },
        OutputBucketName='buckedproyectocloud',  # Mismo bucket
        OutputKey='transcripciones/' + filename[:-4] + '.json',  # Carpeta "transcripciones"
        LanguageCode='en-US'  # Asumiendo que el idioma original es español (puedes cambiar esto)
    )

    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break

    if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
        transcript_uri = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
        return transcript_uri
    else:
        return None

def fix_translation_file_format(translation_data):
    # Reemplazar las comillas incorrectas por las comillas estándar
    fixed_translation_data = translation_data.replace('»', '"').replace('„', '"').replace('«', '"')
    # Corregir el formato de las claves y la estructura del archivo de traducción
    fixed_translation_data = translation_data.replace('"transcripciones"', '"transcripts"')
    fixed_translation_data = translation_data.replace('"transcripción"', '"transcript"')
    fixed_translation_data = translation_data.replace('"alternativas"', '"alternatives"')
    fixed_translation_data = translation_data.replace('"contenido"', '"content"')
    fixed_translation_data = translation_data.replace('"puntuación"', '"punctuation"')
    fixed_translation_data = translation_data.replace('"pronunciación"', '"pronunciation"')
    # Corregir caracteres especiales que podrían haber sido mal interpretados
    fixed_translation_data = translation_data.replace('"}]} , {"', '"}], {"')
    fixed_translation_data = translation_data.replace('"}]}]}', '"}]}]')
    # Retornar el archivo corregido
    return fixed_translation_data
#funcion para traducir la transcripcion
def translate_transcript(transcript_name):
    translate = boto3.client('translate', region_name='us-east-1')

    # Obtener la transcripción desde S3
    response = s3.get_object(Bucket='buckedproyectocloud', Key='transcripciones/' + transcript_name)
    transcript_text = response['Body'].read().decode('utf-8')

    # Traducir a español
    translation = translate.translate_text(
        Text=transcript_text,
        SourceLanguageCode='en',
        TargetLanguageCode='es'
    )

    translated_text = translation['TranslatedText']

    # Corregir el formato del archivo de traducción
    fixed_translation = fix_translation_file_format(translated_text)

    # Guardar la traducción corregida en S3
    translated_key = 'traducciones/' + transcript_name[:-5] + '_traduccion.json'  # Cambiar el formato si es necesario
    s3.put_object(Body=fixed_translation.encode('utf-8'), Bucket='buckedproyectocloud', Key=translated_key)

    # Obtener la URL del archivo traducido en S3
    translated_url = f'https://buckedproyectocloud.s3.amazonaws.com/{translated_key}'

    return translated_url

# Crear la tabla 'Video' si no existe
try:
    cur.execute('CREATE TABLE IF NOT EXISTS Video (id SERIAL PRIMARY KEY, filename VARCHAR(255), urlfile VARCHAR(255), transcripcion VARCHAR(255), traduccion VARCHAR(255), subtitulos VARCHAR(255), video_procesado VARCHAR(255))')
    conn.commit()
except Exception as e:
    print(f'Error {e}')
    conn.rollback()

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return 'Falta el Video en la solicitud', 400

    file = request.files['file']
    filename = secure_filename(file.filename)
    
    # Guardar el Video en S3
    s3.upload_fileobj(
        file,
        'buckedproyectocloud',
        'uploads/' + filename,
        ExtraArgs={'ACL': 'public-read'}
    )

    urlfile = f'https://buckedproyectocloud.s3.amazonaws.com/uploads/{filename}'

    # Transcribir el audio del video con Amazon Transcribe
    transcripcion_url = transcribe_video(filename)  # Pasamos el nombre del archivo
    
    if transcripcion_url:
         # Obtener el nombre del archivo de la URL completa
        transcript_file_name = transcripcion_url.split('/')[-1]
        # Traducir la transcripción si existe
        traduccion_url  = translate_transcript(transcript_file_name)
        
        if traduccion_url:
            traduccion_file_name = traduccion_url.split('/')[-1] 
        
            # subtitulos_url = process_translation_file(traduccion_file_name)
            # print(subtitulos_url)
            # Guardar la URL de la transcripción y la traducción en la base de datos
            try:
                cur.execute('INSERT INTO Video (filename, urlfile, transcripcion, traduccion) VALUES (%s, %s, %s, %s)', (filename, urlfile, transcripcion_url, traduccion_url))
                conn.commit()
                return 'Video subido, transcripción y traducción y subtitulos registradas correctamente', 200
            except Exception as e:
                print(f'Error {e}')
                conn.rollback()
                return 'Error al guardar la transcripción, traducción, y subtitulos', 500
        else:
            return 'Error al obtener la traduccion', 500
    else:
        return 'Error al obtener la transcripción', 500




@app.route('/list', methods=['GET'])
def list_files():
    try:
        cur.execute('SELECT * FROM Video')
        rows = cur.fetchall()
        return {'files': rows}, 200
    except Exception as e:
        print(f'Error {e}')
        conn.rollback()
        return 'Error interno del servidor', 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)

