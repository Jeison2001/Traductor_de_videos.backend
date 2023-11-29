import time
from flask import Flask, request
import boto3
import psycopg2
from werkzeug.utils import secure_filename
from flask_cors import CORS
from decouple import config

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


# Función para traducir la transcripción a español
def translate_transcript(transcript_uri):
    translate = boto3.client('translate', region_name='us-east-1')

    # Obtener la transcripción en texto
    response = boto3.client('s3').get_object(Bucket='bucket-transcripcion', Key='nombre_transcripcion.json')
    transcript_text = response['Body'].read().decode('utf-8')

    # Traducir a español
    translation = translate.translate_text(
        Text=transcript_text,
        SourceLanguageCode='en',
        TargetLanguageCode='es'
    )

    translated_text = translation['TranslatedText']
    # Aquí puedes guardar la traducción en un archivo o base de datos si es necesario

    return translated_text

# Crear la tabla 'Video' si no existe
try:
    cur.execute('CREATE TABLE IF NOT EXISTS Video (id SERIAL PRIMARY KEY, filename VARCHAR(255), urlfile VARCHAR(255), transcripcion VARCHAR(255), traduccion VARCHAR(255), video_procesado VARCHAR(255))')
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

    # Guardar la URL de la transcripción en la base de datos
    try:
        cur.execute('INSERT INTO Video (filename, urlfile, transcripcion) VALUES (%s, %s, %s)', (filename, urlfile, transcripcion_url))
        conn.commit()
    except Exception as e:
        print(f'Error {e}')
        conn.rollback()

    return 'Video subido y registrado correctamente', 200




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

