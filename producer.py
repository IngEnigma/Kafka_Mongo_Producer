from flask import Flask, jsonify
from confluent_kafka import Producer
import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv

app = Flask(__name__)

# Carga variables de entorno
load_dotenv()

# Configuración del productor para Redpanda Cloud
producer_conf = {
    'bootstrap.servers': os.getenv('KAFKA_BROKERS', 'cvq4abs3mareak309q80.any.us-west-2.mpx.prd.cloud.redpanda.com:9092'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': os.getenv('KAFKA_USER', 'IngEnigma'),
    'sasl.password': os.getenv('KAFKA_PASSWORD', 'BrARBOxX98VI4f2LIuIT1911NYGrXu'),
}

producer = Producer(producer_conf)

TOPIC = "crimes_mongo"
JSONL_URL = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"

def delivery_report(err, msg):
    if err:
        print(f'Error al enviar: {err}')
    else:
        print(f'Enviado: {msg.value().decode("utf-8")} a {msg.topic()}')

def transform_for_mongodb(data):
    """Transforma los datos para una estructura más óptima en MongoDB"""
    try:
        # Convertir a objeto datetime
        report_date = datetime.strptime(data['report_date'], '%m/%d/%Y %I:%M:%S %p')
        
        return {
            '_id': str(data['dr_no']),  # MongoDB usa _id como identificador único
            'crime_details': {
                'code_description': data['crm_cd_desc'],
                'report_date': report_date,
                'location': {  # Ejemplo de estructura embebida
                    'area': data.get('area', None),
                    'lat': data.get('lat', None),
                    'lon': data.get('lon', None)
                }
            },
            'victim': {
                'age': data['victim_age'],
                'sex': data['victim_sex'],
                'descent': data.get('victim_descent', 'U')  # U para desconocido
            },
            'metadata': {
                'source': 'LAPD',
                'imported_at': datetime.utcnow()
            }
        }
    except Exception as e:
        print(f"Error transformando datos: {e}")
        return None

@app.route('/send-crimes-mongodb', methods=['POST'])
def send_crimes_mongodb():
    try:
        print("Obteniendo datos desde:", JSONL_URL)
        response = requests.get(JSONL_URL)
        response.raise_for_status()
        
        records = response.text.strip().splitlines()
        print(f"Recibidos {len(records)} registros")

        success_count = 0
        for line in records:
            try:
                original_data = json.loads(line)
                mongo_data = transform_for_mongodb(original_data)
                
                if mongo_data:
                    producer.produce(
                        TOPIC, 
                        json.dumps(mongo_data, default=str).encode('utf-8'), 
                        callback=delivery_report
                    )
                    success_count += 1
            except Exception as e:
                print(f"Error procesando línea: {e}. Línea: {line}")

        producer.flush()
        print(f"Procesamiento completo. Éxitos: {success_count}, Fallos: {len(records)-success_count}")
        
        return jsonify({
            "status": "success",
            "message": f"Datos transformados enviados al tópico '{TOPIC}'",
            "stats": {
                "total": len(records),
                "success": success_count,
                "failed": len(records)-success_count
            }
        }), 200

    except Exception as e:
        print("Error:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
