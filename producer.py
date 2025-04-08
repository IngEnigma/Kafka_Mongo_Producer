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
    'bootstrap.servers': 'cvq4abs3mareak309q80.any.us-west-2.mpx.prd.cloud.redpanda.com:9092',
    'security.protocol': 'SASL_SSL',           
    'sasl.mechanism': 'SCRAM-SHA-256',         
    'sasl.username': 'IngEnigma',            
    'sasl.password': 'BrARBOxX98VI4f2LIuIT1911NYGrXu',          
}

producer = Producer(producer_conf)

TOPIC = "crimes_mongo"  # Cambiamos el tópico
JSONL_URL = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/crimes_by_area/data.jsonl"

def delivery_report(err, msg):
    if err:
        print(f'Error al enviar: {err}')
    else:
        print(f'Enviado: {msg.value().decode("utf-8")} a {msg.topic()}')

def transform_for_mongodb(data):
    """Transforma los datos de área para MongoDB"""
    try:
        return {
            '_id': f"area_{data['area']}",  # ID único para cada área
            'area_number': data['area'],
            'crime_count': data['crime_count'],
            'metadata': {
                'source': 'LAPD',
                'imported_at': datetime.utcnow(),
                'dataset': 'crimes_by_area'
            },
            'stats': {
                'ranking': None,  # Se puede calcular después
                'normalized_count': None  # Se puede calcular después
            }
        }
    except Exception as e:
        print(f"Error transformando datos: {e}")
        return None

@app.route('/send-area', methods=['POST'])
def send_area_stats():
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
            "message": f"Datos de áreas enviados al tópico '{TOPIC}'",
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
