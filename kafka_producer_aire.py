import time
import json
import csv
import random
from kafka import KafkaProducer

# ── Cargar el dataset real ────────────────────────────────────────────────────
print("Cargando dataset de Calidad del Aire...")
registros = []

with open('calidad_aire.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Solo tomamos registros de PM2.5 con promedio válido
        if row.get('Variable') == 'PM2.5' and row.get('Promedio'):
            try:
                registros.append({
                    "estacion":     row.get('Estación', ''),
                    "departamento": row.get('Nombre del Departamento', ''),
                    "variable":     row.get('Variable', ''),
                    "promedio":     float(row.get('Promedio', '0').replace(',', '')),
                    "excedencias":  int(row.get('Excedencias limite actual', 0) or 0),
                    "año":          int(row.get('Año', '0').replace(',', '')),
                    "timestamp":    int(time.time())
                })
            except:
                continue

print(f"Dataset cargado: {len(registros)} registros de PM2.5 disponibles")

# ── Producer Kafka ────────────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Iniciando envío de datos reales al topic calidad_aire_stream...")
indice = 0

while True:
    # Tomar registro real del dataset (en orden, con ciclo)
    registro = registros[indice % len(registros)].copy()
    registro["timestamp"] = int(time.time())  # timestamp actual

    producer.send('calidad_aire_stream', value=registro)
    print(f"Sent [{indice+1}]: {registro['estacion']} | "
          f"{registro['departamento']} | "
          f"PM2.5: {registro['promedio']} | "
          f"Año: {registro['año']}")

    indice += 1
    time.sleep(1)
