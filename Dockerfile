# Imagen base ligera con Python
FROM python:3.12-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Instala dependencias del sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# Copia el archivo de requisitos primero
COPY requirements.txt .

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements-mongodb.txt && \
    # Limpieza para reducir el tamaño de la imagen
    apt-get remove -y gcc python3-dev libffi-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Copia el resto de los archivos de la aplicación
COPY producer.py .

# Expone el puerto para Flask
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=3s \
    CMD curl -f http://localhost:5000/health || exit 1

# Comando para correr la app
CMD ["python", "producer.py"]
