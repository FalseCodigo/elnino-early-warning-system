from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import pyarrow.dataset as ds
import fsspec
import threading
import time

app = FastAPI(title="API de Riesgo por Inundación - Guayaquil")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

HDFS_HOST = "localhost"
HDFS_PORT = 9870
HDFS_PATH = "/user/root/processed/riesgo_combinado"

# Caché en memoria para evitar bloqueos y acelerar la API
LATEST_DATA_CACHE = []

def fetch_data_from_hdfs_loop():
    global LATEST_DATA_CACHE
    while True:
        try:
            fs = fsspec.filesystem('webhdfs', host=HDFS_HOST, port=HDFS_PORT, user='root')
            
            # Buscar todos los archivos y filtrar los de 0 bytes
            files_info = fs.find(HDFS_PATH, detail=True)
            valid_files = [
                path for path, info in files_info.items() 
                if path.endswith('.parquet') and info.get('size', 0) > 0
            ]
            
            if not valid_files:
                time.sleep(5)
                continue
                
            dataset = ds.dataset(valid_files, filesystem=fs, format="parquet", partitioning="hive")
            df = dataset.to_table().to_pandas()
            
            if not df.empty:
                df = df.dropna(subset=['latitud', 'longitud'])
                df = df.sort_values(by="fecha_evento", ascending=False)
                latest = df.groupby("provincia").first().reset_index()
                
                results = []
                for _, row in latest.iterrows():
                    results.append({
                        "provincia": row["provincia"],
                        "latitud": float(row["latitud"]),
                        "longitud": float(row["longitud"]),
                        "precipitacion_mm": float(row["precipitacion_mm"]),
                        "nivel_alerta": row.get("nivel_alerta_lluvia", ""),
                        "nivel_marea_m": float(row.get("nivel_marea_m", 0.0)),
                        "estado_marea": row.get("estado_marea", "Normal"),
                        "riesgo_combinado": row.get("riesgo_combinado", ""),
                        "fecha_evento": row["fecha_evento"]
                    })
                LATEST_DATA_CACHE = results
        except Exception as e:
            print("Error leyendo HDFS en background:", e)
            
        time.sleep(10) # Actualizar cada 10 segundos

# Iniciar el hilo de fondo al cargar la app
threading.Thread(target=fetch_data_from_hdfs_loop, daemon=True).start()

@app.get("/api/clima")
def get_clima_data():
    if not LATEST_DATA_CACHE:
        return {"status": "error", "message": "Cargando datos desde HDFS, intenta en unos segundos...", "data": []}
    return {"status": "success", "data": LATEST_DATA_CACHE}

# Montamos la carpeta "static" en la raíz del servidor para servir el Dashboard Web.
# Esto asegura que al entrar a http://localhost:8000/ veamos el mapa.
app.mount("/", StaticFiles(directory="static", html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    # Lanzamos el servidor en el puerto 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)
