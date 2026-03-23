import os
import io
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# === CONFIGURACIÓN ===
FOLDER_ID = "1dbEXE0clQfHiNVtfhUalUeHdVOu_Zb-1"  
OUTPUT_DIR = "entrada/asistencias_extracurriculares"

os.makedirs(OUTPUT_DIR, exist_ok=True)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

print("=== Autenticación con Service Account ===")

creds = Credentials.from_service_account_info(
    eval(SERVICE_ACCOUNT_JSON),
    scopes=SCOPES
)

service = build("drive", "v3", credentials=creds)


# ================================
# 1️⃣ LISTAR ARCHIVOS DENTRO DE LA CARPETA
# ================================
print("=== Buscando archivos XLSX dentro de la carpeta ===")

query = f"'{FOLDER_ID}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"

results = service.files().list(
    q=query,
    fields="files(id, name)"
).execute()

files = results.get("files", [])

print(f"→ Se encontraron {len(files)} archivos .xlsx")

if len(files) == 0:
    print("⚠ No se encontraron archivos XLSX en la carpeta. Verifica permisos o IDs.")
    exit()


# ================================
# 2️⃣ DESCARGAR CADA ARCHIVO
# ================================
for file in files:
    file_id = file["id"]
    file_name = file["name"]
    local_path = os.path.join(OUTPUT_DIR, file_name)

    print(f"\n📥 Descargando: {file_name}")

    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(local_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"  Progreso: {int(status.progress() * 100)}%")

    print(f"  ✔ Archivo guardado en: {local_path}")

print("\n🎉 DESCARGA COMPLETA: Todos los archivos fueron descargados exitosamente.")

