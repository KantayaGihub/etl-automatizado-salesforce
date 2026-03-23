import os
import requests

print("=== Descargando Google Sheets como Excel ===")

# ID del archivo compartido con tu service account
FILE_ID = "19RR_orn9TGn0R5rfuWcvBqPJkAlENlbygq5AIAuhsBI"

# Ruta donde guardarás el Excel
OUTPUT_PATH = "entrada/Solicitud_matricula.xlsx"

# URL correcta para exportar Google Sheets a Excel
export_url = f"https://docs.google.com/spreadsheets/d/{FILE_ID}/export?format=xlsx"

# Autenticación con service account
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

creds = Credentials.from_service_account_info(
    eval(SERVICE_ACCOUNT_JSON),
    scopes=SCOPES
)

authed = AuthorizedSession(creds)

print("✓ Autenticación correcta")

# Descargar archivo
response = authed.get(export_url)

if response.status_code != 200:
    print("Error en descarga:", response.text)
    raise SystemExit(1)

os.makedirs("entrada", exist_ok=True)

with open(OUTPUT_PATH, "wb") as f:
    f.write(response.content)

print(f"✓ Archivo descargado correctamente → {OUTPUT_PATH}")

