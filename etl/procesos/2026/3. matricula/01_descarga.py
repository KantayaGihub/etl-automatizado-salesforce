import os
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession

print("=== Descargando Google Sheets como Excel ===")

# === CONFIGURACIÓN ===
FILE_ID = "1p2dOWaPmnr_zRrV2UMQj3hxNI1K-K9a0-e8K-GBb0Lg"
OUTPUT_PATH = "data/raw/2026/matricula/Consolidado_Matricula_AfterSchool.xlsx"

export_url = f"https://docs.google.com/spreadsheets/d/{FILE_ID}/export?format=xlsx"

# === AUTENTICACIÓN ===
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

creds = Credentials.from_service_account_info(
    eval(SERVICE_ACCOUNT_JSON),
    scopes=SCOPES
)

authed = AuthorizedSession(creds)

print("Autenticación correcta")

# === DESCARGA ===
response = authed.get(export_url)

if response.status_code != 200:
    print("Error al descargar el archivo:", response.text)
    raise SystemExit(1)

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

with open(OUTPUT_PATH, "wb") as f:
    f.write(response.content)

print(f"Archivo descargado correctamente → {OUTPUT_PATH}")

