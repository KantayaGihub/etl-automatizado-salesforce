import os
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession

print("=== Descargando Google Sheets como Excel ===")

# === CONFIGURACIÓN ===
# ID del archivo Google Sheets
FILE_ID = "1vB4iVs6BIjrtiAWtDc2MrSnifi3D2dw0eovyZ9uqDzY"

# Ruta donde se guardará el archivo
OUTPUT_PATH = "entrada/Consolidado_Matricula_AfterSchool.xlsx"

# URL de exportación correcta
export_url = f"https://docs.google.com/spreadsheets/d/{FILE_ID}/export?format=xlsx"

# === AUTENTICACIÓN ===
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

creds = Credentials.from_service_account_info(
    eval(SERVICE_ACCOUNT_JSON),
    scopes=SCOPES
)

authed = AuthorizedSession(creds)

print("✓ Autenticación correcta")

# === DESCARGA ===
response = authed.get(export_url)

if response.status_code != 200:
    print("❌ Error al descargar el archivo:", response.text)
    raise SystemExit(1)

os.makedirs("entrada", exist_ok=True)

with open(OUTPUT_PATH, "wb") as f:
    f.write(response.content)

print(f"✓ Archivo descargado correctamente → {OUTPUT_PATH}")

