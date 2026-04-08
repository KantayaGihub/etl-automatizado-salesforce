import os
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession

print("=== Descargando Google Sheets (Encuesta de Satisfacción Padres) ===")

# Archivos a descargar (RUTAS CORRECTAS)
FILES = [
    {
        "id": "1dioUznUOz8vrisBVB3S2r0IcglSBmycnLu44R6N7jro",
        "output": "data/raw/2025/5.encuesta_satisfaccion_padres/Encuesta_Inicial_1y2.xlsx"
    },
    {
        "id": "1WhFrCWPOzEHWsXSiriVDNS4VLgdIFT4GJHgikaCuML4",
        "output": "data/raw/2025/5.encuesta_satisfaccion_padres/Encuesta_3y4.xlsx"
    },
    {
        "id": "1DV2pXJTdBf-WucXlbY-6kK5YYj_U4wjOSaU85t8_cyI",
        "output": "data/raw/2025/5.encuesta_satisfaccion_padres/Encuesta_5y6.xlsx"
    }
]

# Autenticación
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

creds = Credentials.from_service_account_info(
    eval(SERVICE_ACCOUNT_JSON),
    scopes=SCOPES
)

authed = AuthorizedSession(creds)

# Crear carpeta correctamente
os.makedirs("data/raw/2025/5.encuesta_satisfaccion_padres", exist_ok=True)

# Descargar archivos
for file in FILES:
    file_id = file["id"]
    output_path = file["output"]

    export_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"

    print(f"\n→ Descargando archivo: {output_path}")

    response = authed.get(export_url)

    if response.status_code != 200:
        print(f"❌ Error al descargar {output_path}: {response.text}")
        raise SystemExit(1)

    with open(output_path, "wb") as f:
        f.write(response.content)

    print(f"✓ Archivo descargado correctamente → {output_path}")

print("\n🎉 Todos los archivos fueron descargados con éxito")
