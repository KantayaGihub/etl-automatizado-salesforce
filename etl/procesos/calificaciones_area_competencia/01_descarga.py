import os
import requests

print("=== Descargando archivo desde OneDrive Personal ===")

TENANT_ID = os.environ["AZ_TENANT_ID"]
CLIENT_ID = os.environ["AZ_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZ_CLIENT_SECRET"]

# NOMBRE EXACTO DEL ARCHIVO EN ONEDRIVE PERSONAL
FILE_PATH = "PE25_Ventanilla y proyectos_Resultados Power BI VF.xlsx"

OUTPUT_PATH = "entrada/PE25_Ventanilla_Resultados.xlsx"
os.makedirs("entrada", exist_ok=True)

# 1. Obtener token
token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
token_data = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": "https://graph.microsoft.com/.default"
}
resp = requests.post(token_url, data=token_data)

if resp.status_code != 200:
    print("❌ Error obteniendo token:", resp.text)
    raise SystemExit(1)

token = resp.json()["access_token"]
print("✓ Token obtenido")

# 2. Descargar archivo
download_url = (
    "https://graph.microsoft.com/v1.0/me/drive/root:/"
    + FILE_PATH +
    ":/content"
)

headers = {
    "Authorization": f"Bearer {token}"
}

print(f"Descargando: {FILE_PATH}")

resp = requests.get(download_url, headers=headers)

if resp.status_code != 200:
    print("❌ Error al descargar archivo:", resp.text)
    raise SystemExit(1)

with open(OUTPUT_PATH, "wb") as f:
    f.write(resp.content)

print(f"✓ Archivo descargado correctamente en {OUTPUT_PATH}")

