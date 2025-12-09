import os
import requests

print("=== Descargando archivo desde SharePoint ===")

TENANT_ID = os.environ["AZ_TENANT_ID"]
CLIENT_ID = os.environ["AZ_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZ_CLIENT_SECRET"]

# NOMBRE EXACTO DEL ARCHIVO EN SHAREPOINT
FILE_NAME = "PE25_Ventanilla y proyectos_Resultados Power BI VF.xlsx"

# SALIDA LOCAL
OUTPUT_PATH = "entrada/PE25_Ventanilla_Resultados.xlsx"
os.makedirs("entrada", exist_ok=True)

# ============================================================
# 1) Obtener token
# ============================================================
token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

data = {
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": "https://graph.microsoft.com/.default",
    "grant_type": "client_credentials"
}

resp = requests.post(token_url, data=data)
resp.raise_for_status()
token = resp.json()["access_token"]

headers = {"Authorization": f"Bearer {token}"}

# ============================================================
# 2) SITE_ID y DRIVE_ID ya detectados
# ============================================================
SITE_ID = "analyticsadvancedconsulting.sharepoint.com,66aec61b-24b6-4190-ad48-bcf8d47c7825,15c34463-4775-42b4-9b39-cf5266896971"
DRIVE_ID = "b!G8auZrYkkEGtSLz41Hx4JWNEwxV1R7RCmznPUmaJaXF3zp1RuYlNSI5iAham6BG7"

# ============================================================
# 3) URL correcta de descarga App-Only
# ============================================================
download_url = (
    f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}"
    f"/drives/{DRIVE_ID}/root:/{FILE_NAME}:/content"
)

print("Descargando:", FILE_NAME)
resp = requests.get(download_url, headers=headers)

if resp.status_code != 200:
    print("❌ Error al descargar archivo:", resp.text)
    raise SystemExit(1)

with open(OUTPUT_PATH, "wb") as f:
    f.write(resp.content)

print(f"✓ Archivo descargado correctamente → {OUTPUT_PATH}")
