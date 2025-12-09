import os
import requests

# ==============================
# 🔐 Credenciales desde GitHub
# ==============================
TENANT_ID = os.environ["AZ_TENANT_ID"]
CLIENT_ID = os.environ["AZ_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZ_CLIENT_SECRET"]

# ==============================
# 🔧 CONFIG (AJUSTA ESTO)
# ==============================
SITE_DOMAIN = "analyticsadvancedconsulting.sharepoint.com"
SITE_PATH = "sites/Prueba"
FILE_NAME = "PE25_Ventanilla y proyectos_Resultados Power BI VF.xlsx"

# Carpeta "Documentos" en español → normalmente es "Shared Documents"
SHAREPOINT_FOLDER = "Shared Documents"

OUTPUT_PATH = "entrada/PE25_Ventanilla_Resultados.xlsx"
os.makedirs("entrada", exist_ok=True)

# ==============================
# 1️⃣ Obtener token
# ==============================
print("=== Obteniendo token ===")

token_resp = requests.post(
    f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
    data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default"
    }
)

token = token_resp.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# ==============================
# 2️⃣ Obtener SITE_ID
# ==============================
print("\n=== Obteniendo SITE_ID ===")

site_url = f"https://graph.microsoft.com/v1.0/sites/{SITE_DOMAIN}:/{SITE_PATH}?$select=id"
site_info = requests.get(site_url, headers=headers).json()

SITE_ID = site_info["id"]
print("SITE_ID:", SITE_ID)

# ==============================
# 3️⃣ Obtener DRIVE_ID
# ==============================
print("\n=== Obteniendo DRIVE_ID ===")

drives_url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives"
drives_info = requests.get(drives_url, headers=headers).json()

DRIVE_ID = None
for d in drives_info["value"]:
    if d["name"] in ["Documentos", "Shared Documents"]:
        DRIVE_ID = d["id"]
        break

print("DRIVE_ID:", DRIVE_ID)

# ==============================
# 4️⃣ Descargar archivo
# ==============================
print("\n=== Descargando archivo ===")

file_relative_path = f"{SHAREPOINT_FOLDER}/{FILE_NAME}"

download_url = (
    f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}"
    f"/drives/{DRIVE_ID}/root:/{file_relative_path}:/content"
)

resp = requests.get(download_url, headers=headers)

if resp.status_code != 200:
    print("❌ Error al descargar:", resp.text)
    raise SystemExit(1)

with open(OUTPUT_PATH, "wb") as f:
    f.write(resp.content)

print(f"✓ Archivo descargado correctamente en {OUTPUT_PATH}")


