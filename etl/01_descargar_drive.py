import os
from pydrive2.auth import ServiceAccountCredentials
from pydrive2.drive import GoogleDrive

print("=== Iniciando descarga desde Google Drive ===")

# 1. LEER EL JSON DEL SECRET
service_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
if not service_json:
    raise ValueError("❌ No se encontró GOOGLE_SERVICE_ACCOUNT_JSON en los Secrets.")

# 2. GUARDAR JSON A ARCHIVO
with open("service_account.json", "w") as f:
    f.write(service_json)

# 3. AUTENTICACIÓN
scopes = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scopes=scopes)
drive = GoogleDrive(creds)

print("✔ Autenticación correcta")

# 4. ID DEL ARCHIVO GOOGLE SHEETS
FILE_ID = "1jx7eXk_lPHiNGrmLwkWmd8-4wEVTkPeZ"

# 5. DESCARGAR COMO EXCEL (.xlsx)
file = drive.CreateFile({'id': FILE_ID})
export_mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

output_path = "data/Ficha_Social_respuestas.xlsx"
os.makedirs("data", exist_ok=True)

print("⬇ Exportando Google Sheets como Excel (.xlsx)...")
file.GetContentFile(output_path, mimetype=export_mime)

print(f"🎉 Descarga completada: {output_path}")

