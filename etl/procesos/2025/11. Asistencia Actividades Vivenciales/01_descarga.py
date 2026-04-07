import os
from pathlib import Path
import requests
from dotenv import load_dotenv


load_dotenv()

CLIENT_SECRET = "W~j8Q~rylnObnr-6g8l7YCBXlwfkABQowMLB.a-7"
CLIENT_ID = "338d0875-39c7-44fe-8f78-db08df73b6f0"
TENANT_ID = "0324f870-1d9c-40a6-9a4c-e1e1a1464f9d"

HOSTNAME = os.getenv("SHAREPOINT_HOSTNAME", "kantaya.sharepoint.com")
SITE_PATH = os.getenv("SHAREPOINT_SITE_PATH", "/sites/EducativoKantaya")

FILE_NAME = os.getenv(
    "SHAREPOINT_FILE_NAME",
    "01 CRONOGRAMA DE VOLUNTARIADO 2025.xlsx"
)

RAW_DIR = Path("data/raw/2025/asistencia_actividades_vivenciales")
RAW_FILE_PATH = RAW_DIR / "cronograma_actividades_vivenciales_2025.xlsx"


def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    response = requests.post(url, data=data, timeout=30)
    response.raise_for_status()
    return response.json()["access_token"]


def get_site_id(access_token):
    url = f"https://graph.microsoft.com/v1.0/sites/{HOSTNAME}:{SITE_PATH}"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()["id"]


def get_item_id_by_filename(access_token, site_id, file_name):
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/search(q='{file_name}')"

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    results = response.json().get("value", [])
    if not results:
        raise FileNotFoundError(f"No se encontró el archivo: {file_name}")

    exact_match = next((item for item in results if item.get("name") == file_name), None)
    selected = exact_match or results[0]

    return selected["id"]


def download_file(file_name, output_path):
    access_token = get_access_token()
    site_id = get_site_id(access_token)
    item_id = get_item_id_by_filename(access_token, site_id, file_name)

    headers = {"Authorization": f"Bearer {access_token}"}
    download_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{item_id}/content"

    response = requests.get(download_url, headers=headers, timeout=120)
    response.raise_for_status()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response.content)

    return output_path


def main():
    print("=== DESCARGA ASISTENCIA ACTIVIDADES VIVENCIALES ===")

    path = download_file(FILE_NAME, RAW_FILE_PATH)

    print("✅ Archivo descargado")
    print(path.resolve())


if __name__ == "__main__":
    main()
