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
    "Currícula - After School 2025.xlsx"
)

RAW_DIR = Path("data/raw/2025/7.progreso_curricular")
RAW_FILE_PATH = RAW_DIR / "curricula_after_school_2025.xlsx"


def get_token() -> str:
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def get_site_id(token: str) -> str:
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{HOSTNAME}:{SITE_PATH}"

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()["id"]


def get_item_id(token: str, site_id: str, filename: str) -> str:
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/search(q='{filename}')"

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    items = r.json().get("value", [])
    if not items:
        raise FileNotFoundError(f"No encontrado: {filename}")

    for item in items:
        if item["name"] == filename:
            return item["id"]

    return items[0]["id"]


def download_file(filename: str, output_path: Path) -> Path:
    token = get_token()
    site_id = get_site_id(token)
    item_id = get_item_id(token, site_id, filename)

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{item_id}/content"

    r = requests.get(url, headers=headers, timeout=120)
    r.raise_for_status()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(r.content)

    return output_path


def main() -> None:
    print("=== DESCARGA PROGRESO CURRICULAR ===")
    path = download_file(FILE_NAME, RAW_FILE_PATH)
    print("✅ Archivo descargado")
    print(path.resolve())


if __name__ == "__main__":
    main()
