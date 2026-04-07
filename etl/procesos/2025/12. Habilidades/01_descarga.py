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
    "PE25_Ventanilla y proyectos_Habilidades.xlsx"
)

RAW_DIR = Path("data/raw/2025/impacto_habilidades")
RAW_FILE_PATH = RAW_DIR / "pe25_ventanilla_proyectos_habilidades.xlsx"


def validar_configuracion() -> None:
    faltantes = []

    if not CLIENT_ID:
        faltantes.append("SHAREPOINT_CLIENT_ID")
    if not CLIENT_SECRET:
        faltantes.append("SHAREPOINT_CLIENT_SECRET")
    if not TENANT_ID:
        faltantes.append("SHAREPOINT_TENANT_ID")

    if faltantes:
        raise EnvironmentError(
            f"Faltan variables de entorno requeridas: {', '.join(faltantes)}"
        )


def get_access_token() -> str:
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    response = requests.post(token_url, data=data, timeout=30)
    response.raise_for_status()
    return response.json()["access_token"]


def get_site_id(access_token: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/sites/{HOSTNAME}:{SITE_PATH}"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()["id"]


def get_item_id_by_filename(access_token: str, site_id: str, file_name: str) -> str:
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


def download_file(file_name: str, output_path: Path) -> Path:
    access_token = get_access_token()
    site_id = get_site_id(access_token)
    item_id = get_item_id_by_filename(access_token, site_id, file_name)

    headers = {"Authorization": f"Bearer {access_token}"}
    download_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{item_id}/content"

    response = requests.get(
        download_url,
        headers=headers,
        timeout=120,
        allow_redirects=True,
    )
    response.raise_for_status()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response.content)

    return output_path


def main() -> None:
    print("=== DESCARGA IMPACTO HABILIDADES ===")
    validar_configuracion()

    output_path = download_file(FILE_NAME, RAW_FILE_PATH)

    print("✅ Archivo descargado correctamente")
    print(f"Ruta: {output_path.resolve()}")


if __name__ == "__main__":
    main()
