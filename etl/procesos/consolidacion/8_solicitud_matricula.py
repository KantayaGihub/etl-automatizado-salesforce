from pathlib import Path
import pandas as pd

# ============================================================
# CONFIG
# ============================================================
ARCHIVO_FUENTE = Path("data/processed/2026/8.solicitud_matricula/Sol_Mtr_Deduplicado.csv")

CARPETA_SALIDA = Path("data/consolidated/solicitud_matricula")
ARCHIVO_SALIDA = CARPETA_SALIDA / "Sol_Mtr_Deduplicado.csv"

def leer_csv_seguro(ruta: Path) -> pd.DataFrame:
    if not ruta.exists():
        print(f"No se encontró la fuente compartida: {ruta}")
        return pd.DataFrame()

    print(f"Leyendo fuente compartida: {ruta}")
    df = pd.read_csv(
        ruta,
        dtype={
            "Número de Documento": str,
            "Número de Documento.1": str,
            "Número de celular": str,
        }
    )
    print(f"-> {len(df)} filas")
    return df


def main():
    print("=== CONSOLIDACIÓN SOLICITUD MATRÍCULA ===")

    consolidado = leer_csv_seguro(ARCHIVO_FUENTE)
    if consolidado.empty:
        raise ValueError("La fuente compartida no contiene registros para preparar.")

    print(f"Total preparado desde la fuente compartida: {len(consolidado)} filas")

    CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)
    consolidado.to_csv(ARCHIVO_SALIDA, index=False, encoding="utf-8-sig")

    print(f"Consolidado guardado en: {ARCHIVO_SALIDA}")
    print(f"Total final: {len(consolidado)} filas")


if __name__ == "__main__":
    main()
