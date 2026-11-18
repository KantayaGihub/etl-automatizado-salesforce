import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd


REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "etl/procesos/2026/3. matricula/02_transformacion.py"
CANONICO = "ESTADO FINAL MATRICULA"
ALIAS = "ESTADO FINAL MATRICULA DOCS"


def cargar_modulo():
    spec = importlib.util.spec_from_file_location("matricula_2026", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    # El script original crea su directorio de salida al importar.
    with patch.object(Path, "mkdir"):
        spec.loader.exec_module(modulo)
    return modulo


class MatriculaTests(unittest.TestCase):
    def test_lectura_excel_reconoce_ambos_encabezados(self):
        for encabezado in (CANONICO, ALIAS, " Estado final matrícula docs "):
            with self.subTest(encabezado=encabezado), tempfile.TemporaryDirectory() as tmp:
                entrada = Path(tmp) / "entrada.xlsx"
                pd.DataFrame({"DNI": ["00000001"], encabezado: [" COMPLETO "]}).to_excel(
                    entrada, sheet_name="01 Sede", startrow=4, startcol=2, index=False,
                )
                resultado = cargar_modulo().leer_tabla_C5(entrada, "01 Sede")
                self.assertEqual(resultado[CANONICO].tolist(), ["COMPLETO"])
                self.assertNotIn(ALIAS, resultado.columns)
                self.assertFalse(resultado.columns.duplicated().any())

    def test_ambas_columnas_complementarias_se_unen_sin_perder_filas(self):
        df = pd.DataFrame({CANONICO: ["COMPLETO", None, "PENDIENTE"],
                           ALIAS: [None, "COMPLETO", "PENDIENTE"]})
        resultado = cargar_modulo().homologar_estado_matricula(df)
        self.assertEqual(resultado[CANONICO].tolist(), ["COMPLETO", "COMPLETO", "PENDIENTE"])
        self.assertNotIn(ALIAS, resultado.columns)
        self.assertIn(ALIAS, df.columns)

    def test_estados_contradictorios_no_se_descartan_silenciosamente(self):
        df = pd.DataFrame({CANONICO: ["COMPLETO"], ALIAS: ["PENDIENTE"]})
        with self.assertRaisesRegex(ValueError, "discrepan"):
            cargar_modulo().homologar_estado_matricula(df)

    def test_transformaciones_2025_2026_consolidan_el_estado_en_una_sola_columna(self):
        with tempfile.TemporaryDirectory() as tmp:
            trabajo = Path(tmp)
            for anio, encabezado, estado in ((2025, CANONICO, "PENDIENTE"), (2026, ALIAS, "COMPLETO")):
                entrada = trabajo / f"data/raw/{anio}/matricula/Consolidado_Matricula_AfterSchool.xlsx"
                entrada.parent.mkdir(parents=True)
                pd.DataFrame({"DNI": [str(anio)], encabezado: [estado]}).to_excel(
                    entrada, sheet_name="01 Sede", startrow=4, startcol=2, index=False,
                )
                script = REPO / f"etl/procesos/{anio}/3. matricula/02_transformacion.py"
                subprocess.run([sys.executable, str(script)], cwd=trabajo, check=True, capture_output=True)
            subprocess.run([sys.executable, str(REPO / "etl/procesos/consolidacion/3_matricula.py")],
                           cwd=trabajo, check=True, capture_output=True)
            resultado = pd.read_csv(trabajo / "data/consolidated/matricula/consolidado_matricula_afterschool.csv")
            self.assertEqual(len(resultado), 2)
            self.assertNotIn(ALIAS, resultado.columns)
            self.assertEqual(resultado.set_index("ANIO_FUENTE")[CANONICO].to_dict(),
                             {2025: "PENDIENTE", 2026: "COMPLETO"})


if __name__ == "__main__":
    unittest.main()
