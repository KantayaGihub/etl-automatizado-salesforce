import contextlib
import importlib.util
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "etl/procesos/2026/7. progreso_curricular/02_transformacion.py"


def cargar_modulo():
    spec = importlib.util.spec_from_file_location("progreso_2026", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


class ProgresoCurricularTests(unittest.TestCase):
    def test_acepta_pachacutec_con_y_sin_tilde_sin_avisar_que_falta(self):
        for nombre in ("01 Pachacutec", "01 Pachacútec"):
            with self.subTest(nombre=nombre), tempfile.TemporaryDirectory() as tmp:
                archivo = Path(tmp) / "entrada.xlsx"
                pd.DataFrame({"ID": [1], "Tema": ["Matemática"], "Capacidad": ["C1"]}).to_excel(
                    archivo, sheet_name=nombre, index=False,
                )
                modulo = cargar_modulo()
                modulo.INPUT_FILE = archivo
                salida = io.StringIO()
                with contextlib.redirect_stdout(salida):
                    resultado = modulo.transformar()
                self.assertEqual(resultado["TEMA"].tolist(), ["Matemática"])
                self.assertEqual(resultado["SEDE"].tolist(), ["01 PACHACUTEC"])
                self.assertNotIn("CAPACIDAD", resultado.columns)
                self.assertNotIn("01 Pachacutec", salida.getvalue())

    def test_sigue_ignorando_hojas_fuera_de_la_lista(self):
        with tempfile.TemporaryDirectory() as tmp:
            archivo = Path(tmp) / "entrada.xlsx"
            with pd.ExcelWriter(archivo) as writer:
                pd.DataFrame({"ID": [1]}).to_excel(writer, sheet_name="02 HDMP", index=False)
                pd.DataFrame({"ID": [999]}).to_excel(writer, sheet_name="Competencias", index=False)
            modulo = cargar_modulo()
            modulo.INPUT_FILE = archivo
            with contextlib.redirect_stdout(io.StringIO()):
                resultado = modulo.transformar()
            self.assertEqual(resultado["ID"].tolist(), [1])
            self.assertEqual(resultado["SEDE"].tolist(), ["02 HDMP"])

    def test_genera_csv_de_ambos_anios_y_los_consolida_sin_cargas(self):
        with tempfile.TemporaryDirectory() as tmp:
            trabajo = Path(tmp)
            for anio, hoja in ((2025, "02 HDMP"), (2026, "01 Pachacútec")):
                entrada = trabajo / f"data/raw/{anio}/7.progreso_curricular/curricula_after_school_{anio}.xlsx"
                entrada.parent.mkdir(parents=True)
                pd.DataFrame({"ID": [anio], "Tema": [f"Tema {anio}"]}).to_excel(
                    entrada, sheet_name=hoja, index=False,
                )
                script = REPO / f"etl/procesos/{anio}/7. progreso_curricular/02_transformacion.py"
                subprocess.run([sys.executable, str(script)], cwd=trabajo, check=True, capture_output=True)
                procesado = trabajo / f"data/processed/{anio}/7.progreso_curricular/BD_Curricula_Consolidada.csv"
                self.assertEqual(pd.read_csv(procesado)["ID"].tolist(), [anio])

            script = REPO / "etl/procesos/consolidacion/7_progreso_curricular.py"
            subprocess.run([sys.executable, str(script)], cwd=trabajo, check=True, capture_output=True)
            consolidado = pd.read_csv(trabajo / "data/consolidated/progreso_curricular/BD_Curricula_Consolidada.csv")
            self.assertEqual(len(consolidado), 2)
            self.assertEqual(set(consolidado["ANIO_FUENTE"]), {2025, 2026})
            self.assertEqual(set(consolidado["TEMA"]), {"Tema 2025", "Tema 2026"})


if __name__ == "__main__":
    unittest.main()
