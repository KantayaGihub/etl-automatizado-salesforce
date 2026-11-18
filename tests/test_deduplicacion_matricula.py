import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "etl/procesos/consolidacion/3_matricula.py"
SPEC = importlib.util.spec_from_file_location("consolidacion_matricula", SCRIPT)
modulo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(modulo)


class DeduplicacionMatriculaTests(unittest.TestCase):
    def test_prefiere_2025_aunque_2026_aparezca_primero(self):
        entrada = pd.DataFrame({"DNI": ["00000001"] * 3, "ESTADO": ["ACTIVO"] * 3,
                                "ANIO_FUENTE": ["2026", "2025", "2025"]})
        resultado = modulo.quitar_duplicados(entrada)
        self.assertEqual(resultado["ANIO_FUENTE"].tolist(), ["2025"])
        self.assertEqual(len(entrada), 3)

    def test_mismo_dni_con_otra_diferencia_no_se_elimina(self):
        entrada = pd.DataFrame({"DNI": ["00000001", "00000001"],
                                "ESTADO": ["ACTIVO", "INACTIVO"],
                                "ANIO_FUENTE": ["2025", "2026"]})
        resultado = modulo.quitar_duplicados(entrada)
        self.assertEqual(len(resultado), 2)
        self.assertEqual(resultado["ESTADO"].tolist(), ["ACTIVO", "INACTIVO"])

    def test_csv_final_deduplica_ambos_anios_y_funciona_con_solo_2026(self):
        for incluir_2025 in (True, False):
            with self.subTest(incluir_2025=incluir_2025), tempfile.TemporaryDirectory() as tmp:
                trabajo = Path(tmp)
                fuentes = {
                    2026: pd.DataFrame({"DNI": ["00000001", "00000002", "00000002"],
                                        "ESTADO": ["ACTIVO", "INACTIVO", "INACTIVO"],
                                        "OBSERVACION": [None, None, None]}),
                }
                if incluir_2025:
                    fuentes[2025] = pd.DataFrame({"DNI": ["00000001", "00000001"],
                                                 "ESTADO": ["ACTIVO", "ACTIVO"]})
                for anio, df in fuentes.items():
                    ruta = trabajo / f"data/processed/{anio}/matricula/consolidado_matricula_afterschool_{anio}_UNICO.csv"
                    ruta.parent.mkdir(parents=True)
                    df.to_csv(ruta, index=False)
                ejecucion = subprocess.run([sys.executable, str(SCRIPT)], cwd=trabajo,
                                           check=True, capture_output=True, text=True)
                salida = trabajo / "data/consolidated/matricula/consolidado_matricula_afterschool.csv"
                resultado = pd.read_csv(salida, dtype={"DNI": str})
                self.assertEqual(resultado["DNI"].tolist(), ["00000001", "00000002"])
                self.assertEqual(resultado["ANIO_FUENTE"].tolist(), [2025 if incluir_2025 else 2026, 2026])
                self.assertEqual(resultado["ESTADO"].tolist(), ["ACTIVO", "INACTIVO"])
                self.assertTrue(resultado["OBSERVACION"].isna().all())
                self.assertIn("Filas duplicadas eliminadas", ejecucion.stdout)


if __name__ == "__main__":
    unittest.main()
