import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "etl/procesos/2026/11. Habilidades/02_transformacion.py"


def cargar_modulo():
    spec = importlib.util.spec_from_file_location("habilidades_2026", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


class HabilidadesTests(unittest.TestCase):
    def test_encabezados_anteriores_nuevos_y_mixtos_conservan_base_y_salida(self):
        variantes = [
            ("Nivel de logro Base", "Nivel de logro Salida"),
            ("Nivel de logro Base (LB)", "Nivel de Logro Salida (LS)"),
            ("Nivel de logro Base", "Nivel de Logro Salida (LS)"),
            ("Nivel de logro Base (LB)", "Nivel de logro Salida"),
        ]
        for base, salida in variantes:
            with self.subTest(base=base, salida=salida):
                df = pd.DataFrame({"DNI": ["00000001"], base: ["En inicio"], salida: ["Logrado"]})
                resultado = cargar_modulo().transformar_hoja(df, "Autogestión").set_index("Escenario")
                self.assertEqual(len(resultado), 2)
                self.assertEqual(resultado.loc["LB", "Nivel_de_logro__c"], "En inicio")
                self.assertEqual(resultado.loc["LS", "Nivel_de_logro__c"], "Logrado")
                self.assertEqual(resultado.loc["LB", "Nivel_logroOrden"], 1)
                self.assertEqual(resultado.loc["LS", "Nivel_logroOrden"], 3)
                self.assertEqual(resultado.loc["LB", "Evaluacion__c"], "2026-I")
                self.assertEqual(resultado.loc["LS", "Evaluacion__c"], "2026-II")
                self.assertTrue((resultado["DNI__c"] == "00000001").all())

    def test_excel_con_los_nuevos_encabezados_genera_el_csv_de_las_tres_hojas(self):
        with tempfile.TemporaryDirectory() as tmp:
            trabajo = Path(tmp)
            entrada = trabajo / "data/raw/2026/11.habilidades/impacto_habilidades_2026.xlsx"
            entrada.parent.mkdir(parents=True)
            modulo = cargar_modulo()
            with pd.ExcelWriter(entrada) as writer:
                for hoja in modulo.SHEETS_MAP:
                    pd.DataFrame({
                        "DNI": ["00000001"],
                        "Nivel de logro Base (LB)": ["En inicio"],
                        "Nivel de Logro Salida (LS)": ["Logrado"],
                    }).to_excel(writer, sheet_name=hoja, index=False)
            subprocess.run([sys.executable, str(SCRIPT)], cwd=trabajo, check=True, capture_output=True)
            resultado = pd.read_csv(trabajo / "data/processed/2026/11.habilidades/BD_impacto_habilidades.csv")
            self.assertEqual(len(resultado), 6)
            self.assertEqual(set(resultado["Habilidades__c"]), set(modulo.SHEETS_MAP.values()))
            self.assertEqual(resultado["Escenario"].value_counts().to_dict(), {"LB": 3, "LS": 3})
            self.assertEqual(set(resultado["Evaluacion__c"]), {"2026-I", "2026-II"})


if __name__ == "__main__":
    unittest.main()
