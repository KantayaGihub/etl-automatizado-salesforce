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
    def test_encabezados_nuevos_y_originales_producen_la_misma_salida(self):
        modulo = cargar_modulo()
        original = pd.DataFrame({"DNI": ["00000001"],
                                 "Nivel de logro Base": ["En inicio"],
                                 "Nivel de logro Salida": ["Logrado"]})
        esperado = modulo.transformar_hoja(original, "Sociales")
        for base, salida in (("Nivel de logro Base (LB)", "Nivel de Logro Salida (LS)"),
                             (" NIVEL DE LOGRO BASE (LB) ", "Nivel  de logro Salida (LS)")):
            with self.subTest(base=base, salida=salida):
                nuevo = original.rename(columns={"Nivel de logro Base": base,
                                                 "Nivel de logro Salida": salida})
                resultado = modulo.transformar_hoja(nuevo, "Sociales")
                pd.testing.assert_frame_equal(resultado, esperado)
        self.assertEqual(esperado["Escenario"].tolist(), ["LB", "LS"])
        self.assertEqual(esperado["Nivel_de_logro__c"].tolist(), ["En inicio", "Logrado"])
        self.assertEqual(esperado["Evaluacion__c"].tolist(), ["2026-I", "2026-II"])

    def test_variantes_complementarias_no_duplican_evaluaciones(self):
        entrada = pd.DataFrame({"DNI": ["1", "2"],
                                "Nivel de logro Base": ["En inicio", None],
                                "Nivel de logro Base (LB)": [None, "Logrado"],
                                "Nivel de logro Salida": ["Logrado", "Sobresaliente"],
                                "Nivel de Logro Salida (LS)": ["Logrado", None]})
        resultado = cargar_modulo().transformar_hoja(entrada, "Sociales")
        self.assertEqual(len(resultado), 4)
        self.assertFalse(resultado.duplicated(["DNI__c", "Escenario"]).any())
        self.assertEqual(resultado["Nivel_de_logro__c"].tolist(),
                         ["En inicio", "Logrado", "Logrado", "Sobresaliente"])

    def test_variantes_con_valores_contradictorios_fallan(self):
        for destino, alias in (("Nivel de logro Base", "Nivel de logro Base (LB)"),
                               ("Nivel de logro Salida", "Nivel de Logro Salida (LS)")):
            with self.subTest(destino=destino), self.assertRaisesRegex(ValueError, "discrepan"):
                cargar_modulo().homologar_encabezados_logro(
                    pd.DataFrame({destino: ["En inicio"], alias: ["Logrado"]}))

    def test_excels_2025_y_2026_se_consolidan_con_los_mismos_campos(self):
        modulo = cargar_modulo()
        with tempfile.TemporaryDirectory() as tmp:
            trabajo = Path(tmp)
            for anio in (2025, 2026):
                entrada = trabajo / f"data/raw/{anio}/11.habilidades/impacto_habilidades_{anio}.xlsx"
                entrada.parent.mkdir(parents=True)
                base = "Nivel de logro Base" + (" (LB)" if anio == 2026 else "")
                salida = "Nivel de logro Salida" + (" (LS)" if anio == 2026 else "")
                with pd.ExcelWriter(entrada) as writer:
                    for hoja in modulo.SHEETS_MAP:
                        pd.DataFrame({"DNI": [str(anio)], base: ["En inicio"],
                                      salida: ["Logrado"]}).to_excel(writer, sheet_name=hoja, index=False)
                subprocess.run([sys.executable, str(REPO / f"etl/procesos/{anio}/11. Habilidades/02_transformacion.py")],
                               cwd=trabajo, check=True, capture_output=True)
            subprocess.run([sys.executable, str(REPO / "etl/procesos/consolidacion/11_habilidades.py")],
                           cwd=trabajo, check=True, capture_output=True)
            resultado = pd.read_csv(trabajo / "data/consolidated/habilidades/BD_impacto_habilidades.csv")
            self.assertEqual(len(resultado), 12)
            self.assertEqual(set(resultado["ANIO_FUENTE"]), {2025, 2026})
            self.assertEqual(set(resultado["Evaluacion__c"]), {"2025-I", "2025-II", "2026-I", "2026-II"})
            self.assertEqual(set(resultado["Nivel_de_logro__c"]), {"En inicio", "Logrado"})
            self.assertFalse(any("(LB)" in c or "(LS)" in c for c in resultado.columns))


if __name__ == "__main__":
    unittest.main()
