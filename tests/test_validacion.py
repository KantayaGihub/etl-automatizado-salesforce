import importlib.util
import json
from pathlib import Path
import shlex
import tempfile
import unittest

import pandas as pd
import yaml


REPO = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("resumir", REPO / "scripts/resumir_validacion.py")
resumir = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(resumir)


class ValidacionTests(unittest.TestCase):
    def test_workflow_solo_ejecuta_las_etapas_previas_a_salesforce(self):
        original = yaml.load((REPO / ".github/workflows/etl.yml").read_text(), Loader=yaml.BaseLoader)
        validacion = yaml.load((REPO / ".github/workflows/validar-etl.yml").read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(validacion["on"]["push"]["branches"], ["Fixing-etl-rc"])
        self.assertEqual(validacion["permissions"], {"contents": "read"})
        esperados = [s["run"] for s in original["jobs"]["etl"]["steps"]
                     if s.get("run", "").startswith('python "etl/procesos/')
                     and "/carga/" not in s["run"]]
        pasos = validacion["jobs"]["validar"]["steps"]
        reales = [s for s in pasos if s.get("id", "").startswith("paso_")]
        self.assertEqual(len(reales), 41)
        self.assertEqual([s["run"] for s in reales], esperados)
        self.assertNotIn("SF_", str(validacion))
        for paso in reales:
            self.assertEqual(paso["continue-on-error"], "true")
            argumentos = shlex.split(paso["run"])
            self.assertEqual(len(argumentos), 2)
            archivo = Path(argumentos[1])
            self.assertIn(archivo.parts[2], ("2025", "2026", "consolidacion"))
            self.assertTrue((REPO / archivo).is_file())
        for paso in pasos:
            if paso.get("id") == "resumen" or paso.get("uses", "").startswith("actions/upload-artifact"):
                self.assertIn("always()", paso["if"])

    def test_informe_con_fallo_conserva_los_resultados_parciales(self):
        with tempfile.TemporaryDirectory() as tmp:
            raiz = Path(tmp)
            data = raiz / "data/consolidated/progreso_curricular"
            data.mkdir(parents=True)
            pd.DataFrame({"ID": [1, 2]}).to_csv(data / "correcto.csv", index=False)
            pd.DataFrame(columns=["ID"]).to_csv(data / "sin_filas.csv", index=False)
            (data / "ilegible.xlsx").write_bytes(b"no es un excel")
            error = resumir.generar_resumen(raiz, {
                "paso_01": {"outcome": "failure"}, "paso_02": {"outcome": "success"},
            })
            self.assertTrue(error)
            informe = json.loads((raiz / "resultados-validacion/resumen.json").read_text())
            self.assertEqual(informe["pasos_no_exitosos"], ["paso_01"])
            archivos = {Path(a["archivo"]).name: a for a in informe["archivos"]}
            self.assertEqual(archivos["correcto.csv"]["filas"], 2)
            self.assertEqual(archivos["sin_filas.csv"]["error"], "Sin registros")
            self.assertIsNotNone(archivos["ilegible.xlsx"]["error"])
            self.assertTrue((data / "correcto.csv").exists())
            self.assertTrue((raiz / "resultados-validacion/resumen.md").exists())

    def test_sin_archivos_no_se_reporta_como_validado(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertTrue(resumir.generar_resumen(Path(tmp), {}))


if __name__ == "__main__":
    unittest.main()
