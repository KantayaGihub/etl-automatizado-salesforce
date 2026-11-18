from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd
import yaml


REPO = Path(__file__).resolve().parents[1]


class FuentesCompartidasTests(unittest.TestCase):
    def test_ambos_workflows_procesan_cada_fuente_una_sola_vez(self):
        for archivo, job in (("etl.yml", "etl"), ("validar-etl.yml", "validar")):
            workflow = yaml.load((REPO / ".github/workflows" / archivo).read_text(), Loader=yaml.BaseLoader)
            comandos = [paso.get("run", "") for paso in workflow["jobs"][job]["steps"]]
            for carpeta, descarga in (("6. ficha_social", "01_descargar.py"),
                                      ("8. solicitud_matricula", "01_descargar.py")):
                with self.subTest(archivo=archivo, carpeta=carpeta):
                    for script in (descarga, "02_transformacion.py"):
                        self.assertEqual(sum(f"/{carpeta}/{script}" in cmd for cmd in comandos), 1)
                    self.assertFalse(any(f"/2025/{carpeta}/" in cmd for cmd in comandos))

    def test_salida_final_ignora_copias_2025_y_no_inventa_anio(self):
        for proceso, carpeta, nombre, extension in (
            ("6_ficha_social.py", "6.ficha_social", "Ficha_Social_v2", "xlsx"),
            ("8_solicitud_matricula.py", "8.solicitud_matricula", "Sol_Mtr_Deduplicado", "csv"),
        ):
            with self.subTest(proceso=proceso), tempfile.TemporaryDirectory() as tmp:
                trabajo = Path(tmp)
                for anio, valores in ((2025, ["copia-antigua", "sobrante"]), (2026, ["actual", "otro"])):
                    entrada = trabajo / f"data/processed/{anio}/{carpeta}/{nombre}.{extension}"
                    entrada.parent.mkdir(parents=True)
                    df = pd.DataFrame({"DATO": valores})
                    if extension == "xlsx":
                        df.to_excel(entrada, sheet_name=nombre, index=False)
                    else:
                        df.to_csv(entrada, index=False)
                script = REPO / "etl/procesos/consolidacion" / proceso
                subprocess.run([sys.executable, str(script)], cwd=trabajo, check=True, capture_output=True)
                salida = trabajo / f"data/consolidated/{carpeta.split('.', 1)[1]}/{nombre}.{extension}"
                resultado = pd.read_excel(salida) if extension == "xlsx" else pd.read_csv(salida)
                self.assertEqual(resultado["DATO"].tolist(), ["actual", "otro"])
                self.assertEqual(list(resultado.columns), ["DATO"])
                # Una copia antigua no debe servir de respaldo automático si falta la fuente actual.
                entrada.unlink()
                fallo = subprocess.run([sys.executable, str(script)], cwd=trabajo, capture_output=True)
                self.assertNotEqual(fallo.returncode, 0)


if __name__ == "__main__":
    unittest.main()
