import os
from pathlib import Path
import runpy
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pandas as pd


REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "etl/procesos/carga/03_matricula.py"


class MatriculaDniUnicoTests(unittest.TestCase):
    def ejecutar(self, registros):
        with tempfile.TemporaryDirectory() as tmp:
            entrada = Path(tmp) / "data/consolidated/matricula/consolidado_matricula_afterschool.csv"
            entrada.parent.mkdir(parents=True)
            registros.to_csv(entrada, index=False)
            preparar = Mock()
            sf = Mock()
            anterior = Path.cwd()
            try:
                os.chdir(tmp)
                with patch.dict(sys.modules, {
                    "_carga_segura": SimpleNamespace(preparar=preparar),
                    "simple_salesforce": SimpleNamespace(Salesforce=Mock(return_value=sf)),
                }), patch.dict(os.environ, {"SF_USERNAME": "simulado", "SF_PASSWORD": "simulado",
                                           "SF_SECURITY_TOKEN": "simulado"}):
                    runpy.run_path(str(SCRIPT), run_name="__main__")
            finally:
                os.chdir(anterior)
            preparar.assert_called_once()
            self.assertEqual(preparar.call_args.args[1], "Matricula_e__c")
            resultado = preparar.call_args.args[2]
            archivo = pd.read_csv(entrada.parent / "matricula_final_para_salesforce.csv", dtype={"DNI__c": str})
            self.assertEqual(len(archivo), len(resultado))
            self.assertEqual(len(pd.read_csv(entrada)), len(registros))
            self.assertEqual(sf.mock_calls, [])
            return resultado, archivo

    def test_conserva_primera_fila_por_dni_aunque_otros_datos_sean_distintos(self):
        resultado, archivo = self.ejecutar(pd.DataFrame({
            "DNI": ["00000001", "00000002", "00000001", "00000002"],
            "GRADO": ["PRIMERO", "SEGUNDO", "SEGUNDO", "TERCERO"],
            "ANIO_FUENTE": [2025, 2025, 2026, 2026],
        }))
        self.assertEqual(resultado["DNI__c"].tolist(), ["00000001", "00000002"])
        self.assertEqual(resultado["GRADO__c"].tolist(), ["PRIMERO", "SEGUNDO"])
        self.assertEqual(archivo["DNI__c"].tolist(), ["00000001", "00000002"])

    def test_respeta_la_primera_fila_aunque_su_anio_sea_2026(self):
        resultado, _ = self.ejecutar(pd.DataFrame({
            "DNI": ["91250439", "91250439"],
            "GRADO": ["TERCERO", "SEGUNDO"], "ANIO_FUENTE": [2026, 2025],
        }))
        self.assertEqual(resultado["GRADO__c"].tolist(), ["TERCERO"])

    def test_no_colapsa_filas_sin_dni_y_compara_sin_espacios_exteriores(self):
        resultado, _ = self.ejecutar(pd.DataFrame({
            "DNI": [None, None, " 00000001 ", "00000001"],
            "GRADO": ["PRIMERO", "SEGUNDO", "TERCERO", "CUARTO"],
        }))
        self.assertEqual(resultado["GRADO__c"].tolist(), ["PRIMERO", "SEGUNDO", "TERCERO"])


if __name__ == "__main__":
    unittest.main()
