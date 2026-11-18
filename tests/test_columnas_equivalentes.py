import importlib.util
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
CARGA = REPO / "etl/procesos/carga"
SPEC = importlib.util.spec_from_file_location("homologacion", CARGA / "_homologacion_columnas.py")
homologacion = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(homologacion)


class ColumnasEquivalentesTests(unittest.TestCase):
    def test_unifica_complementarias_iguales_y_vacias(self):
        entrada = pd.DataFrame([[1, None], [None, 2], [3, 3], [" ", 4], [None, None]],
                               columns=["GRADOS__c", "GRADOS__c"])
        resultado = homologacion.unificar_columnas_equivalentes(entrada)
        self.assertEqual(list(resultado.columns), ["GRADOS__c"])
        self.assertEqual(resultado.iloc[:4, 0].tolist(), [1, 2, 3, 4])
        self.assertTrue(pd.isna(resultado.iloc[4, 0]))
        self.assertEqual(entrada.shape, (5, 2))

    def test_rechaza_conflictos_sin_mostrar_datos(self):
        entrada = pd.DataFrame([["privado1", "privado2"]], columns=["GRADOS__c", "GRADOS__c"])
        with self.assertRaisesRegex(ValueError, "GRADOS__c.*discrepan en 1 filas") as error:
            homologacion.unificar_columnas_equivalentes(entrada)
        self.assertNotIn("privado", str(error.exception))

    def test_script_real_unifica_grados_areas_y_temas_antes_de_preparar(self):
        with tempfile.TemporaryDirectory() as tmp:
            entrada = Path(tmp) / "data/consolidated/progreso_curricular/BD_Curricula_Consolidada.csv"
            entrada.parent.mkdir(parents=True)
            pd.DataFrame({"GRADO": [1, None, 3], "GRADOS": [None, 2, 3],
                          "AREA": ["A", None, "C"], "AREAS": [None, "B", "C"],
                          "TEMA": ["T1", None, "T3"], "TEMAS": [None, "T2", "T3"],
                          "ANIO_FUENTE": [2025, 2026, 2026]}).to_csv(entrada, index=False)
            preparar = Mock()
            sf = Mock()
            anterior = Path.cwd()
            try:
                os.chdir(tmp)
                with patch.dict(sys.modules, {
                    "_homologacion_columnas": homologacion,
                    "_carga_segura": SimpleNamespace(preparar=preparar),
                    "simple_salesforce": SimpleNamespace(Salesforce=Mock(return_value=sf)),
                }), patch.dict(os.environ, {"SF_USERNAME": "simulado", "SF_PASSWORD": "simulado",
                                           "SF_SECURITY_TOKEN": "simulado"}):
                    runpy.run_path(str(CARGA / "07_progreso_curricular.py"), run_name="__main__")
            finally:
                os.chdir(anterior)
            preparar.assert_called_once()
            resultado = preparar.call_args.args[2]
            self.assertFalse(resultado.columns.duplicated().any())
            self.assertEqual(resultado["GRADOS__c"].tolist(), [1, 2, 3])
            self.assertEqual(resultado["AREA__c"].tolist(), ["A", "B", "C"])
            self.assertEqual(resultado["TEMAS__c"].tolist(), ["T1", "T2", "T3"])
            self.assertEqual(resultado["ANIO_FUENTE__c"].tolist(), [2025, 2026, 2026])
            self.assertEqual(sf.mock_calls, [])
            debug = pd.read_csv(entrada.parent / "DEBUG_para_salesforce.csv")
            self.assertEqual(list(debug.columns), list(resultado.columns))


if __name__ == "__main__":
    unittest.main()
