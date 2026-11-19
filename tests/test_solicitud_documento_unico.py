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
SCRIPT = REPO / "etl/procesos/carga/08_solicitud_matricula.py"


class SolicitudDocumentoUnicoTests(unittest.TestCase):
    def ejecutar(self, registros):
        with tempfile.TemporaryDirectory() as tmp:
            entrada = Path(tmp) / "data/consolidated/solicitud_matricula/Sol_Mtr_Deduplicado.csv"
            entrada.parent.mkdir(parents=True)
            datos = registros.copy()
            datos["Nombre/s del niño/a"] = "simulado"
            datos["Departamento.1"] = "simulado"
            datos.to_csv(entrada, index=False)
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
            self.assertEqual(preparar.call_args.args[1], "Formulario_SolicitudMatricula_e__c")
            self.assertEqual(len(pd.read_csv(entrada)), len(datos))
            self.assertEqual(len(pd.read_csv(entrada.parent / "DEBUG_para_salesforce.csv")), len(datos))
            self.assertEqual(sf.mock_calls, [])
            return preparar.call_args.args[2]

    def test_conserva_primera_solicitud_aunque_otros_campos_y_fechas_difieran(self):
        resultado = self.ejecutar(pd.DataFrame({
            "Número de Documento": ["00000001", "00000002", "00000001"],
            "Grado al que postula": ["PRIMERO", "SEGUNDO", "TERCERO"],
            "Marca temporal": ["2025-01-01", "2025-01-02", "2026-01-01"],
            "Edad actual": [10, 9, 11],
        }))
        self.assertEqual(resultado["Nmero_de_Documento__c"].tolist(), ["00000001", "00000002"])
        self.assertEqual(resultado["Grado_al_que_postula__c"].tolist(), ["PRIMERO", "SEGUNDO"])
        self.assertNotIn("Edad actual", resultado.columns)

    def test_documento_compartido_del_apoderado_no_elimina_solicitudes_de_distintos_ninios(self):
        resultado = self.ejecutar(pd.DataFrame({
            "Número de Documento": ["00000001", "00000002"],
            "Número de Documento.1": ["12345678", "12345678"],
        }))
        self.assertEqual(len(resultado), 2)

    def test_no_colapsa_documentos_vacios_y_compara_sin_espacios_exteriores(self):
        resultado = self.ejecutar(pd.DataFrame({
            "Número de Documento": [None, None, " 00000001 ", "00000001"],
            "Grado al que postula": ["PRIMERO", "SEGUNDO", "TERCERO", "CUARTO"],
        }))
        self.assertEqual(resultado["Grado_al_que_postula__c"].tolist(), ["PRIMERO", "SEGUNDO", "TERCERO"])


if __name__ == "__main__":
    unittest.main()
