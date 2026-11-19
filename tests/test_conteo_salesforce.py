import csv
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch


REPO = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("conteo", REPO / "scripts/contar_salesforce.py")
conteo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(conteo)


class ConteoSalesforceTests(unittest.TestCase):
    def test_objeto_inactivo_no_disponible_se_informa_sin_fallar_el_diagnostico(self):
        sf = Mock(spec_set=["query"])
        sf.query.side_effect = [{"totalSize": 1}] * 10 + [RuntimeError("no disponible")]
        resultado = conteo.contar(sf)
        self.assertEqual(resultado[-1]["estado"], "no disponible")
        self.assertIsNone(resultado[-1]["registros"])
        with patch.object(conteo, "conectar", return_value=sf), \
                patch.object(conteo, "contar", return_value=resultado), \
                patch.object(conteo, "guardar_resultados"):
            self.assertEqual(conteo.main(), 0)

    def test_detalle_de_error_conserva_codigo_sin_publicar_mensaje(self):
        error = RuntimeError("mensaje-privado")
        error.status = 400
        error.content = [{"errorCode": "INVALID_TYPE", "message": "mensaje-privado"}]
        detalle = conteo.detalle_error(error)
        self.assertIn("INVALID_TYPE", detalle)
        self.assertIn("HTTP 400", detalle)
        self.assertNotIn("mensaje-privado", detalle)

    def test_conexion_configura_timeout_y_solo_reintentos_get(self):
        with patch.dict(os.environ, {"SF_USERNAME": "simulado", "SF_PASSWORD": "simulado",
                                     "SF_SECURITY_TOKEN": "simulado"}), \
                patch.object(conteo, "Salesforce") as factory:
            conteo.conectar()
        opciones = factory.call_args.kwargs
        self.assertNotIn("timeout", opciones)
        session = opciones["session"]
        self.assertEqual(session.get_adapter("https://example.test").max_retries.allowed_methods, {"GET"})
        with patch("requests.Session.request") as request:
            session.get("https://example.test")
        self.assertEqual(request.call_args.kwargs["timeout"], 60)

    def test_solo_consulta_count_y_conserva_cero_como_resultado_valido(self):
        sf = Mock(spec_set=["query"])
        sf.query.side_effect = [{"totalSize": n} for n in range(len(conteo.OBJETOS))]
        resultado = conteo.contar(sf)
        self.assertEqual([r["registros"] for r in resultado], list(range(11)))
        self.assertTrue(all(r["estado"] == "correcto" for r in resultado))
        self.assertEqual([c.args[0] for c in sf.query.call_args_list],
                         [f"SELECT COUNT() FROM {o}" for _, o, _ in conteo.OBJETOS])
        self.assertEqual(sum(r["activo_en_etl"] for r in resultado), 9)

    def test_error_de_un_objeto_no_impide_consultar_los_demas(self):
        sf = Mock(spec_set=["query"])
        sf.query.side_effect = [{"totalSize": 5}, RuntimeError("dato-privado")] + [{"totalSize": 0}] * 9
        resultado = conteo.contar(sf)
        self.assertEqual(sf.query.call_count, 11)
        self.assertEqual(resultado[1]["estado"], "error")
        self.assertIsNone(resultado[1]["registros"])
        self.assertNotIn("dato-privado", resultado[1]["detalle"])
        self.assertEqual(resultado[-1]["estado"], "correcto")

    def test_resumen_csv_json_y_github_contienen_las_mismas_cantidades(self):
        sf = Mock(spec_set=["query"])
        sf.query.return_value = {"totalSize": 1338}
        resultado = conteo.contar(sf)
        with tempfile.TemporaryDirectory() as tmp:
            raiz = Path(tmp)
            summary = raiz / "summary.md"
            with patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": str(summary)}):
                conteo.guardar_resultados(resultado, raiz / "salida")
            with (raiz / "salida/resumen.csv").open(encoding="utf-8-sig", newline="") as archivo:
                csv_filas = list(csv.DictReader(archivo))
            self.assertEqual(len(csv_filas), 11)
            self.assertEqual(csv_filas[2]["proceso"], "Matrícula")
            self.assertEqual(csv_filas[2]["registros"], "1338")
            informe = json.loads((raiz / "salida/resumen.json").read_text())
            self.assertEqual(informe["objetos"], resultado)
            self.assertIn("-05:00", informe["fecha_peru"])
            self.assertEqual(summary.read_text(), (raiz / "salida/resumen.md").read_text())
            self.assertIn("Solicitud Matrícula", summary.read_text())

    def test_fallo_de_conexion_deja_informe_y_devuelve_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            anterior = Path.cwd()
            try:
                os.chdir(tmp)
                with patch.object(conteo, "conectar", side_effect=RuntimeError("secret-no-publicar")), \
                        patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": ""}):
                    self.assertEqual(conteo.main(), 1)
                informe = json.loads(Path("resultados-salesforce/resumen.json").read_text())
                self.assertEqual(informe["error_general"], "RuntimeError")
                self.assertNotIn("secret-no-publicar", Path("resultados-salesforce/resumen.md").read_text())
            finally:
                os.chdir(anterior)


if __name__ == "__main__":
    unittest.main()
