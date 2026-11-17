import ast
from copy import deepcopy
import gzip
import importlib.util
import json
import os
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
import yaml


REPO = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("carga_segura", REPO / "etl/procesos/carga/_carga_segura.py")
segura = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(segura)
OBJETO = "Progreso_curricular_e__c"


class SalesforceSimulado:
    def __init__(self):
        self.registros = [{"Id": "anterior", "SystemModstamp": "fecha1", "Valor__c": "original"}]
        self.campos = [
            {"name": "Id", "createable": False},
            {"name": "SystemModstamp", "createable": False},
            {"name": "Valor__c", "createable": True, "nillable": False,
             "defaultedOnCreate": False, "length": 10},
        ]

    def __getattr__(self, nombre):
        return SimpleNamespace(describe=lambda: {"fields": deepcopy(self.campos)})

    def query(self, query):
        return {"records": [{"Id": "organizacion"}]}

    def query_all(self, query):
        return {"records": deepcopy(self.registros)}


class BulkSimulado:
    def __init__(self, sf, falla=None):
        self.sf, self.falla = sf, falla
        self.operaciones, self.lotes = [], {}

    def create_delete_job(self, objeto, **kwargs):
        self.operaciones.append("delete")
        return "delete"

    def create_insert_job(self, objeto, **kwargs):
        self.operaciones.append("insert")
        return "insert"

    def post_batch(self, job, stream):
        registros = json.load(stream)
        batch = str(len(self.lotes))
        resultados = []
        for registro in registros:
            exito = self.falla != job
            ident = registro.get("Id", "nuevo" + str(len(self.sf.registros)))
            if exito:
                if job == "delete":
                    self.sf.registros = [r for r in self.sf.registros if r["Id"] != ident]
                else:
                    self.sf.registros.append(dict(registro, Id=ident, SystemModstamp="fecha2"))
            resultados.append({"id": ident, "success": exito})
        self.lotes[batch] = resultados
        return batch

    def close_job(self, job):
        pass

    def abort_job(self, job):
        self.operaciones.append("abort")

    def is_batch_done(self, batch, job):
        return True

    def get_batch_results(self, batch, job):
        return self.lotes[batch]


class CargaSeguraTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        raiz = Path(self.tmp.name)
        self.parches = [
            patch.object(segura, "PREPARADOS", raiz / "prepared"),
            patch.object(segura, "BACKUP", raiz / "backup"),
            patch.object(segura, "RESULTADOS", raiz / "resultados.json"),
            patch.dict("os.environ", {"GITHUB_RUN_ID": "1", "GITHUB_RUN_ATTEMPT": "1",
                                      "ETL_BACKUP_ARTIFACT_ID": "123"}),
        ]
        for parche in self.parches:
            parche.start()
        self.sf = SalesforceSimulado()
        self.bulk = BulkSimulado(self.sf)

    def tearDown(self):
        for parche in reversed(self.parches):
            parche.stop()
        self.tmp.cleanup()

    def preparar_y_respaldar(self):
        segura.preparar(self.sf, OBJETO, pd.DataFrame({"Valor__c": ["nuevo"]}))
        segura.respaldar(self.sf)

    def test_no_prepara_vacios_nan_campos_desconocidos_o_requeridos_ausentes(self):
        casos = [[], [{"Valor__c": float("nan")}], [{"Otro__c": "x"}], [{"Valor__c": None}],
                 [{"Valor__c": "mas-de-diez-caracteres"}]]
        for registros in casos:
            with self.subTest(registros=registros), self.assertRaises(ValueError):
                segura.validar_registros(registros, self.sf.campos)
        self.assertEqual(self.sf.registros[0]["Id"], "anterior")

    def test_columnas_duplicadas_se_rechazan_antes_de_convertir_a_registros(self):
        with self.assertRaises(ValueError):
            segura.preparar(self.sf, OBJETO, pd.DataFrame([[1, 2]], columns=["Valor__c", "Valor__c"]))
        self.assertFalse(segura.PREPARADOS.exists())

    def test_valida_tipos_fecha_y_opciones_antes_del_borrado(self):
        for tipo, valor in (("double", "test 4"), ("int", "1.5"), ("date", "ayer"), ("boolean", "si")):
            campos = [{"name": "Valor__c", "createable": True, "nillable": True, "type": tipo}]
            with self.subTest(tipo=tipo), self.assertRaises(ValueError):
                segura.validar_registros([{"Valor__c": valor}], campos)
        campos = [{"name": "Valor__c", "createable": True, "nillable": True,
                   "restrictedPicklist": True, "picklistValues": [{"value": "A", "active": True}]}]
        with self.assertRaises(ValueError):
            segura.validar_registros([{"Valor__c": "B"}], campos)

    def test_backup_corrupto_no_borra(self):
        self.preparar_y_respaldar()
        (segura.BACKUP / f"{OBJETO}.json.gz").write_bytes(b"corrupto")
        with self.assertRaises(ValueError):
            segura.cargar(self.sf, self.bulk)
        self.assertEqual(self.bulk.operaciones, [])

    def test_todos_los_objetos_se_validan_antes_de_respaldar(self):
        segura.preparar(self.sf, OBJETO, pd.DataFrame({"Valor__c": ["nuevo"]}))
        otro = "Asistencias_e7__c"
        segura.guardar_json(segura.PREPARADOS / f"{otro}.json", {"objeto": otro, "registros": []})
        with self.assertRaises(ValueError):
            segura.respaldar(self.sf)
        self.assertFalse((segura.BACKUP / "manifest.json").exists())

    def test_script_original_de_horario_prepara_sin_consultar_ids_ni_borrar(self):
        # Ejecutar un adaptador real con CSV sintético y SDK de solo lectura simulado.
        entrada = Path(self.tmp.name) / "data/consolidated/horario_regular/base_horaria_final.csv"
        entrada.parent.mkdir(parents=True)
        pd.DataFrame({"Valor": ["nuevo"]}).to_csv(entrada, index=False)
        script = REPO / "etl/procesos/carga/09_horario_regular.py"
        falso = SimpleNamespace(Salesforce=lambda **kwargs: self.sf)
        anterior = Path.cwd()
        try:
            os.chdir(self.tmp.name)
            with patch.dict(sys.modules, {"_carga_segura": segura, "simple_salesforce": falso}), \
                    patch.dict("os.environ", {"SF_USERNAME": "simulado", "SF_PASSWORD": "simulado", "SF_SECURITY_TOKEN": "simulado"}), \
                    patch.object(self.sf, "query_all", side_effect=AssertionError("No consultar IDs al preparar")):
                runpy.run_path(str(script), run_name="__main__")
        finally:
            os.chdir(anterior)
        self.assertTrue((segura.PREPARADOS / "horario_regular__c.json").exists())

    def test_lote_que_no_termina_se_reporta_con_timeout(self):
        with patch.object(self.bulk, "is_batch_done", return_value=False), self.assertRaises(TimeoutError):
            segura.ejecutar_lotes(self.bulk, OBJETO, "insert", [{"Valor__c": "nuevo"}], {}, timeout=0)

    def test_error_al_enviar_guarda_job_e_intenta_abortar(self):
        with patch.object(self.bulk, "post_batch", side_effect=OSError("fallo de red")), self.assertRaises(OSError):
            segura.ejecutar_lotes(self.bulk, OBJETO, "insert", [{"Valor__c": "nuevo"}], {})
        informe = json.loads(segura.RESULTADOS.read_text())
        self.assertEqual(informe["jobs"][0]["job"], "insert")
        self.assertIn("abort", self.bulk.operaciones)

    def test_backup_guarda_campos_completos_ids_y_metadata_sin_modificar_salesforce(self):
        self.preparar_y_respaldar()
        with gzip.open(segura.BACKUP / f"{OBJETO}.json.gz", "rt", encoding="utf-8") as f:
            copia = json.load(f)
        self.assertEqual(copia["registros"], self.sf.registros)
        self.assertEqual(copia["descripcion"]["fields"], self.sf.campos)
        self.assertEqual(self.sf.registros[0]["Id"], "anterior")

    def test_sin_confirmacion_de_artefacto_no_borra(self):
        self.preparar_y_respaldar()
        with patch.dict("os.environ", {"ETL_BACKUP_ARTIFACT_ID": ""}), self.assertRaises(ValueError):
            segura.cargar(self.sf, self.bulk)
        self.assertEqual(self.bulk.operaciones, [])

    def test_preparado_modificado_despues_del_backup_no_borra(self):
        self.preparar_y_respaldar()
        segura.guardar_json(segura.PREPARADOS / f"{OBJETO}.json", {"objeto": OBJETO, "registros": []})
        with self.assertRaises(ValueError):
            segura.cargar(self.sf, self.bulk)
        self.assertEqual(self.bulk.operaciones, [])

    def test_cambios_remotos_desde_el_backup_no_borra(self):
        self.preparar_y_respaldar()
        self.sf.registros[0]["SystemModstamp"] = "cambio-externo"
        with self.assertRaises(ValueError):
            segura.cargar(self.sf, self.bulk)
        self.assertEqual(self.bulk.operaciones, [])

    def test_backup_de_otro_intento_no_borra(self):
        self.preparar_y_respaldar()
        with patch.dict("os.environ", {"GITHUB_RUN_ATTEMPT": "2"}), self.assertRaises(ValueError):
            segura.cargar(self.sf, self.bulk)
        self.assertEqual(self.bulk.operaciones, [])

    def test_delete_fallido_no_inicia_insert_y_guarda_informe(self):
        self.preparar_y_respaldar()
        self.bulk.falla = "delete"
        with self.assertRaises(RuntimeError):
            segura.cargar(self.sf, self.bulk)
        self.assertEqual(self.bulk.operaciones, ["delete"])
        self.assertEqual(json.loads(segura.RESULTADOS.read_text())["estado"], "error")
        self.assertTrue((segura.BACKUP / "manifest.json").exists())

    def test_insert_fallido_no_se_reporta_como_exitoso(self):
        self.preparar_y_respaldar()
        self.bulk.falla = "insert"
        with self.assertRaises(RuntimeError):
            segura.cargar(self.sf, self.bulk)
        self.assertEqual(json.loads(segura.RESULTADOS.read_text())["estado"], "error")
        self.assertTrue((segura.BACKUP / f"{OBJETO}.json.gz").exists())

    def test_carga_correcta_espera_resultados_y_verifica_ids_finales(self):
        self.preparar_y_respaldar()
        segura.cargar(self.sf, self.bulk)
        self.assertEqual(self.bulk.operaciones, ["delete", "insert"])
        self.assertEqual(self.sf.registros[0]["Valor__c"], "nuevo")
        self.assertEqual(json.loads(segura.RESULTADOS.read_text())["estado"], "completado")

    def test_scripts_individuales_solo_preparan_y_workflow_separa_fases(self):
        for ruta in (REPO / "etl/procesos/carga").glob("[0-9][0-9]_*.py"):
            tree = ast.parse(ruta.read_text())
            atributos = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
            self.assertFalse(atributos & {"create_delete_job", "create_insert_job", "post_batch"}, ruta.name)
            self.assertTrue(any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                                and n.func.id == "preparar" for n in ast.walk(tree)))
        w = yaml.load((REPO / ".github/workflows/etl.yml").read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(w["concurrency"]["cancel-in-progress"], "false")
        self.assertNotIn("schedule", w["on"])
        pasos = w["jobs"]["etl"]["steps"]
        fuentes = [i for i, s in enumerate(pasos) if 'python "etl/procesos/' in s.get("run", "") and "/carga/" not in s["run"]]
        preparados = [i for i, s in enumerate(pasos) if '/carga/' in s.get("run", "")]
        self.assertEqual(len(fuentes), 45)
        self.assertEqual(len(preparados), 9)
        self.assertLess(max(fuentes), min(preparados))
        self.assertEqual(w["jobs"]["cargar"]["needs"], "etl")
        self.assertIn("refs/heads/main", w["jobs"]["cargar"]["if"])
        self.assertIn("inputs.cargar_salesforce", w["jobs"]["cargar"]["if"])


if __name__ == "__main__":
    unittest.main()
