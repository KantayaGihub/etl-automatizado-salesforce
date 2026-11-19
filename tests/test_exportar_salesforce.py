from copy import deepcopy
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from openpyxl import load_workbook


REPO = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("exportar", REPO / "scripts/exportar_salesforce.py")
exportar = importlib.util.module_from_spec(SPEC)
with patch.object(sys, "path", [str(REPO / "scripts"), *sys.path]):
    SPEC.loader.exec_module(exportar)


class ExportarSalesforceTests(unittest.TestCase):
    def cliente(self, vacio=False):
        sf = Mock(spec_set=["query_all", "Matricula_e__c", "Formulario_SolicitudMatricula_e__c"])
        campos = [{"name": "Id", "label": "ID del registro"},
                  {"name": "DNI__c", "label": "Documento"},
                  {"name": "Comentario__c", "label": "Comentario"}]
        for _, objeto in exportar.OBJETOS:
            getattr(sf, objeto).describe.return_value = {"fields": deepcopy(campos)}
        registros = [] if vacio else [
            {"Id": "registro1", "DNI__c": "00000001", "Comentario__c": "=1+1", "attributes": {"type": "simulado"}},
            {"Id": "registro2", "DNI__c": "00000002", "Comentario__c": None},
        ]
        sf.query_all.return_value = {"totalSize": len(registros), "records": registros}
        return sf

    def test_excel_tiene_dos_hojas_y_todos_los_registros_campos_y_dni_como_texto(self):
        sf = self.cliente()
        with tempfile.TemporaryDirectory() as tmp:
            salida = Path(tmp) / "datos.xlsx"
            resumen = exportar.exportar(sf, salida)
            libro = load_workbook(salida, data_only=False)
            self.assertEqual(libro.sheetnames, ["Matricula", "Solicitud_Matricula"])
            for hoja in libro:
                self.assertEqual(hoja.max_row, 3)
                self.assertEqual(hoja.max_column, 3)
                self.assertEqual([c.value for c in hoja[1]], ["Id", "DNI__c", "Comentario__c"])
                self.assertEqual(hoja["B2"].value, "00000001")
                self.assertEqual(hoja["C2"].value, "=1+1")
                self.assertEqual(hoja["C2"].data_type, "s")
                self.assertIsNone(hoja["C3"].value)
                self.assertEqual(hoja["B1"].comment.text, "Documento")
            libro.close()
            self.assertEqual([r["registros"] for r in resumen], [2, 2])
        self.assertEqual([c.args[0] for c in sf.query_all.call_args_list],
                         [f"SELECT Id,DNI__c,Comentario__c FROM {o} ORDER BY Id" for _, o in exportar.OBJETOS])
        self.assertEqual(set(c[0] for c in sf.mock_calls),
                         {"Matricula_e__c.describe", "Formulario_SolicitudMatricula_e__c.describe", "query_all"})

    def test_objetos_vacios_conservan_las_dos_hojas_y_encabezados(self):
        with tempfile.TemporaryDirectory() as tmp:
            salida = Path(tmp) / "datos.xlsx"
            exportar.exportar(self.cliente(vacio=True), salida)
            libro = load_workbook(salida)
            self.assertEqual(len(libro.sheetnames), 2)
            self.assertTrue(all(hoja.max_row == 1 for hoja in libro))
            libro.close()

    def test_fallo_de_segundo_objeto_no_publica_excel_parcial(self):
        sf = self.cliente()
        sf.Formulario_SolicitudMatricula_e__c.describe.side_effect = RuntimeError("sin acceso")
        with tempfile.TemporaryDirectory() as tmp:
            salida = Path(tmp) / "datos.xlsx"
            with self.assertRaises(RuntimeError):
                exportar.exportar(sf, salida)
            self.assertFalse(salida.exists())

    def test_respuesta_incompleta_no_se_publica(self):
        sf = self.cliente()
        sf.query_all.return_value["totalSize"] = 10
        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
            exportar.exportar(sf, Path(tmp) / "datos.xlsx")

    def test_no_trunca_valores_que_exceden_el_limite_de_excel(self):
        with self.assertRaises(ValueError):
            exportar.valor_excel("x" * 32768)


if __name__ == "__main__":
    unittest.main()
