import pkg_resources

from orquestador2.step import Step


class Etl(Step):
    def obtener_ruta(self):
        return pkg_resources.resource_filename(__name__, "static")

    def fn_validar_ingestiones(self):
        return None

    def fn_ejecutar_etl(self):
        self.log.print_encabezado()
        for file in self.list_sql_file:
            archivo = self.getSQLPath() + file
            self.hp.ejecutar_archivo(archivo, self.params)

    def ejecutar(self):
        self.setGlobalConfig(self.initGlobalConfiguration())
        self.params = self.getGlobalConfiguration()["parametros_lz"]
        self.hp = self.getHelper()
        self.list_sql_file = self.getStepConfig()["archivos"]
        self.executeTasks()
