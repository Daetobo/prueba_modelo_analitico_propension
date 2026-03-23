import json
import pkg_resources

from orquestador2.step import Step


class Preprocesador(Step):
    def obtener_ruta(self):
        return pkg_resources.resource_filename(__name__, "static")

    def _resolver_valor(self, valor):
        if isinstance(valor, str):
            try:
                return valor.format(**self.params)
            except KeyError:
                return valor
        return valor

    def fn_leer_parametros(self):
        lista_diccionarios = []
        relaciones = {}

        for tabla_logica, valores in self.ingestiones.items():
            relaciones_temp = {}
            diccionario_temp = {
                "id_tabla": valores["id_tabla"],
                "zona": self._resolver_valor(valores["zona"]),
                "tabla_fuente": self._resolver_valor(valores["tabla_fuente"]),
                "tabla_logica": tabla_logica,
            }

            campos = list(valores["campos"].items())
            for indice in range(1, 4):
                campo_key = f"campo{indice}"
                if indice <= len(campos):
                    diccionario_temp[campo_key] = campos[indice - 1][1]
                    relaciones_temp[campos[indice - 1][0]] = campo_key
                else:
                    diccionario_temp[campo_key] = "0"

            lista_diccionarios.append(diccionario_temp)
            relaciones[tabla_logica] = relaciones_temp

        self.lista_dicts = lista_diccionarios
        self.relaciones_campos = relaciones

    def fn_consultar_ingestiones(self):
        ruta_query_crear = self.getSQLPath() + "ingestiones/1_crear_ingestiones.sql"
        self.hp.ejecutar_archivo(ruta_query_crear, self.params)

        ruta_query_insertar = self.getSQLPath() + "ingestiones/2_insertar_ingestiones.sql"
        with open(ruta_query_insertar, "r", encoding="utf-8") as file:
            template_query = file.read()
            for dictionary in self.lista_dicts:
                text_query = template_query
                for key, value in dictionary.items():
                    text_query = text_query.replace("{" + key + "}", str(value))
                self.hp.ejecutar_consulta(text_query, self.params)

    def fn_guardar_ingestiones(self):
        ruta_query_obtener = self.getSQLPath() + "ingestiones/3_obtener_ingestiones.sql"
        df_ingestiones = self.hp.obtener_dataframe_archivo(ruta_query_obtener, self.params)

        dic_ingestiones = {}
        if "tabla_logica" in df_ingestiones.columns:
            for tabla_logica, campos in self.relaciones_campos.items():
                tabla_df = df_ingestiones.loc[df_ingestiones["tabla_logica"] == tabla_logica]
                if tabla_df.empty:
                    continue
                for campo_destino, valor_origen in campos.items():
                    dic_ingestiones[campo_destino] = str(tabla_df[valor_origen].iloc[0])

        ruta_json = pkg_resources.resource_filename(__name__, "static/config.json")
        with open(ruta_json, "r", encoding="utf-8") as file:
            json_data = json.load(file)
        json_data["global"]["parametros_lz"].update(dic_ingestiones)
        with open(ruta_json, "w", encoding="utf-8") as file:
            json.dump(json_data, file, indent=4)

    def ejecutar(self):
        self.setGlobalConfig(self.initGlobalConfiguration())
        self.params = self.getGlobalConfiguration()["parametros_lz"]
        self.hp = self.getHelper()
        self.ingestiones = self.getStepConfig()["ingestiones"]
        self.executeTasks()
