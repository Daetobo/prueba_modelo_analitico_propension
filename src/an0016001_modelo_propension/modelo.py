import os
import pkg_resources
from datetime import datetime

import pandas as pd
from orquestador2.step import Step

from .modelo_seleccion import ModeloSeleccion


class Modelo(Step):
    @staticmethod
    def obtener_ruta():
        return pkg_resources.resource_filename(__name__, "static")

    def _ruta_modelo(self):
        return os.path.join(self.obtener_ruta(), "pkl", self.params_modelo["nombre_modelo"])

    def _ruta_columnas(self):
        return os.path.join(self.obtener_ruta(), "pkl", self.params_modelo["nombre_columnas"])

    def fc_descargar_lz_df(self, indice):
        self.log.print_encabezado()
        ruta_sql = self.getSQLPath() + self.list_sql_file[indice]
        return self.hp.obtener_dataframe_archivo(ruta_sql, self.params)

    def fc_subir_df_lz(self, dataframe, tabla_key):
        tabla_destino = self.params_modelo.get("name_tablas", {}).get(tabla_key, "")
        if not tabla_destino or dataframe.empty:
            return
        query = f"{self.params['zona_r']}.{tabla_destino}"
        self.sparky.subir_df(dataframe, query, modo="overwrite")

    def fc_training(self):
        if not self.params_modelo.get("entrenar_modelo", True):
            return

        dia_entrenamiento = self.params_modelo.get("dia_entrenamiento")
        if dia_entrenamiento and self.dia_entrenamiento != dia_entrenamiento:
            return

        df_entrenamiento = self.fc_descargar_lz_df(indice=0)
        df_pagos = self.fc_descargar_lz_df(indice=2)
        df_customer = self.fc_descargar_lz_df(indice=3)
        df_scores = self.fc_descargar_lz_df(indice=4)

        entrenado, metricas = self.selmodelo.entrenamiento(
            df_entrenamiento=df_entrenamiento,
            df_pagos=df_pagos,
            df_customer=df_customer,
            df_scores=df_scores,
            ruta=self._ruta_modelo(),
            ruta_columnas=self._ruta_columnas(),
            config=self.params_modelo,
        )
        if entrenado:
            self.df_metricas = pd.DataFrame(metricas)
            self.fc_subir_df_lz(self.df_metricas, "metricas")

    def fc_predecir(self):
        if not self.params_modelo.get("predecir_modelo", True):
            return

        df_modelo = self.fc_descargar_lz_df(indice=1)
        df_pagos = self.fc_descargar_lz_df(indice=2)
        df_customer = self.fc_descargar_lz_df(indice=3)
        df_scores = self.fc_descargar_lz_df(indice=4)

        self.df_respuesta = self.selmodelo.ejecucion(
            df_modelo=df_modelo,
            df_pagos=df_pagos,
            df_customer=df_customer,
            df_scores=df_scores,
            ruta=self._ruta_modelo(),
            ruta_columnas=self._ruta_columnas(),
            config=self.params_modelo,
        )
        self.fc_subir_df_lz(self.df_respuesta, "predic")

    def ejecutar(self):
        self.setGlobalConfig(self.initGlobalConfiguration())
        self.params = self.getGlobalConfiguration()["parametros_lz"]
        self.params_modelo = self.getStepConfig()
        self.list_sql_file = self.getStepConfig()["archivos"]
        self.hp = self.getHelper()
        self.fecha_entrenamiento = datetime.today().strftime("%Y-%m-%d")
        self.dia_entrenamiento = datetime.today().day
        self.df_respuesta = pd.DataFrame()
        self.df_metricas = pd.DataFrame()
        self.selmodelo = ModeloSeleccion()
        self.executeTasks()
