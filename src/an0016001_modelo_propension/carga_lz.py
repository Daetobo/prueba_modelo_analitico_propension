from pathlib import Path

import pandas as pd
from orquestador2.step import Step


class CargaLz(Step):
    def _resolver_directorio_datos(self):
        configured_path = self.getStepConfig().get("data_dir", "")
        if not configured_path:
            configured_path = self.getGlobalConfiguration()["parametros_lz"].get("data_dir", "")
        if configured_path:
            return Path(configured_path)
        return Path(__file__).resolve().parents[3]

    @staticmethod
    def _read_source(file_path):
        suffix = file_path.suffix.lower()
        if suffix == ".csv":
            return pd.read_csv(file_path)
        if suffix in {".xlsx", ".xlsm"}:
            return pd.read_excel(file_path)
        raise ValueError(f"Formato no soportado para carga a LZ: {file_path.name}")

    def fn_subir_fuentes_lz(self):
        config = self.getStepConfig()
        data_dir = self._resolver_directorio_datos()
        if not data_dir.exists():
            raise FileNotFoundError(f"No existe el directorio de datos fuente: {data_dir}")

        modo = config.get("modo_carga", "overwrite")
        archivos = config.get("archivos", [])

        for archivo in archivos:
            source_path = data_dir / archivo["archivo"]
            if not source_path.exists():
                raise FileNotFoundError(f"No existe el archivo fuente para cargar a LZ: {source_path}")

            dataframe = self._read_source(source_path)
            zona_destino = archivo.get("zona", self.params["zona_raw"])
            tabla_destino = archivo["tabla"]
            destino = f"{zona_destino}.{tabla_destino}"

            self.log.print_encabezado()
            self.sparky.subir_df(dataframe, destino, modo=modo)

    def ejecutar(self):
        self.setGlobalConfig(self.initGlobalConfiguration())
        self.params = self.getGlobalConfiguration()["parametros_lz"]
        self.hp = self.getHelper()
        self.executeTasks()
