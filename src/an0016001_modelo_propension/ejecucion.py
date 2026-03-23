import argparse
import os
import sys
from datetime import datetime

import json
import pkg_resources
import warnings
from orquestador2.orquestador2 import Orchestrator

if __package__ in (None, ""):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    from an0016001_modelo_propension.carga_lz import CargaLz
    from an0016001_modelo_propension.etl import Etl
    from an0016001_modelo_propension.modelo import Modelo
    from an0016001_modelo_propension.preprocesador import Preprocesador
else:
    from .carga_lz import CargaLz
    from .etl import Etl
    from .modelo import Modelo
    from .preprocesador import Preprocesador

warnings.simplefilter(action="ignore", category=UserWarning)


def asegurar_directorio(path):
    if not os.path.exists(path):
        os.mkdir(path)


def actualizar_fechas_ejecucion(fecha):
    ruta_json = pkg_resources.resource_filename(__name__, "static/config.json")
    with open(ruta_json, "r", encoding="utf-8") as file:
        data = json.load(file)

    data["global"]["parametros_lz"]["fecha_num"] = int(fecha.strftime("%Y%m%d"))
    data["global"]["parametros_lz"]["year"] = fecha.year
    data["global"]["parametros_lz"]["month"] = fecha.month
    data["global"]["parametros_lz"]["day"] = fecha.day

    with open(ruta_json, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)


def leer_argumentos_command_line():
    parser = argparse.ArgumentParser(description="Orq 2 modelo propension")
    parser.add_argument("-y", "--kwargs_year", type=int, help="Año de ejecución")
    parser.add_argument("-m", "--kwargs_month", type=int, help="Mes de ejecución")
    parser.add_argument("-d", "--kwargs_day", type=int, help="Día de ejecución")
    parser.add_argument("--f", "--fecha", dest="fecha", help="Fecha en formato yyyymmdd")
    parser.add_argument("-zp", "--zona_procesamiento", type=str, help="Zona de procesamiento")
    parser.add_argument(
        "-s",
        "--step",
        choices=["carga_lz", "preprocesador", "etl", "modelo", "full"],
        default="full",
        help="Ejecuta solo un paso o el flujo completo",
    )
    parser.add_argument("-lt", "--log_type", type=str, default="", help="Tipo de log")
    parser.add_argument(
        "-pl",
        "--porcentaje_limit",
        type=int,
        default=100,
        help="Porcentaje limit para logs cmp",
    )
    args = parser.parse_args()

    fecha = datetime.now().date()
    if args.fecha:
        fecha = datetime.strptime(args.fecha, "%Y%m%d").date()
    elif args.kwargs_year and args.kwargs_month and args.kwargs_day:
        fecha = datetime(args.kwargs_year, args.kwargs_month, args.kwargs_day).date()

    kwargs = {key: value for key, value in vars(args).items() if value not in (None, "")}
    kwargs.pop("fecha", None)
    kwargs["log_type"] = args.log_type
    kwargs["porcentaje_limit"] = args.porcentaje_limit
    return fecha, kwargs


def ejecutar(kwargs=None):
    kwargs = kwargs or {}
    ruta_paquete = pkg_resources.resource_filename(__name__, "")
    logs_path = os.path.join(ruta_paquete.split("src")[0], "logs_calendarizacion")
    asegurar_directorio("logs")
    asegurar_directorio(logs_path)

    nom_proyecto = "AN0016001-modelo-propension"
    if kwargs.get("log_type") not in ("cmp", "est"):
        kwargs.pop("log_type", None)
        kwargs.pop("porcentaje_limit", None)
        kwargs["log_path"] = os.path.join(ruta_paquete.split("src")[0], "logs")
    else:
        kwargs["log_path"] = logs_path

    requested_step = kwargs.pop("step", "full")
    step_map = {
        "carga_lz": [CargaLz(**kwargs)],
        "preprocesador": [Preprocesador(**kwargs)],
        "etl": [Etl(**kwargs)],
        "modelo": [Modelo(**kwargs)],
        "full": [CargaLz(**kwargs), Preprocesador(**kwargs), Etl(**kwargs), Modelo(**kwargs)]
    }
    steps = step_map[requested_step]
    orquestador = Orchestrator(nom_proyecto, steps, **kwargs)
    orquestador.ejecutar()


def main():
    fecha, kwargs = leer_argumentos_command_line()
    actualizar_fechas_ejecucion(fecha)
    ejecutar(kwargs=kwargs)


if __name__ == "__main__":
    main()
