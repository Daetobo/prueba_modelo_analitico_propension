# Orq 2 modelo propension

modelo analtico de propensin de aceptacin de opciones de pago desarrollado por Daniel Escobar Soluciones y Analtica

---

[[_TOC_]]

---
## Instalación

Se recomienda realizar la instalación en un [**ambiente virtual**](https://dev.azure.com/GrupoBancolombia/Vicepresidencia%20de%20Innovaci%C3%B3n%20y%20Transformaci%C3%B3n%20Digital/_wiki/wikis/Vicepresidencia-de-Innovaci%C3%B3n-y-Transformaci%C3%B3n-Digital.wiki/105117/Ambientes-virtuales-en-Python-3.9).

*Si se tiene configurado Artifactory:*
```
pip install an0016001-modelo-propension
```
*Si no se tiene configurado Artifactory:*

```
pip install an0016001-modelo-propension -i https://artifactory.apps.bancolombia.com/api/pypi/pypi-bancolombia/simple --trusted-host artifactory.apps.bancolombia.com
```

---
## Ejecución

Si se trabaja con un ambiente virtual, se debe activar primero. [**Más información sobre el uso de ambientes virtuales**](https://dev.azure.com/GrupoBancolombia/Vicepresidencia%20de%20Innovaci%C3%B3n%20y%20Transformaci%C3%B3n%20Digital/_wiki/wikis/Vicepresidencia-de-Innovaci%C3%B3n-y-Transformaci%C3%B3n-Digital.wiki/105117/Ambientes-virtuales-en-Python-3.9).

*Se debe ejecutar el siguiente comando:*
```
python -m an0016001_modelo_propension.ejecucion
```

Para efectos de la generación de logs para la calendarización se pueden indicar los parámetros directamente en los siguientes comandos.

*Logs de estabilidad:*
```
python -m an0016001_modelo_propension.ejecucion -lt "est"
```

*Logs de compilación:*
```
python -m an0016001_modelo_propension.ejecucion -lt "cmp" -pl [porcentaje]
```

En estos comandos para la calendarización el parámetro ```lt``` hace referencia al tipo de log, estabilidad ```est``` o compilación ```cmp```. Cabe resaltar que, si se va a generar un log de compilación, se requiere también el parámetro ```pl``` que hace referencia al porcentaje límite de datos que se toma de las tablas insumo para dicha ejecución (valor entero entre 1 y 100). De igual manera, si se habilita tanto el log de estabilidad o el de compilación, la carpeta para almacenar los logs generados será ```logs_calendarizacion``` y para otros casos la carpeta será ```logs```; en ambos casos se creará la carpeta en el directorio de trabajo actual.

Adicionalmente, con el fin de abreviar los comandos de ejecución, se habilitó utilizar el nombre del paquete (con guiones bajos) directamente para reemplazar la sintáxis de módulos de python; lo cuál permite sustituir la expresión ```python -m an0016001_modelo_propension.ejecucion``` por ```an0016001_modelo_propension``` en cada comando de ejecución si así lo desea.

---
## Prerrequisitos

El paquete ha sido generado para la versión de Python
	```
    3.9.12
    ```
. Las librerías o paquetes necesarios para la ejecución son:
- `pyodbc==4.0.27`
- `orquestador2>=1.2.2`

---
## Insumos y resultados

Los insumos utilizados en el proceso son:

| Zona de Datos | Tabla |
| - | - |
| _zone_x_ | _table_y_ |
| _zone_z_ | _table_k_ |

Los resultados obtenidos son:

| Zona de Datos | Tabla | Descripción | Tipo de Ingestión |
| - | - | - | - |
| _zone_results_ | _table_n_ | Esta información debe describir la tabla. | Incremental |
| _zone_results_ | _table_h_ | Esta información debe describir la tabla. | Full |

---
