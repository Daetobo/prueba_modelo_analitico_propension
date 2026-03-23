insert into {zona_p}.{tabla_ingestiones_temp}
select
    {id_tabla} as id_tabla,
    '{tabla_logica}' as tabla_logica,
    '{tabla_fuente}' as tabla_fuente,
    cast(max({campo1}) as string) as campo1,
    cast(0 as string) as campo2,
    cast(0 as string) as campo3
from {zona}.{tabla_fuente};
