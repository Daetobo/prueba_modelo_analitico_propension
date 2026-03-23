create table if not exists {zona_p}.{tabla_ingestiones_temp} (
    id_tabla int,
    tabla_logica string,
    tabla_fuente string,
    campo1 string,
    campo2 string,
    campo3 string
)
stored as parquet;

truncate table {zona_p}.{tabla_ingestiones_temp};
