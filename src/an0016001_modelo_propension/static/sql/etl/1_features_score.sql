drop table if exists {zona_p}.{tabla_features_score};

create table {zona_p}.{tabla_features_score}
stored as parquet as
select
    concat_ws(
        '#',
        cast(base.nit_enmascarado as string),
        cast(base.num_oblig_orig_enmascarado as string),
        cast(base.num_oblig_enmascarado as string)
    ) as id,
    cast(null as int) as var_rpta_alt,
    base.*
from {zona_raw}.{tabla_oot_raw} base;
