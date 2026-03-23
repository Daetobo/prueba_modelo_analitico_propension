drop table if exists {zona_p}.{tabla_features_train};

create table {zona_p}.{tabla_features_train}
stored as parquet as
select
    concat_ws(
        '#',
        cast(base.nit_enmascarado as string),
        cast(base.num_oblig_orig_enmascarado as string),
        cast(base.num_oblig_enmascarado as string)
    ) as id,
    base.*
from {zona_raw}.{tabla_train_raw} base;
