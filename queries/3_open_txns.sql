select
    trx_id,
    trx_mysql_thread_id,
    trx_state,
    unix_timestamp() - (to_seconds(trx_started) - to_seconds('1970-01-01 00:00:00')) as trx_age_seconds,
    trx_weight,
    trx_query,
    trx_tables_in_use,
    trx_tables_locked,
    trx_lock_structs,
    trx_rows_locked,
    trx_rows_modified,
    trx_isolation_level,
    trx_unique_checks,
    trx_is_read_only
from
    information_schema.innodb_trx
order by trx_started asc