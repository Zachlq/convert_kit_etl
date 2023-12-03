dataset = "convert_kit"

lsql_ck_table = "lsql_total_subs"

lsql_creds = "lsql_ck_secret.json"

total_subs_schema = [{
    "name": "total", "type": "INTEGER", "mode": "NULLABLE"
},
{
    "name": "dt_updated", "type": "TIMESTAMP", "mode": "NULLABLE"
}
]
