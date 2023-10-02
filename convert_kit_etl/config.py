bucket = "convert_kit"

creds = "convert_kit_secret.json"

dataset = "convert_kit"

subscriptions_table = "subscriptions"

all_subs_schema = [{
    "name": "created_at", "type": "DATE", "mode": "NULLABLE"
},
{
    "name": "email_address", "type": "STRING", "mode": "NULLABLE"
},
{
    "name": "id", "type": "STRING", "mode": "NULLABLE"
},
{
    "name": "state", "type": "STRING", "mode": "NULLABLE"
}]

total_subscriptions_table = "total_subscriptions"

total_subs_schema = [{
    "name": "total", "type": "INTEGER", "mode": "NULLABLE"
},
{
    "name": "dt_updated", "type": "TIMESTAMP", "mode": "NULLABLE"
}
]
