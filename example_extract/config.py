from dotenv import load_dotenv
import os
load_dotenv()
PIPELINE = {
    "layer" : "landing",
    "run_date" : "2025-01-01",
    "database":{
        "type":"postgres",
        "host":"",
        "port":"",
        "database":"",
        "user":"",
        "password":"",
        "tables":["orders"]
    },
    "files":{
        "format":"csv",
        "path":"",
        "header":True
    },
    "paths":{
        "output":""
    },
    "load":{
        "mode":"FULL"
    },
    "schema":{
        "order_id":"string",
        "amount": "decimal(10,2)",
        "country":"string",
        "created_at":"timestamp"
    },
    "quality":{
        "required_columns":["order_id","amount","country","created_at"],
        "not null":["order_id"]
    }
    
}
