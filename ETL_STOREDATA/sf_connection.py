from sqlalchemy import create_engine
import json
from pathlib import Path
from urllib.parse import quote_plus

def get_snowflake_engine():
    base_path = Path(__file__).resolve().parent
    config_path = base_path / "db_config.json"

    with open(config_path) as f:
        config = json.load(f)

    sf_config = config["snowflake"]

    encoded_password = quote_plus(sf_config["password"])
    print(encoded_password)

    connection_string = (
        f"snowflake://{sf_config['user']}:{encoded_password}"
        f"@{sf_config['account']}/"
        f"{sf_config['database']}/{sf_config['schema']}"
        f"?warehouse={sf_config['warehouse']}"
        f"&role={sf_config['role']}"
    )

    engine = create_engine(connection_string)
    return engine