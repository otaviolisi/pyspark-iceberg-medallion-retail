import os
from dotenv import load_dotenv

load_dotenv()


def get_env(name: str, default: str | None = None, required: bool = True) -> str:
    value = os.getenv(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return str(value)


SQLSERVER_HOST = get_env("SQLSERVER_HOST")
SQLSERVER_PORT = get_env("SQLSERVER_PORT", "1433", required=False)
SQLSERVER_DATABASE = get_env("SQLSERVER_DATABASE")
SQLSERVER_USER = get_env("SQLSERVER_USER")
SQLSERVER_PASSWORD = get_env("SQLSERVER_PASSWORD")
SQLSERVER_ENCRYPT = get_env("SQLSERVER_ENCRYPT", "false", required=False)
SQLSERVER_TRUST_SERVER_CERTIFICATE = get_env(
    "SQLSERVER_TRUST_SERVER_CERTIFICATE", "true", required=False
)