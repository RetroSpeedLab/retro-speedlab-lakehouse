import os
from typing import Any

from flask_appbuilder.security.manager import AUTH_REMOTE_USER
from pyhive.sqlalchemy_hive import HiveDialect
from sqlalchemy import text
from superset.security import SupersetSecurityManager

GATE_USER_HEADER = "HTTP_X_GATE_USER"
QUERY_SECONDS = 300
CACHE_SECONDS = 300
PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]


class GateSecurityManager(SupersetSecurityManager):
    register_superset_auth_view = False
    register_superset_registeruser_view = False


def spark_listing(dialect: HiveDialect, connection: Any, command: str, schema: str | None) -> list[Any]:
    if schema is None:
        return list(connection.execute(text(command)))
    return list(connection.execute(text(f"{command} IN {dialect.identifier_preparer.quote_identifier(schema)}")))


def spark_table_names(dialect: HiveDialect, connection: Any, schema: str | None, **kwargs: Any) -> list[str]:
    return [row.tableName for row in spark_listing(dialect, connection, "SHOW TABLES", schema)]


def spark_view_names(dialect: HiveDialect, connection: Any, schema: str | None, **kwargs: Any) -> list[str]:
    return [row.viewName for row in spark_listing(dialect, connection, "SHOW VIEWS", schema)]


HiveDialect.get_table_names = spark_table_names
HiveDialect.get_view_names = spark_view_names
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
AUTH_TYPE = AUTH_REMOTE_USER
AUTH_REMOTE_USER_ENV_VAR = GATE_USER_HEADER
CUSTOM_SECURITY_MANAGER = GateSecurityManager
AUTH_USER_REGISTRATION = False
AUTH_ROLES_SYNC_AT_LOGIN = False
PUBLIC_ROLE_LIKE = None
FAB_API_SWAGGER_UI = False
ENABLE_PROXY_FIX = True
SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
RATELIMIT_STORAGE_URI = "memory://"
SQLLAB_TIMEOUT = QUERY_SECONDS
SUPERSET_WEBSERVER_TIMEOUT = QUERY_SECONDS
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": CACHE_SECONDS}
EXTRA_CATEGORICAL_COLOR_SCHEMES = [
    {"id": "retroSpeedlab", "label": "Retro Speedlab", "description": "", "isDefault": True, "colors": PALETTE}
]
