import os
from typing import Any

from flask_appbuilder.security.manager import AUTH_OAUTH
from pyhive.sqlalchemy_hive import HiveDialect
from sqlalchemy import text
from superset.security import SupersetSecurityManager

DISCORD = "discord"
DISCORD_USER_ID = "712191617095696493"
ADMIN = "Admin"
QUERY_SECONDS = 300


def discord_username(user_id: str) -> str:
    return f"{DISCORD}_{user_id}"


class DiscordSecurityManager(SupersetSecurityManager):
    register_superset_registeruser_view = False

    def get_oauth_user_info(self, provider: str, resp: dict[str, Any]) -> dict[str, Any]:
        me = self.oauth_remotes[provider].get("users/@me").json()
        return {"username": discord_username(me["id"]), "first_name": me["username"], "last_name": ""}

    def auth_user_oauth(self, userinfo: dict[str, Any]) -> Any:
        if userinfo["username"] != discord_username(DISCORD_USER_ID):
            return None
        if self.find_user(username=userinfo["username"]) is None:
            self.add_admin(userinfo)
        return super().auth_user_oauth(userinfo)

    def add_admin(self, userinfo: dict[str, Any]) -> None:
        admin = self.add_user(
            username=userinfo["username"],
            first_name=userinfo["first_name"],
            last_name=userinfo["last_name"],
            email=f"{userinfo['username']}@discord.invalid",
            role=self.find_role(ADMIN),
        )
        if not admin:
            raise RuntimeError(f"Superset could not create the admin {userinfo['username']}")


def spark_listing(dialect: HiveDialect, connection: Any, command: str, schema: str | None) -> list[Any]:
    if schema is None:
        return list(connection.execute(text(command)))
    return list(connection.execute(text(f"{command} IN {dialect.identifier_preparer.quote_identifier(schema)}")))


def spark_table_names(dialect: HiveDialect, connection: Any, schema: str | None, **kwargs: Any) -> list[str]:
    return [row.tableName for row in spark_listing(dialect, connection, "SHOW TABLES", schema)]


def spark_view_names(dialect: HiveDialect, connection: Any, schema: str | None, **kwargs: Any) -> list[str]:
    return [row.viewName for row in spark_listing(dialect, connection, "SHOW VIEWS", schema)]


def discord_provider() -> dict[str, Any]:
    return {
        "name": DISCORD,
        "icon": "fa-discord",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ["DISCORD_CLIENT_ID"],
            "client_secret": os.environ["DISCORD_CLIENT_SECRET"],
            "api_base_url": "https://discord.com/api/",
            "client_kwargs": {"scope": "identify"},
            "access_token_url": "https://discord.com/api/oauth2/token",
            "authorize_url": "https://discord.com/oauth2/authorize",
        },
    }


HiveDialect.get_table_names = spark_table_names
HiveDialect.get_view_names = spark_view_names
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
AUTH_TYPE = AUTH_OAUTH
OAUTH_PROVIDERS = [discord_provider()]
CUSTOM_SECURITY_MANAGER = DiscordSecurityManager
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
