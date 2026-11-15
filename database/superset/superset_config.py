import os
from typing import Any

from flask_appbuilder.security.manager import AUTH_OAUTH
from superset.security import SupersetSecurityManager

GITHUB = "github"
GITHUB_USERNAME = "github_datenwissenschaften"
QUERY_SECONDS = 300


class GitHubSecurityManager(SupersetSecurityManager):
    def auth_user_oauth(self, userinfo: dict[str, Any]) -> Any:
        if userinfo["username"] != GITHUB_USERNAME:
            return None
        return super().auth_user_oauth(userinfo)


def github_provider() -> dict[str, Any]:
    return {
        "name": GITHUB,
        "icon": "fa-github",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ["GITHUB_CLIENT_ID"],
            "client_secret": os.environ["GITHUB_CLIENT_SECRET"],
            "api_base_url": "https://api.github.com/",
            "client_kwargs": {"scope": "read:user"},
            "access_token_url": "https://github.com/login/oauth/access_token",
            "authorize_url": "https://github.com/login/oauth/authorize",
            "request_token_url": None,
        },
    }


SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
AUTH_TYPE = AUTH_OAUTH
OAUTH_PROVIDERS = [github_provider()] if "GITHUB_CLIENT_ID" in os.environ else []
CUSTOM_SECURITY_MANAGER = GitHubSecurityManager
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Admin"
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
