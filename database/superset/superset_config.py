import os

from flask import Flask, request
from flask_appbuilder.security.manager import AUTH_DB
from flask_login import current_user, login_user

GATE_USER_HEADER = "X-Gate-User"
QUERY_SECONDS = 300
CACHE_SECONDS = 300
PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]


def trust_gate(app: Flask) -> None:
    @app.before_request
    def sign_in_gate_user() -> None:
        if GATE_USER_HEADER not in request.headers or current_user.is_authenticated:
            return
        user = app.appbuilder.sm.find_user(username=request.headers[GATE_USER_HEADER])
        if user is not None:
            login_user(user)


SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
AUTH_TYPE = AUTH_DB
FLASK_APP_MUTATOR = trust_gate
AUTH_USER_REGISTRATION = False
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
