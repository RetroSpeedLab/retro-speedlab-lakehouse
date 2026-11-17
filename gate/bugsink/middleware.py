from django.contrib.auth.backends import RemoteUserBackend
from django.contrib.auth.middleware import RemoteUserMiddleware


class GateUserMiddleware(RemoteUserMiddleware):
    header = "HTTP_X_GATE_USER"


class GateUserBackend(RemoteUserBackend):
    create_unknown_user = False
