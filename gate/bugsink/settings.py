from bugsink_conf import *  # noqa: F403
from bugsink_conf import MIDDLEWARE

AUTHENTICATION = "django.contrib.auth.middleware.AuthenticationMiddleware"
SIGNED_IN = MIDDLEWARE.index(AUTHENTICATION) + 1

MIDDLEWARE = [*MIDDLEWARE[:SIGNED_IN], "gate.middleware.GateUserMiddleware", *MIDDLEWARE[SIGNED_IN:]]
AUTHENTICATION_BACKENDS = ["gate.middleware.GateUserBackend", "django.contrib.auth.backends.ModelBackend"]
