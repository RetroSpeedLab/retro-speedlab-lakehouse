import os


def secret(name: str) -> str:
    if name not in os.environ:
        raise RuntimeError(f"Set {name}")
    return os.environ[name]
