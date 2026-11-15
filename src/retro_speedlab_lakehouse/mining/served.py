import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import httpx
from pydantic import BaseModel, ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

MANIFEST = "manifest.json"
TIMEOUT_SECONDS = 60.0


class Manifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: str
    cartridge: str
    app: str
    verifier: str


@dataclass(frozen=True, slots=True)
class ServedGame:
    manifest: Manifest
    app: Path


def unpacked_app(app: bytes, directory: Path) -> Path:
    with zipfile.ZipFile(BytesIO(app)) as archive:
        roots = {Path(name).parts[0] for name in archive.namelist()}
        if len(roots) != 1:
            raise RuntimeError(f"The game app holds {sorted(roots)}, not one app folder")
        archive.extractall(directory)
    return directory / roots.pop()


@retry(
    retry=retry_if_exception_type(httpx.TransportError),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2),
    reraise=True,
)
def served_game(games_url: str, slug: str, directory: Path) -> ServedGame:
    with httpx.Client(base_url=f"{games_url}/{slug}/", timeout=TIMEOUT_SECONDS) as client:
        listed = client.get(MANIFEST)
        listed.raise_for_status()
        manifest = Manifest.model_validate(listed.json())
        app = client.get(manifest.app)
        app.raise_for_status()
    return ServedGame(manifest, unpacked_app(app.content, directory))
