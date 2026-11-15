import os
import subprocess
import sys
from pathlib import Path

CORE = Path("retro-speedlab-core", "src")
ENGINE = Path("retro-speedlab-engine")
GAMES = Path("retro-speedlab-games")
TRACE = "datenwissenschaften.speedrun.trace"
LEVEL_CLOCK = "datenwissenschaften.speedrun.level_clock"
INHERITED = ("PATH", "LD_LIBRARY_PATH")
ERROR_TAIL = 2000


def collection(speedlab: Path) -> Path:
    return speedlab / GAMES


def inherited() -> dict[str, str]:
    return {name: os.environ[name] for name in INHERITED if name in os.environ}


def search_sources(speedlab: Path) -> dict[str, str]:
    return {**inherited(), "PYTHONPATH": os.pathsep.join(str(speedlab / part) for part in (CORE, ENGINE, GAMES))}


def core_sources(speedlab: Path) -> dict[str, str]:
    return {**inherited(), "PYTHONPATH": str(speedlab / CORE)}


def ran(command: list[str], stdin: bytes, cwd: Path | None, environment: dict[str, str]) -> bytes:
    done = subprocess.run(command, input=stdin, capture_output=True, cwd=cwd, env=environment, check=False)
    if done.returncode != 0:
        raise RuntimeError(f"{' '.join(command)} failed: {done.stderr.decode()[-ERROR_TAIL:]}")
    return done.stdout


def record_pieces(speedlab: Path, slug: str, record: Path) -> bytes:
    command = [sys.executable, "-m", TRACE, "pieces", str(collection(speedlab)), slug, str(record)]
    return ran(command, b"", None, search_sources(speedlab))


def piece_traces(speedlab: Path, slug: str, pieces: bytes) -> bytes:
    command = [sys.executable, "-m", TRACE, "frames", str(collection(speedlab)), slug]
    return ran(command, pieces, None, search_sources(speedlab))


def verified_by(app: Path, verifier: str, cartridge: str, inputs: bytes) -> bytes:
    return ran([sys.executable, "-E", "-s", "-m", verifier, cartridge], inputs, app, inherited())


def clocked_by(speedlab: Path, app: Path, cartridge: str, inputs: bytes) -> bytes:
    return ran([sys.executable, "-s", "-m", LEVEL_CLOCK, cartridge], inputs, app, core_sources(speedlab))
