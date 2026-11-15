#!/usr/bin/env bash
set -euo pipefail

readonly SERVER=black-omen
readonly WHEELHOUSE=/mnt/fastdata/wheelhouse
readonly MAVEN=https://repo1.maven.org/maven2
readonly JAVA=https://api.adoptium.net/v3/binary/version/jdk-21.0.13%2B9/linux/x64/jre/hotspot/normal/eclipse
readonly GIT_REQUIREMENT='import sys, tomllib, pathlib
source = tomllib.loads(pathlib.Path(sys.argv[1], "pyproject.toml").read_text())["tool"]["uv"]["sources"]
print("git+" + source["spark-data-quality"]["git"] + "@" + source["spark-data-quality"]["rev"])'
readonly PLATFORMS=(manylinux_2_28_x86_64 manylinux_2_27_x86_64 manylinux2014_x86_64 manylinux_2_17_x86_64
    manylinux_2_5_x86_64 linux_x86_64 any)
readonly BUILD_TOOLS=(setuptools==80.9.0 wheel==0.45.1 hatchling==1.27.0)
LAKEHOUSE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly LAKEHOUSE

wheels="${HOME}/.cache/retro-speedlab-wheelhouse"
pins="$(mktemp)"
trap 'rm -f "${pins}"' EXIT
mkdir -p "${wheels}/spark-jars" "${wheels}/java"
uv export --project "${LAKEHOUSE}" --frozen --no-dev --no-emit-project --no-emit-package spark-data-quality \
    --no-hashes --quiet | grep '==' | sed 's/ ;.*//' >"${pins}"
while read -r pin; do
    uvx --from pip==25.3 pip download --quiet --no-deps --only-binary=:all: --python-version 3.12 --implementation cp \
        "${PLATFORMS[@]/#/--platform=}" --dest "${wheels}" "${pin}" \
        || uvx --from pip==25.3 pip download --quiet --no-deps --no-binary=:all: --dest "${wheels}" "${pin}"
done <"${pins}"
uvx --python 3.12 --from pip==25.3 pip download --quiet --only-binary=:all: --dest "${wheels}" "${BUILD_TOOLS[@]}"
uvx --from pip==25.3 pip wheel --no-deps --wheel-dir "${wheels}" "$(python3 -c "${GIT_REQUIREMENT}" "${LAKEHOUSE}")"
while read -r sum group artifact version; do
    jar="${artifact}-${version}.jar"
    curl --fail --silent --show-error --location --output "${wheels}/spark-jars/${jar}"         "${MAVEN}/${group//.//}/${artifact}/${version}/${jar}"
    echo "${sum}  ${wheels}/spark-jars/${jar}" | sha256sum --check --quiet
done <"${LAKEHOUSE}/jars.lock"
read -r java_sum java_file <"${LAKEHOUSE}/java.lock"
curl --fail --silent --show-error --location --output "${wheels}/java/${java_file}" "${JAVA}"
echo "${java_sum}  ${wheels}/java/${java_file}" | sha256sum --check --quiet
rsync --archive --partial --whole-file "${wheels}/" "${SERVER}:${WHEELHOUSE}/"
