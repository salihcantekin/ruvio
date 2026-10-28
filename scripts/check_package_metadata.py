#!/usr/bin/env python3
"""Check the descriptions, links and license declarations that package
registries show: crates.io (Cargo.toml), NuGet (csproj), npm (package.json), PyPI
(pyproject.toml), Maven Central (pom.xml), Docker Hub (Dockerfile
labels and docker/hub/*.md). Exits 1 and lists every problem."""
import json
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REPO = "https://github.com/salihcantekin/ruvio"
SITE = "https://ruviodb.com/"
errors = []


def fail(where, message):
    errors.append(f"{where}: {message}")


def cargo(path, license_expected, published):
    data = tomllib.loads((ROOT / path).read_text())["package"]
    where = path
    if data.get("license") != license_expected:
        fail(where, f"license is {data.get('license')!r}, expected {license_expected!r}")
    if not (ROOT / Path(path).parent / "LICENSE").exists() and not (ROOT / Path(path).parent.parent / "LICENSE").exists():
        fail(where, "no LICENSE file in the crate directory or its parent")
    if not data.get("description"):
        fail(where, "description missing")
    if published:
        for key in ("repository", "homepage", "readme", "keywords", "categories"):
            if not data.get(key):
                fail(where, f"{key} missing")
        if data.get("repository") != REPO:
            fail(where, "repository is not " + REPO)
        readme = data.get("readme")
        if readme and not (ROOT / Path(path).parent / readme).exists():
            fail(where, f"readme file {readme} not found")


def tag(text, name):
    match = re.search(rf"<{name}>(.*?)</{name}>", text, re.S)
    return match.group(1).strip() if match else None


def nuget(path):
    # A project's own value wins; shared metadata comes from Directory.Build.props.
    text = (ROOT / path).read_text() + (ROOT / "Directory.Build.props").read_text()
    if tag(text, "IsPackable") == "false":
        return
    for name in ("PackageId", "Description", "Authors", "PackageTags", "PackageReadmeFile", "PackageIcon", "RepositoryUrl", "PackageProjectUrl", "PackageLicenseExpression"):
        if not tag(text, name):
            fail(path, f"{name} missing")
    if tag(text, "PackageLicenseExpression") != "MIT":
        fail(path, "PackageLicenseExpression is not MIT")
    if tag(text, "RepositoryUrl") != REPO:
        fail(path, "RepositoryUrl is not " + REPO)
    if tag(text, "PackageProjectUrl") != SITE:
        fail(path, "PackageProjectUrl is not " + SITE)
    readme = tag(text, "PackageReadmeFile")
    if readme and not (ROOT / Path(path).parent / readme).exists():
        fail(path, f"{readme} not found next to the project")


def node(path):
    data = json.loads((ROOT / path).read_text())
    for key in ("name", "version", "description", "license", "author", "homepage", "repository", "bugs", "keywords", "files"):
        if not data.get(key):
            fail(path, f"{key} missing")
    if data.get("license") != "MIT":
        fail(path, "license is not MIT")
    if "salihcantekin/ruvio" not in json.dumps(data.get("repository")):
        fail(path, "repository does not point to " + REPO)
    if not (ROOT / Path(path).parent / "LICENSE").exists():
        fail(path, "no LICENSE file next to package.json")
    if not (ROOT / Path(path).parent / "README.md").exists():
        fail(path, "no README.md next to package.json")


def python(path):
    data = tomllib.loads((ROOT / path).read_text())["project"]
    for key in ("name", "version", "description", "readme", "license", "requires-python", "classifiers", "keywords", "urls"):
        if not data.get(key):
            fail(path, f"{key} missing")
    if "MIT" not in str(data.get("license")):
        fail(path, "license is not MIT")
    if REPO not in data.get("urls", {}).values():
        fail(path, "urls has no Repository entry for " + REPO)
    for name in (data.get("readme"), "LICENSE"):
        if name and not (ROOT / Path(path).parent / name).exists():
            fail(path, f"{name} not found next to pyproject.toml")


def maven(path):
    text = (ROOT / path).read_text()
    for name in ("groupId", "artifactId", "version", "name", "description", "url"):
        if not tag(text, name):
            fail(path, f"{name} missing")
    for needle in ("<licenses>", "<developers>", "<scm>", "<connection>", "maven-source-plugin", "maven-javadoc-plugin", "maven-gpg-plugin", "central-publishing-maven-plugin"):
        if needle not in text:
            fail(path, f"{needle} missing")
    if REPO not in text:
        fail(path, "scm does not point to " + REPO)


def docker(dockerfile, hub_file):
    text = (ROOT / dockerfile).read_text()
    final = text.rsplit("\nFROM ", 1)[-1]
    for key in ("title", "description", "source", "url", "documentation", "licenses", "authors"):
        if f"org.opencontainers.image.{key}=" not in final:
            fail(dockerfile, f"OCI label {key} missing in the final stage")
    if hub_file:
        body = (ROOT / hub_file).read_text()
        if len(body.encode()) > 25000:
            fail(hub_file, "longer than the 25000 bytes Docker Hub accepts")
        for needle in (REPO, SITE, "AGPL-3.0", "## License"):
            if needle not in body:
                fail(hub_file, f"does not mention {needle}")


cargo("Cargo.toml", "AGPL-3.0-only", True)
cargo("clients/ruvio-client/Cargo.toml", "MIT", True)
cargo("clients/ruvio-ui/Cargo.toml", "AGPL-3.0-only", False)
for project in sorted(list((ROOT / "clients/csharp").glob("*/*.csproj")) + list((ROOT / "integrations/dotnet").glob("*/*.csproj"))):
    rel = project.relative_to(ROOT).as_posix()
    if rel.endswith(("Tests.csproj", "Benchmarks.csproj", "Tour.csproj", "Generators.csproj")):
        continue
    nuget(rel)
node("clients/node/package.json")
python("clients/python/pyproject.toml")
maven("clients/java/pom.xml")
def pins():
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]
    for path in ("README.md", "docs/SETUP.md"):
        found = re.findall(r"(?m)^v=(\d+\.\d+\.\d+)$", (ROOT / path).read_text())
        if not found:
            fail(path, "has no v=x.y.z download line")
        for pinned in found:
            if pinned != version:
                fail(path, f"download line pins {pinned}, Cargo.toml is {version}; run scripts/release_version.py server --install-pins")


    manifest = (ROOT / "deploy/kubernetes.yaml").read_text()
    for image in re.findall(r"image: salihcantekin/ruvio:(\S+)", manifest):
        if image != version:
            fail("deploy/kubernetes.yaml", f"image tag is {image}, Cargo.toml is {version}")


pins()
docker("Dockerfile.linux", "docker/hub/ruvio.md")
docker("Dockerfile", None)
docker("clients/ruvio-ui/Dockerfile", "docker/hub/ruvio-ui.md")

if errors:
    print("\n".join(errors))
    sys.exit(1)
print("package metadata ok")
