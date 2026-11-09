"""Publish dependencies first; verify existing artifacts before resuming."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tomllib
import urllib.error
import urllib.request


def run(*args):
    subprocess.run(args, check=True)


def package_contents(artifact, name, version):
    prefix = f"{name}-{version}/"
    contents = {}
    with tarfile.open(fileobj=io.BytesIO(artifact), mode="r:gz") as archive:
        for member in archive:
            if not member.name.startswith(prefix):
                raise SystemExit(f"{name} {version} has an unexpected archive path")
            path = member.name.removeprefix(prefix)
            # Cargo records the packaging commit even when no crate file changed.
            if path == ".cargo_vcs_info.json":
                continue
            if path in contents:
                raise SystemExit(f"{name} {version} has duplicate archive paths")
            data = archive.extractfile(member).read() if member.isfile() else None
            contents[path] = (member.type, member.mode, member.linkname, data)
    return contents


def publish(manifest):
    package = tomllib.loads(Path(manifest).read_text())["package"]
    name, version = package["name"], package["version"]
    # Always compile the packaged source, even when retrying a completed upload.
    run("cargo", "publish", "-p", name, "--locked", "--dry-run", "--registry", "crates-io")
    request = urllib.request.Request(
        f"https://index.crates.io/{name[:2]}/{name[2:4]}/{name}",
        headers={"User-Agent": "we-breeze-release (https://github.com/we-breeze)"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            entries = [json.loads(line) for line in response]
            existing = next((entry for entry in entries if entry["vers"] == version), None)
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        existing = None
    if existing is not None:
        artifact = Path("target/package") / f"{name}-{version}.crate"
        local = artifact.read_bytes()
        checksum = existing["cksum"]
        if hashlib.sha256(local).hexdigest() != checksum:
            url = f"https://static.crates.io/crates/{name}/{name}-{version}.crate"
            with urllib.request.urlopen(url, timeout=30) as response:
                published = response.read()
            if hashlib.sha256(published).hexdigest() != checksum:
                raise SystemExit(f"{name} {version} archive does not match the registry checksum")
            if package_contents(local, name, version) != package_contents(published, name, version):
                raise SystemExit(f"{name} {version} exists with different contents; refusing to skip it")
        print(f"{name} {version} already published with matching contents")
        return
    run("cargo", "publish", "-p", name, "--locked", "--registry", "crates-io")


if __name__ == "__main__":
    publish("protocol/engineioxide/Cargo.toml")
    publish("protocol/socketioxide/Cargo.toml")
    publish("macros/Cargo.toml")
    publish("Cargo.toml")
