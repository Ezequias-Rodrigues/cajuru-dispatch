import json
import platform
import tarfile
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from environment import run


def import_image(image: str, alias: str) -> None:
    """Transfer a Podman image into Incus without publishing it to a registry."""
    container_id = run(
        "podman", "create", "--network=none", image, capture=True
    ).stdout.strip()
    try:
        run("podman", "init", container_id)
        spec_path = run(
            "podman",
            "inspect",
            "--format",
            "{{.OCIConfigPath}}",
            container_id,
            capture=True,
        ).stdout.strip()
        spec: dict[str, object] = json.loads(Path(spec_path).read_text())
        spec["root"] = {"path": "rootfs"}
        metadata = {
            "architecture": platform.machine(),
            "creation_date": int(time.time()),
            "properties": {"type": "oci", "description": alias},
        }
        with TemporaryDirectory(prefix="cajuru-image-") as directory:
            output = Path(directory)
            (output / "config.json").write_text(json.dumps(spec))
            (output / "metadata.yaml").write_text(json.dumps(metadata))
            with tarfile.open(output / "metadata.tar", "w") as archive:
                for filename in ("config.json", "metadata.yaml"):
                    archive.add(output / filename, arcname=filename)
            run(
                "podman", "export", "--output", str(output / "rootfs.tar"), container_id
            )
            run(
                "incus",
                "image",
                "import",
                str(output / "metadata.tar"),
                str(output / "rootfs.tar"),
                "--alias",
                alias,
                "--reuse",
            )
    finally:
        run("podman", "rm", "--force", container_id, check=False, capture=True)
