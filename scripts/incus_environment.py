import json
import shlex
import shutil
from typing import TypedDict, cast

from environment import (
    BROKER_COMMAND,
    BROKER_PASSWORD,
    NODES,
    POSITIONS,
    ROOT,
    RUNTIME,
    Action,
    configure_broker,
    run,
    wait_for_ready,
)
from oci_image import import_image

APP_IMAGE = "cajuru-dispatch"
BROKER_IMAGE = "cajuru-mosquitto"
NETWORK = "cajuru"


class Network(TypedDict):
    name: str
    config: dict[str, str]


class Address(TypedDict):
    family: str
    scope: str
    address: str


class Interface(TypedDict):
    addresses: list[Address]


class State(TypedDict):
    network: dict[str, Interface] | None


class Instance(TypedDict):
    name: str
    status: str
    state: State | None


def down() -> None:
    for name in reversed(NODES):
        run("incus", "delete", name, "--force", check=False, capture=True)


def build_images() -> None:
    run(
        "podman",
        "build",
        "-f",
        "Dockerfile",
        "-t",
        "localhost/cajuru-dispatch:local",
        ".",
    )
    import_image("localhost/cajuru-dispatch:local", APP_IMAGE)
    run("podman", "pull", "docker.io/library/eclipse-mosquitto:2")
    import_image("docker.io/library/eclipse-mosquitto:2", BROKER_IMAGE)


def up(build: bool) -> None:
    if not shutil.which("podman"):
        raise RuntimeError(
            "Instale Podman para preparar as imagens do Dockerfile no Linux."
        )
    networks = cast(
        list[Network],
        json.loads(
            run("incus", "network", "list", "--format", "json", capture=True).stdout
        ),
    )
    network = next(
        (network for network in networks if network["name"] == NETWORK), None
    )
    if network is None:
        run(
            "incus",
            "network",
            "create",
            NETWORK,
            "ipv4.address=192.168.66.1/24",
            "ipv4.nat=true",
            "ipv6.address=none",
        )
    elif network["config"].get("ipv4.address") != "192.168.66.1/24":
        raise RuntimeError("A rede cajuru existente deve usar 192.168.66.1/24.")
    if build or any(
        run("incus", "image", "info", image, check=False, capture=True).returncode
        for image in (APP_IMAGE, BROKER_IMAGE)
    ):
        build_images()
    down()
    configure_broker("podman")
    for index, name in enumerate(NODES, 10):
        broker = name == "cajuru-broker"
        run(
            "incus",
            "init",
            BROKER_IMAGE if broker else APP_IMAGE,
            name,
            "--network",
            NETWORK,
            "-c",
            "limits.cpu=1",
            "-c",
            "limits.memory=1GiB",
        )
        run(
            "incus",
            "config",
            "device",
            "set",
            name,
            "eth0",
            f"ipv4.address=192.168.66.{index}",
        )
        run(
            "incus",
            "config",
            "device",
            "add",
            name,
            "workspace",
            "disk",
            f"source={RUNTIME if broker else ROOT}",
            f"path={'/mosquitto/config' if broker else '/workspace'}",
            "readonly=true",
        )
        settings: dict[str, str]
        if broker:
            settings = {
                "oci.uid": "1883",
                "oci.gid": "1883",
                "oci.entrypoint": shlex.join(["/bin/sh", "-c", BROKER_COMMAND]),
            }
        else:
            username = (
                "central"
                if name == "cajuru-central"
                else name.removeprefix("cajuru-").upper()
            )
            settings = {
                "environment.BROKER_HOST": "192.168.66.1",
                "environment.BROKER_PORT": "1883",
                "environment.BROKER_USERNAME": username,
                "environment.BROKER_PASSWORD": BROKER_PASSWORD,
                "environment.VEHICLE_ID": username,
            }
            if username in POSITIONS:
                latitude, longitude = POSITIONS[username]
                settings.update(
                    {
                        "environment.VEHICLE_LAT": str(latitude),
                        "environment.VEHICLE_LON": str(longitude),
                        "oci.entrypoint": "/opt/venv/bin/python -m src.vehicle",
                    }
                )
        run(
            "incus",
            "config",
            "set",
            name,
            *(f"{key}={value}" for key, value in settings.items()),
        )
        if broker or name == "cajuru-central":
            run(
                "incus",
                "config",
                "device",
                "add",
                name,
                "mqtt" if broker else "http",
                "proxy",
                f"listen=tcp:{'192.168.66.1:1883' if broker else '127.0.0.1:8000'}",
                f"connect=tcp:127.0.0.1:{1883 if broker else 8000}",
            )
        run("incus", "start", name)
    wait_for_ready()
    topology()
    print("Painel: http://127.0.0.1:8000")


def topology() -> None:
    instances = cast(
        list[Instance],
        json.loads(run("incus", "list", "--format", "json", capture=True).stdout),
    )
    nodes: list[dict[str, object]] = []
    for name in NODES:
        instance = next((item for item in instances if item["name"] == name), None)
        state = instance["state"] if instance else None
        interfaces = (state["network"] or {}) if state else {}
        addresses = [
            address["address"]
            for interface in interfaces.values()
            for address in interface["addresses"]
            if address["family"] == "inet" and address["scope"] == "global"
        ]
        nodes.append(
            {
                "node": name,
                "status": instance["status"] if instance else "Not found",
                "ipv4": addresses,
            }
        )
    print(
        json.dumps(
            {
                "runtime": "Incus",
                "subnet": "192.168.66.0/24",
                "gateway": "192.168.66.1",
                "mqtt_endpoint": "192.168.66.1:1883",
                "addressing": "Reservas DHCP por nó",
                "ports": {"mqtt": 1883, "http": 8000},
                "nodes": nodes,
            },
            indent=2,
            ensure_ascii=False,
        )
    )


def execute(action: Action, build: bool, vehicle: str) -> None:
    if action == "up":
        up(build)
    elif action == "down":
        down()
    elif action == "status":
        topology()
    else:
        command = [
            "incus",
            "stop" if action == "disconnect" else "start",
            f"cajuru-{vehicle.lower()}",
        ]
        if action == "disconnect":
            command.append("--force")
        run(*command)
        print(
            f"{vehicle}: {'desconectada' if action == 'disconnect' else 'reconectada'}"
        )
