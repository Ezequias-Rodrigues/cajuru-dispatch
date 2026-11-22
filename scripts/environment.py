import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Literal, TypedDict, cast
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parent.parent
RUNTIME = ROOT / "deployment/runtime"
IMAGE = "cajuru-dispatch:local"
BROKER_IMAGE = "eclipse-mosquitto:2"
BROKER_PASSWORD = "cajuru123"
type Action = Literal["up", "down", "status", "disconnect", "reconnect"]

BROKER_COMMAND = "cp /mosquitto/config/passwords /tmp/passwords && chmod 600 /tmp/passwords && exec mosquitto -c /mosquitto/config/mosquitto.conf"

NODES: list[str] = [
    "cajuru-broker",
    "cajuru-central",
    "cajuru-amb-01",
    "cajuru-amb-02",
    "cajuru-amb-03",
]
MACS = {name: f"02:42:00:42:00:{index:02x}" for index, name in enumerate(NODES, 16)}
POSITIONS: dict[str, tuple[float, float]] = {
    "AMB-01": (-25.4386732, -49.23786),
    "AMB-02": (-25.4316563, -49.244012),
    "AMB-03": (-25.4406542, -49.249693),
}


class ContainerNetwork(TypedDict):
    ipv4Address: str


class ContainerStatus(TypedDict):
    networks: list[ContainerNetwork]


class ContainerInspection(TypedDict):
    status: ContainerStatus


class ContainerNetworkRecord(TypedDict, total=False):
    id: str
    name: str


class VehicleReadiness(TypedDict):
    online: bool


class Readiness(TypedDict):
    broker_connected: bool
    vehicles: list[VehicleReadiness]


def run(
    *args: str, check: bool = True, capture: bool = False
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args, check=check, text=True, capture_output=capture, cwd=ROOT
    )


def inspect(name: str) -> ContainerInspection:
    return cast(
        list[ContainerInspection],
        json.loads(run("container", "inspect", name, capture=True).stdout),
    )[0]


def address(name: str) -> str:
    return inspect(name)["status"]["networks"][0]["ipv4Address"].split("/")[0]


def configure_broker(engine: Literal["container", "podman"] = "container") -> None:
    RUNTIME.mkdir(parents=True, exist_ok=True)
    for filename in ("mosquitto.conf", "acl"):
        shutil.copyfile(ROOT / "deployment" / filename, RUNTIME / filename)
    (RUNTIME / "passwords").write_text(
        "".join(
            f"{user}:{BROKER_PASSWORD}\n"
            for user in ("central", *POSITIONS, "observer")
        )
    )
    (RUNTIME / "passwords").chmod(0o600)
    run(
        engine,
        "run",
        "--rm",
        *(["--security-opt", "label=disable"] if engine == "podman" else []),
        "--volume",
        f"{RUNTIME}:/mosquitto/config",
        f"docker.io/library/{BROKER_IMAGE}" if engine == "podman" else BROKER_IMAGE,
        "mosquitto_passwd",
        "-U",
        "/mosquitto/config/passwords",
    )
    (RUNTIME / "passwords").chmod(0o644)


def down() -> None:
    for name in reversed(NODES):
        run("container", "stop", name, check=False, capture=True)
        run("container", "delete", name, check=False, capture=True)


def up(build: bool) -> None:
    run("container", "system", "start")
    if build:
        run(
            "container",
            "build",
            "--progress",
            "plain",
            "--cpus",
            "4",
            "--memory",
            "4G",
            "-f",
            "Dockerfile",
            "-t",
            IMAGE,
            ".",
        )
    networks = cast(
        list[ContainerNetworkRecord],
        json.loads(
            run("container", "network", "list", "--format", "json", capture=True).stdout
        ),
    )
    if not any(
        network.get("id") == "cajuru" or network.get("name") == "cajuru"
        for network in networks
    ):
        run("container", "network", "create", "cajuru", "--subnet", "192.168.66.0/24")
    down()
    configure_broker()
    run(
        "container",
        "run",
        "-d",
        "--name",
        "cajuru-broker",
        "--network",
        f"cajuru,mac={MACS['cajuru-broker']}",
        "--cpus",
        "1",
        "--memory",
        "256M",
        "--user",
        "1883:1883",
        "--publish",
        "192.168.66.1:1883:1883",
        "--volume",
        f"{RUNTIME}:/mosquitto/config:ro",
        "--entrypoint",
        "/bin/sh",
        BROKER_IMAGE,
        "-c",
        BROKER_COMMAND,
    )
    broker = "192.168.66.1"
    for name in NODES[1:]:
        vehicle_id = (
            "central"
            if name == "cajuru-central"
            else name.removeprefix("cajuru-").upper()
        )
        env_file = RUNTIME / f"{name}.env"
        content = f"BROKER_HOST={broker}\nBROKER_PORT=1883\nBROKER_USERNAME={vehicle_id}\nBROKER_PASSWORD={BROKER_PASSWORD}\nVEHICLE_ID={vehicle_id}\n"
        if vehicle_id in POSITIONS:
            lat, lon = POSITIONS[vehicle_id]
            content += f"VEHICLE_LAT={lat}\nVEHICLE_LON={lon}\n"
        env_file.write_text(content)
        env_file.chmod(0o600)
        args = [
            "container",
            "run",
            "-d",
            "--name",
            name,
            "--network",
            f"cajuru,mac={MACS[name]}",
            "--cpus",
            "1",
            "--memory",
            "1G",
            "--env-file",
            str(env_file),
            "--volume",
            f"{ROOT}:/workspace:ro",
        ]
        if name == "cajuru-central":
            args += ["--publish", "127.0.0.1:8000:8000"]
        args += (
            [IMAGE]
            if name == "cajuru-central"
            else [IMAGE, "python", "-m", "src.vehicle"]
        )
        run(*args)
    wait_for_ready()
    topology()
    print("Painel: http://127.0.0.1:8000")


def wait_for_ready() -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            with urlopen("http://127.0.0.1:8000/api/state", timeout=2) as response:
                state = cast(Readiness, json.load(response))
                if (
                    state.get("broker_connected")
                    and len(state.get("vehicles", [])) == 3
                    and all(vehicle.get("online") for vehicle in state["vehicles"])
                ):
                    break
        except OSError:
            pass
        time.sleep(1)
    else:
        raise RuntimeError(
            "A central não respondeu em 60 segundos. Verifique os logs da central."
        )


def topology() -> None:
    rows = [
        {
            "node": name,
            "ip": address(name),
            "mac": MACS[name],
            "role": "broker MQTT"
            if name.endswith("broker")
            else "central HTTP + MQTT"
            if name.endswith("central")
            else "cliente MQTT",
        }
        for name in NODES
    ]
    print(
        json.dumps(
            {
                "runtime": "Apple container",
                "subnet": "192.168.66.0/24",
                "mask": "255.255.255.0",
                "gateway": "192.168.66.1",
                "mqtt_endpoint": "192.168.66.1:1883",
                "addressing": "DHCP nos nós; publicação MQTT estável no gateway da rede",
                "ports": {"mqtt": 1883, "http": 8000},
                "nodes": rows,
            },
            indent=2,
            ensure_ascii=False,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action", choices=["up", "down", "status", "disconnect", "reconnect"]
    )
    parser.add_argument(
        "--runtime",
        choices=["container", "incus"],
        default="container" if sys.platform == "darwin" else "incus",
    )
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--vehicle", choices=list(POSITIONS), default="AMB-01")
    args = parser.parse_args()
    if not shutil.which(args.runtime):
        parser.error(f"Comando não encontrado: {args.runtime}")
    if args.runtime == "incus":
        from incus_environment import execute

        execute(cast(Action, args.action), args.build, args.vehicle)
    elif args.action == "up":
        up(args.build)
    elif args.action == "down":
        down()
    elif args.action == "status":
        topology()
    else:
        run(
            "container",
            "stop" if args.action == "disconnect" else "start",
            f"cajuru-{args.vehicle.lower()}",
        )
        print(
            f"{args.vehicle}: {'desconectada' if args.action == 'disconnect' else 'reconectada'}"
        )


if __name__ == "__main__":
    main()
