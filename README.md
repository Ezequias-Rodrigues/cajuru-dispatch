# Cajuru Dispatch

Simulação de uma central com três ambulâncias na região da PUCPR e do Hospital Cajuru. A central escolhe o veículo disponível com o menor percurso pelas ruas. Cinco contêineres Linux se comunicam por MQTT e HTTP.

## Executar

Execute os comandos na raiz do repositório. No macOS, instale **uv** e **Apple container**.

```sh
uv run scripts/environment.py up --runtime container --build
```

No Linux, instale **uv**, **Podman** e inicialize o **Incus** com `incus admin init`.

```sh
uv run scripts/environment.py up --runtime incus --build
```

Omita `--build` para reutilizar a imagem existente.

Abra <http://127.0.0.1:8000> e clique em **Ocorrência aleatória**. O botão **Limpar**, na linha do tempo, apaga o histórico de eventos sem interromper os atendimentos. A API pode ser consultada em <http://127.0.0.1:8000/docs>.

## Controlar o ambiente

```sh
uv run scripts/environment.py status
uv run scripts/environment.py disconnect --vehicle AMB-01
uv run scripts/environment.py reconnect --vehicle AMB-01
uv run scripts/environment.py down
```

A desconexão para o contêiner escolhido. Quando a reserva da missão vence, a ocorrência volta à fila. Encerrar a central elimina o estado mantido em memória.

MQTT em `192.168.66.1:1883`. As contas `central`, `AMB-01`, `AMB-02`, `AMB-03` e `observer` usam a senha de demonstração `cajuru123`.

Dados viários do [OpenStreetMap](https://www.openstreetmap.org/copyright), sob licença ODbL.
