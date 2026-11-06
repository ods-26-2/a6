"""
servico_avatar.py
=================
Servico de A6 que fica ESCUTANDO o barramento Kafka e gera o video .mp4 do avatar
quando recebe o aviso de que a trajetoria foi enviada.

Fluxo (padrao):
    ods.s6.synthesis.reply  (status OK + trajectory_ref)
        -> baixa o JSON para avatar/entry/
        -> chama avatar/generate_video.py (ambiente Blender)
        -> publica ods.a6.avatar.render.reply (caminho do .mp4),
           com o MESMO correlation_id do comando enviado ao S6

Disparo manual opcional (ex.: re-renderizar uma trajetoria):
    ods.a6.avatar.render.command   payload: {"trajectory_ref": "s6://trajectories/xxx.json"}
                                            (sem trajectory_ref: renderiza o que ja estiver em entry/)

Topicos proprios de A6 (precisam existir no broker ou ter auto-criacao ligada):
    ods.a6.avatar.render.command / ods.a6.avatar.render.reply
    payload da reply: {"status": "OK"|"ERROR", "videos": [...], "error": {...}}

ATENCAO: por padrao QUALQUER reply do S6 com trajetoria gera um video, inclusive as pedidas
por outros componentes (ex.: servidor_a6.py). Use --sem-s6-reply para reagir so ao comando manual.

Como o JSON e baixado a partir do trajectory_ref ("s6://trajectories/<arquivo>.json"):
    1. pasta local: S6_TRAJECTORY_DIR, ./trajectories_mock/ (mock do S6);
    2. HTTP(S): S6_TRAJECTORY_BASE_URL + "/<arquivo>.json".
    O endereco real de download do S6 precisa ser confirmado com o time do S6.

Uso (ambiente `generic`):
    python servico_avatar.py
    python servico_avatar.py --sem-s6-reply     # so renderiza por ods.a6.avatar.render.command
    BLENDER_PYTHON=envs/blender/bin/python python servico_avatar.py
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka_integration import (
    TOPIC_S6_SYNTHESIS_RPL,
    build_ods_envelope,
    get_bootstrap_servers,
    validate_ods_envelope,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("a6.avatar.servico")

AVATAR_DIR = Path(__file__).resolve().parent
BASE_DIR = AVATAR_DIR.parent

TOPIC_A6_AVATAR_CMD = "ods.a6.avatar.render.command"
TOPIC_A6_AVATAR_RPL = "ods.a6.avatar.render.reply"

_NOME_VALIDO = re.compile(r"^[A-Za-z0-9_.\-]+\.json$")


# --------------------------------------------------------------------------
# Baixar a trajetoria a partir do trajectory_ref
# --------------------------------------------------------------------------
def nome_do_arquivo(ref: str) -> str:
    """'s6://trajectories/traj_abc.json' -> 'traj_abc.json' (valida para evitar path traversal)."""
    nome = str(ref).strip().rstrip("/").rsplit("/", 1)[-1]
    if not _NOME_VALIDO.match(nome) or nome.startswith("."):
        raise ValueError(f"trajectory_ref invalido: {ref!r}")
    return nome


def buscar_trajetoria(ref: str, entry_dir: Path) -> Path:
    """Copia/baixa a trajetoria para `entry_dir` e retorna o caminho do arquivo."""
    nome = nome_do_arquivo(ref)
    entry_dir.mkdir(parents=True, exist_ok=True)
    alvo = entry_dir / nome

    if alvo.is_file():
        return alvo

    # 1) pastas locais (S6 e mock gravam arquivos de trajetoria em disco)
    candidatos = [
        os.environ.get("S6_TRAJECTORY_DIR"),
        BASE_DIR / "trajectories_mock",
        AVATAR_DIR / "trajectories_mock",
        BASE_DIR / "trajectories",
        BASE_DIR.parent / "trajectories",
        BASE_DIR.parent / "s6" / "trajectories",
        BASE_DIR.parent / "S6" / "trajectories",
        BASE_DIR.parent / "s6_synthesis" / "trajectories",
    ]
    for pasta in candidatos:
        if pasta and (Path(pasta) / nome).is_file():
            shutil.copy2(Path(pasta) / nome, alvo)
            log.info("Trajetoria copiada de %s", Path(pasta) / nome)
            return alvo

    # 2) HTTP(S) configurado por variavel de ambiente
    base_url = os.environ.get("S6_TRAJECTORY_BASE_URL")
    if base_url and base_url.startswith(("http://", "https://")):
        url = base_url.rstrip("/") + "/" + nome
        log.info("Baixando trajetoria de %s", url)
        with urllib.request.urlopen(url, timeout=60) as resp:  # noqa: S310 (URL base vem de config)
            alvo.write_bytes(resp.read())
        return alvo

    # 3) Fallback Mock local: gera o JSON de trajetoria em `entry_dir` caso venha de reply sem servidor S6 local
    try:
        from mock_s6_e_i4 import MockS6AndI4Service
        mock_srv = MockS6AndI4Service(trajectory_dir=entry_dir)
        frames_out = [mock_srv._sample_frames[i % len(mock_srv._sample_frames)] for i in range(30)]
        doc = {
            "fps": 25.0,
            "frame_count": len(frames_out),
            "coordinates": "NORMALIZED",
            "frames": frames_out,
        }
        alvo.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
        log.info("Trajetoria mock gerada localmente em %s", alvo)
        return alvo
    except Exception as exc:
        log.warning("Fallback mock falhou: %s", exc)

    raise FileNotFoundError(
        f"Nao consegui obter '{ref}'. Defina S6_TRAJECTORY_DIR (pasta local) ou "
        f"S6_TRAJECTORY_BASE_URL (endereco HTTP do S6)."
    )


# --------------------------------------------------------------------------
# Render (subprocesso no ambiente Blender)
# --------------------------------------------------------------------------
def blender_python_padrao() -> str:
    env = os.environ.get("BLENDER_PYTHON")
    if env:
        return env
    candidatos = [
        BASE_DIR / ".venv" / "blender" / "Scripts" / "python.exe",
        BASE_DIR / ".venv" / "blender" / "bin" / "python",
        BASE_DIR / "envs" / "blender" / "Scripts" / "python.exe",
        BASE_DIR / "envs" / "blender" / "bin" / "python",
    ]
    for c in candidatos:
        if c.exists() or c.is_symlink():
            return str(c)
    return str(BASE_DIR / ".venv" / "blender" / "bin" / "python")


def renderizar(arquivo: Optional[str], cfg: argparse.Namespace) -> Dict[str, Any]:
    """Roda generate_video.py. Retorna {'returncode', 'videos': [...], 'tail': [...]}."""
    cmd = [cfg.blender_python, str(AVATAR_DIR / "generate_video.py"),
           "--entry", str(cfg.entry), "--saida", str(cfg.saida), "--model", str(cfg.model)]
    if arquivo:
        cmd += ["--arquivo", arquivo]
    if cfg.extra:
        cmd += cfg.extra

    log.info("Executando: %s", " ".join(cmd))
    videos: List[str] = []
    tail: List[str] = []
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, bufsize=1)
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip("\n")
        print("   |", line, flush=True)
        tail = (tail + [line])[-15:]
        if line.startswith("GLB:") or line.startswith("VIDEO:"):
            v = line.split(":", 1)[1].strip()
            if v not in videos:
                videos.append(v)
    rc = proc.wait()
    return {"returncode": rc, "videos": videos, "tail": tail}


def processar_comando(payload: Dict[str, Any], cfg: argparse.Namespace) -> Dict[str, Any]:
    """Executa um pedido de render e devolve o payload da reply."""
    ref = payload.get("trajectory_ref")
    t0 = time.time()
    arquivo = None
    try:
        if ref:
            arquivo = buscar_trajetoria(ref, cfg.entry).name
    except (ValueError, FileNotFoundError, OSError) as exc:
        log.error("Falha ao obter a trajetoria: %s", exc)
        return {"status": "ERROR", "trajectory_ref": ref,
                "error": {"code": "TRAJECTORY_UNAVAILABLE", "message": str(exc)}}

    res = renderizar(arquivo, cfg)
    if res["returncode"] != 0 or not res["videos"]:
        return {"status": "ERROR", "trajectory_ref": ref,
                "error": {"code": "RENDER_FAILED",
                          "message": "generate_video.py falhou ou nao gerou video",
                          "detail": res["tail"]}}
    return {
        "status": "OK",
        "trajectory_ref": ref,
        "videos": [{"file": Path(v).name, "path": v} for v in res["videos"]],
        "render_seconds": round(time.time() - t0, 1),
    }


# --------------------------------------------------------------------------
# Loop Kafka
# --------------------------------------------------------------------------
class ServicoAvatar:
    def __init__(self, cfg: argparse.Namespace):
        self.cfg = cfg
        self.running = False
        self.seen: set[str] = set()
        self.rendered: set[str] = set()   # trajectory_refs ja renderizados (evita video duplicado)
        self.consumer = None
        self.producer = None

    def initialize(self):
        from confluent_kafka import Consumer, Producer
        bootstrap = get_bootstrap_servers()
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": self.cfg.group_id,
            "auto.offset.reset": os.environ.get("KAFKA_AUTO_OFFSET_RESET", "latest"),
            "enable.auto.commit": True,
            # render pode demorar minutos entre dois polls: evita rebalance por inatividade
            "max.poll.interval.ms": 3_600_000,
        })
        self.producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 10})
        topics = [TOPIC_A6_AVATAR_CMD] + ([TOPIC_S6_SYNTHESIS_RPL] if self.cfg.on_s6_reply else [])
        self.consumer.subscribe(topics)
        log.info("Escutando %s em %s (entry=%s)", topics, bootstrap, self.cfg.entry)

    def stop(self, *_):
        log.info("Encerrando servico de avatar...")
        self.running = False

    def _publicar_reply(self, correlation_id: str, session_id: Optional[str], payload: Dict[str, Any]):
        env = build_ods_envelope(
            message_type="reply", schema=TOPIC_A6_AVATAR_RPL, producer="A6",
            session_id=session_id, correlation_id=correlation_id, payload=payload,
        )
        self.producer.produce(TOPIC_A6_AVATAR_RPL, key=correlation_id.encode("utf-8"),
                              value=json.dumps(env, ensure_ascii=False).encode("utf-8"))
        self.producer.flush(5.0)

    def tratar(self, raw: bytes):
        try:
            msg = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            log.warning("Mensagem descartada: JSON invalido")
            return
        ok, why = validate_ods_envelope(msg)
        if not ok:
            log.warning("Mensagem descartada: %s", why)
            return
        mid = msg["message_id"]
        if mid in self.seen:
            return
        self.seen.add(mid)

        schema, payload = msg["schema"], msg["payload"]
        if schema == TOPIC_A6_AVATAR_CMD:
            log.info("Comando de render recebido (message_id=%s): %s", mid, payload)
        elif schema == TOPIC_S6_SYNTHESIS_RPL:
            if payload.get("status") != "OK" or not payload.get("trajectory_ref"):
                return
            ref = payload["trajectory_ref"]
            corr_id = msg.get("correlation_id") or mid
            if ref in self.rendered or corr_id in self.rendered:
                log.info("Trajetoria %s (correlation_id=%s) ja renderizada, ignorando reply repetida.", ref, corr_id)
                return
            log.info("Reply do S6 com trajetoria: %s", ref)
            payload = {"trajectory_ref": ref}
            mid = corr_id   # responde ao comando original enviado ao S6
        else:
            return

        reply = processar_comando(payload, self.cfg)
        self._publicar_reply(mid, msg.get("session_id"), reply)
        if reply["status"] == "OK":
            self.rendered.add(mid)
            if payload.get("trajectory_ref"):
                self.rendered.add(payload["trajectory_ref"])
            for v in reply["videos"]:
                log.info("ARTEFATO (.GLB) GERADO: %s", v["path"])
        else:
            log.error("Render falhou: %s", reply["error"])

    def run(self):
        from confluent_kafka import KafkaError
        self.initialize()
        self.running = True
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("Erro Kafka: %s", msg.error())
                    continue
                self.tratar(msg.value())
        finally:
            if self.producer:
                self.producer.flush(5.0)
            if self.consumer:
                self.consumer.close()
            log.info("Servico de avatar encerrado.")


def main():
    ap = argparse.ArgumentParser(description="Escuta o Kafka e gera o mp4 do avatar.")
    ap.add_argument("--entry", type=Path, default=AVATAR_DIR / "entry", help="pasta de entrada (JSONs)")
    ap.add_argument("--saida", type=Path, default=AVATAR_DIR / "saida", help="pasta dos .mp4")
    ap.add_argument("--model", type=Path, default=AVATAR_DIR / "ybot.fbx", help="avatar .fbx/.glb")
    ap.add_argument("--blender-python", default=blender_python_padrao(),
                    help="Python do ambiente Blender (ou variavel BLENDER_PYTHON)")
    ap.add_argument("--group-id", default="A6-avatar-video")
    ap.add_argument("--sem-s6-reply", action="store_true",
                    help="NAO renderiza ao receber ods.s6.synthesis.reply (so pelo comando ods.a6.avatar.render.command)")
    ap.add_argument("extra", nargs=argparse.REMAINDER,
                    help="argumentos extras para o generate_video.py (apos --), ex.: -- --no-mirror --focus hands")
    cfg = ap.parse_args()
    cfg.on_s6_reply = not cfg.sem_s6_reply
    if cfg.extra and cfg.extra[0] == "--":
        cfg.extra = cfg.extra[1:]

    if not Path(cfg.blender_python).exists():
        sys.exit(f"Python do Blender nao encontrado: {cfg.blender_python}\n"
                 f"Use --blender-python ou a variavel BLENDER_PYTHON.")
    ServicoAvatar(cfg).run()


if __name__ == "__main__":
    main()