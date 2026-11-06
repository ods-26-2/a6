"""
entrada_texto.py
================
Simula a entrada de texto do usuario pelo terminal e percorre todo o fluxo de A6:

    texto digitado
      -> separado em palavras (LibrasCorrectionPipeline.prepare_synthesis_command):
         remove palavras funcionais, identifica negacao/pergunta, compara cada palavra com
         as 100 glosas do LEXICON (sinonimos + radical + similaridade de texto com difflib),
         reordena para [TEMPO]+[SUJEITO]+[OBJETO]+[VERBO]
      -> ods.s6.synthesis.command   (para o time do S6)
      <- ods.s6.synthesis.reply     (trajectory_ref)
         ... o servico_avatar.py ouve essa mesma reply, baixa a trajetoria e gera o .mp4 ...
      <- ods.a6.avatar.render.reply (caminho do video; mesmo correlation_id do comando ao S6)

Uso (ambiente `generic`; deixe o servico_avatar.py rodando em outro terminal):
    python entrada_texto.py
    python entrada_texto.py --frase "O estudante desejar banco cinco?" --speed 1.0
    python entrada_texto.py --sem-video          # so ate a resposta do S6
    python entrada_texto.py --comando-render     # pede o render explicitamente (servico com --sem-s6-reply)
"""
from __future__ import annotations

import argparse
import json
import time
import uuid
import sys
import os
from typing import Any, Dict, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from confluent_kafka import Consumer, KafkaError, Producer

from kafka_integration import (
    TOPIC_S6_SYNTHESIS_CMD,
    TOPIC_S6_SYNTHESIS_RPL,
    build_ods_envelope,
    get_bootstrap_servers,
)
from servico_avatar import TOPIC_A6_AVATAR_CMD, TOPIC_A6_AVATAR_RPL
from sugestao_correcao_pipeline import LibrasCorrectionPipeline


def mostrar_preparacao(texto: str, prep: Dict[str, Any]):
    print("-" * 70)
    print(f" Palavras digitadas : {texto.replace('?', ' ?').replace('!', ' !').split()}")
    corr = prep["pre_validation"]["word_corrections"]
    if corr:
        for wc in corr:
            sugs = [f"{s['gloss']} ({s['score']})" for s in wc["suggestions"]]
            alvo = wc["applied_gloss"] or "sem correspondencia"
            print(f" Fora do LEXICON    : '{wc['original_word']}' -> {alvo}  (opcoes: {sugs})")
    else:
        print(" Todas as palavras ja estao no LEXICON.")
    if prep["pre_validation"]["unresolved_words"]:
        print(f" Sem correspondencia: {prep['pre_validation']['unresolved_words']}")
    g = prep["grammar"]
    print(f" Ordem gramatical   : {g['before_syntax_order']} -> {g['after_syntax_order']}")
    print(f" Marcador facial    : {g['facial_marker_applied']}")
    print("-" * 70)


class Sessao:
    """Consumer + producer da sessao de terminal."""

    def __init__(self):
        bootstrap = get_bootstrap_servers()
        self.pipeline = LibrasCorrectionPipeline()
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": f"A6-texto-{uuid.uuid4().hex[:6]}",
            "auto.offset.reset": "latest",
        })
        self.consumer.subscribe([TOPIC_S6_SYNTHESIS_RPL, TOPIC_A6_AVATAR_RPL])
        self.consumer.poll(1.0)   # garante a atribuicao das particoes antes de publicar
        self.producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 10})

    def publicar(self, topic: str, envelope: Dict[str, Any]):
        self.producer.produce(topic, key=envelope["message_id"].encode("utf-8"),
                              value=json.dumps(envelope, ensure_ascii=False).encode("utf-8"))
        self.producer.flush(5.0)

    def aguardar_reply(self, schema: str, correlation_id: str, timeout: float) -> Optional[Dict[str, Any]]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("Erro Kafka:", msg.error())
                continue
            try:
                env = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if env.get("schema") == schema and env.get("correlation_id") == correlation_id:
                return env
        return None

    def fechar(self):
        self.consumer.close()


def processar_frase(s: Sessao, texto: str, speed: float, timeout_s6: float,
                    timeout_video: float, com_video: bool, comando_render: bool = False):
    prep = s.pipeline.prepare_synthesis_command(
        input_words_or_sentence=texto, speed_factor=speed, auto_apply_top_suggestion=True)
    mostrar_preparacao(texto, prep)

    if not prep["s6_command_payload"]["glosses"]:
        print(" Nenhuma glosa valida para enviar ao S6. Tente outra frase.")
        return

    cmd = build_ods_envelope(message_type="command", schema=TOPIC_S6_SYNTHESIS_CMD,
                             target="S6", payload=prep["s6_command_payload"])
    s.publicar(TOPIC_S6_SYNTHESIS_CMD, cmd)
    print(f" [A6 -> S6] comando enviado: glosas={prep['s6_command_payload']['glosses']}")
    print(f" Aguardando a trajetoria do S6 (ate {timeout_s6:.0f}s)...")

    rpl = s.aguardar_reply(TOPIC_S6_SYNTHESIS_RPL, cmd["message_id"], timeout_s6)
    if rpl is None:
        print(" [AVISO] Tempo esgotado sem reply do S6 (o S6 ou o mock esta rodando?).")
        return

    payload = rpl.get("payload", {})
    avaliacao = s.pipeline.evaluate_s6_synthesis_reply(payload)
    if avaliacao["has_unknown_signs"]:
        print(" [S6] sinais desconhecidos:", avaliacao["unknown_signs"])
        print(" -> sugestoes de A6:", avaliacao["suggestions_for_unknown_signs"])
    if avaliacao["status"] != "OK" or not avaliacao["trajectory_ref"]:
        print(f" [S6] sem trajetoria. status={avaliacao['status']} erro={payload.get('error')}")
        return
    print(f" [S6 -> A6] trajetoria: {avaliacao['trajectory_ref']} ({avaliacao['duration_ms']} ms)")

    if not com_video:
        return

    if comando_render:
        render = build_ods_envelope(message_type="command", schema=TOPIC_A6_AVATAR_CMD,
                                    target="A6-AVATAR",
                                    payload={"trajectory_ref": avaliacao["trajectory_ref"]})
        s.publicar(TOPIC_A6_AVATAR_CMD, render)
        correlacao = render["message_id"]
        print(f" [A6] render pedido explicitamente -> {TOPIC_A6_AVATAR_CMD}")
    else:
        correlacao = cmd["message_id"]   # o servico renderiza ao ouvir a reply do S6
        print(" [A6] o servico_avatar.py gera o .glb animado ao receber a reply do S6.")
    print(f" Aguardando o avatar .glb (ate {timeout_video:.0f}s)...")
    out = s.aguardar_reply(TOPIC_A6_AVATAR_RPL, correlacao, timeout_video)
    if out is None:
        print(" [AVISO] Sem resposta do servico_avatar.py (ele esta rodando?).")
        return
    p = out.get("payload", {})
    if p.get("status") == "OK":
        for v in p.get("videos", []):
            print(f" [AVATAR (.GLB) PRONTO] {v['path']}")
    else:
        print(f" [ERRO NO RENDER] {p.get('error')}")


def main():
    ap = argparse.ArgumentParser(description="Entrada de texto no terminal -> S6 -> video do avatar.")
    ap.add_argument("--frase", help="envia so esta frase (sem prompt interativo)")
    ap.add_argument("--speed", type=float, default=1.0, help="speed_factor (ex: 0.8, 1.0, 1.5)")
    ap.add_argument("--timeout-s6", type=float, default=30)
    ap.add_argument("--timeout-video", type=float, default=1800)
    ap.add_argument("--sem-video", action="store_true", help="nao espera o video do servico_avatar.py")
    ap.add_argument("--comando-render", action="store_true",
                    help="publica ods.a6.avatar.render.command (use se o servico roda com --sem-s6-reply)")
    a = ap.parse_args()

    sessao = Sessao()
    try:
        if a.frase:
            processar_frase(sessao, a.frase, a.speed, a.timeout_s6, a.timeout_video,
                            not a.sem_video, a.comando_render)
            return
        print("Digite uma frase em portugues e tecle Enter ('sair' para encerrar).")
        while True:
            try:
                texto = input("\nFrase> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if texto.lower() in {"sair", "exit", "quit"}:
                break
            if texto:
                processar_frase(sessao, texto, a.speed, a.timeout_s6, a.timeout_video,
                                not a.sem_video, a.comando_render)
    finally:
        sessao.fechar()


if __name__ == "__main__":
    main()