"""
testar_com_s6.py
=================
Envia um comando `ods.s6.synthesis.command` real de A6 para o S6 via Kafka e aguarda
a resposta `ods.s6.synthesis.reply` do S6 no barramento.

Uso:
    python testar_com_s6.py
    python testar_com_s6.py --frase "O estudante desejar ir no banco?" --speed 1.0
"""

from __future__ import annotations

import argparse
import json
import time
import uuid

from confluent_kafka import Consumer, KafkaError, Producer
from kafka_integration import (
    TOPIC_S6_SYNTHESIS_CMD,
    TOPIC_S6_SYNTHESIS_RPL,
    build_ods_envelope,
    get_bootstrap_servers,
    validate_ods_envelope,
)
from sugestao_correcao_pipeline import LibrasCorrectionPipeline


def main():
    ap = argparse.ArgumentParser(description="Envia comando de síntese de A6 para S6 e aguarda a reply.")
    ap.add_argument(
        "--frase",
        default="O estudante desejar banco cinco?",
        help="Frase ou glosas a enviar para o S6 (passa pela pré-validação e sugestão de A6 antes)",
    )
    ap.add_argument("--speed", type=float, default=1.0, help="speed_factor (ex: 0.8, 1.0, 1.5)")
    ap.add_argument("--timeout", type=int, default=20, help="Segundos aguardando a reply do S6")
    args = ap.parse_args()

    bootstrap = get_bootstrap_servers()
    pipeline = LibrasCorrectionPipeline()

    # 1. Pré-validação local em A6 ANTES de enviar ao S6
    prep = pipeline.prepare_synthesis_command(
        input_words_or_sentence=args.frase,
        speed_factor=args.speed,
        auto_apply_top_suggestion=True,
    )

    print("=" * 75)
    print(" [PASSO 1 - A6] PRÉ-VALIDAÇÃO & SUGESTÃO DE CORREÇÃO ANTES DO ENVIO")
    print("=" * 75)
    print(f" Frase de entrada : {args.frase}")
    if prep["pre_validation"]["word_corrections"]:
        for wc in prep["pre_validation"]["word_corrections"]:
            sugs = [f"{s['gloss']} ({s['score']})" for s in wc["suggestions"]]
            print(f" -> Sugestão aplicada: '{wc['original_word']}' -> '{wc['applied_gloss']}' (opções: {sugs})")
    else:
        print(" -> Todas as palavras já estão no LEXICON.")

    print(f" -> Ordem Gramatical Libras : {prep['grammar']['after_syntax_order']}")
    print(f" -> Marcador Facial         : {prep['grammar']['facial_marker_applied']}")

    # 2. Prepara Consumer para escutar `ods.s6.synthesis.reply` antes de enviar o comando
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"A6-s6-tester-{uuid.uuid4().hex[:6]}",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([TOPIC_S6_SYNTHESIS_RPL])
    # Faz um poll curto para garantir que a partição foi atribuída
    consumer.poll(1.0)

    # 3. Monta e publica `ods.s6.synthesis.command`
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 10})
    cmd_msg = build_ods_envelope(
        message_type="command",
        schema=TOPIC_S6_SYNTHESIS_CMD,
        target="S6",
        payload=prep["s6_command_payload"],
    )
    cmd_id = cmd_msg["message_id"]

    print("\n" + "=" * 75)
    print(f" [PASSO 2 - A6 -> S6] PUBLICANDO `ods.s6.synthesis.command`")
    print("=" * 75)
    print(json.dumps(cmd_msg, indent=2, ensure_ascii=False))

    producer.produce(
        topic=TOPIC_S6_SYNTHESIS_CMD,
        key=cmd_id.encode("utf-8"),
        value=json.dumps(cmd_msg, ensure_ascii=False).encode("utf-8"),
    )
    producer.flush(5.0)
    print(f"\n[OK] Comando enviado! Aguardando `ods.s6.synthesis.reply` do S6 (correlation_id={cmd_id}) por até {args.timeout}s...")

    # 4. Aguarda a reply do S6
    deadline = time.time() + args.timeout
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("Erro Kafka:", msg.error())
                continue

            reply_env = json.loads(msg.value().decode("utf-8"))
            if reply_env.get("correlation_id") == cmd_id:
                print("\n" + "=" * 75)
                print(" [PASSO 3 - S6 -> A6] REPLY RECEBIDA DO S6 (`ods.s6.synthesis.reply`)!")
                print("=" * 75)
                print(json.dumps(reply_env, indent=2, ensure_ascii=False))

                eval_rpl = pipeline.evaluate_s6_synthesis_reply(reply_env.get("payload", {}))
                if eval_rpl["has_unknown_signs"]:
                    print("\n [A6 FALLBACK] Sinais desconhecidos pelo S6:", eval_rpl["unknown_signs"])
                    print(" -> Sugestões de A6:", eval_rpl["suggestions_for_unknown_signs"])
                else:
                    print(f"\n [SUCESSO TOTAL] Trajetória gerada pelo S6 em: {eval_rpl['trajectory_ref']}")
                return

        print(
            "\n[AVISO] Tempo esgotado aguardando reply do S6.\n"
            "Certifique-se de que o serviço do S6 está rodando (`python -m s6_synthesis.bus_service`)."
        )
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

