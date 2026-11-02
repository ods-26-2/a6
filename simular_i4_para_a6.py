"""
simular_i4_para_a6.py
======================
Simulação simples e realista de captura em tempo real vinda de I4 -> A6 via Kafka:
Envia vários símbolos (glosas) em sequência e demonstra como o A6 identifica
automaticamente quando os símbolos fazem parte da MESMA FRASE (mesmo contexto)
e quando uma frase ACABOU e iniciou OUTRO CONTEXTO!

Cenário simulado:
- CONTEXTO 1 (Frase 1 — intervalos curtos de ~0.5s entre sinais):
    1. Sinal 'ALUNO'   (00.0s -> 01.2s, conf=0.91)
    2. Sinal 'ESTUDAR' (01.6s -> 02.8s, conf=0.89)
    3. Sinal 'ONTEM'   (03.1s -> 04.2s, conf=0.58 - baixa confiança, sugere 'HOJE')
    -> A6 agrega os 3 sinais no mesmo contexto (`frase-001`) e reordena gramaticalmente:
       ['ONTEM', 'ALUNO', 'ESTUDAR']

- PAUSA LONGA DO USUÁRIO (5.5 segundos entre o fim de 'ONTEM' e o início do próximo sinal):
    -> Quando chega o próximo sinal (em 09.7s), o A6 detecta que a pausa (5.5s >= 3.0s)
       encerrou a Frase 1 (`frase-001`) e abre automaticamente um NOVO CONTEXTO (`frase-002`)!

- CONTEXTO 2 (Frase 2 — nova frase + marcador facial interrogativo):
    4. Sinal 'VOCE'    (09.7s -> 10.8s, conf=0.94)
    5. Sinal 'SUCO'    (11.2s -> 12.3s, conf=0.88)
    6. Sinal 'QUERER'  (12.6s -> 13.8s, conf=0.90)
    7. Marcador Facial 'INTERROGATIVA_SN' (10.0s -> 13.8s)
    -> Quando o usuário para de sinalizar (~3.5s de silêncio), o A6 fecha automaticamente
       a Frase 2 (`frase-002`)!
"""

from __future__ import annotations

import json
import time
import uuid

from confluent_kafka import Producer
from kafka_integration import (
    TOPIC_I4_FACIAL_MARKER,
    TOPIC_I4_SIGN,
    build_ods_envelope,
    get_bootstrap_servers,
)


def publicar_sinal_i4(
    producer: Producer,
    session_id: str,
    sign: str,
    confidence: float,
    start_at: str,
    end_at: str,
    alternatives: list | None = None,
    signer_id: str = "signer-01",
):
    env = build_ods_envelope(
        message_type="event",
        schema=TOPIC_I4_SIGN,
        producer="I4",
        session_id=session_id,
        payload={
            "sign": sign,
            "confidence": confidence,
            "start_at": start_at,
            "end_at": end_at,
            "alternatives": alternatives or [],
            "signer_id": signer_id,
        },
    )
    producer.produce(
        TOPIC_I4_SIGN,
        key=session_id.encode("utf-8"),
        value=json.dumps(env, ensure_ascii=False).encode("utf-8"),
    )
    producer.flush(2.0)


def main():
    bootstrap = get_bootstrap_servers()
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 10})
    session_id = f"sessao-realtime-{uuid.uuid4().hex[:6]}"

    print("=" * 78)
    print(" SIMULAÇÃO EM TEMPO REAL: AGREGAÇÃO DE FRASES E DETECÇÃO DE CONTEXTO (I4 -> A6)")
    print(f" Broker: {bootstrap} | Session ID: {session_id}")
    print("=" * 78)

    # =========================================================================
    # CONTEXTO 1 (FRASE 1): Sinais contínuos com intervalo curto (< 0.5s)
    # =========================================================================
    print("\n--- [ENVIANDO CONTEXTO 1: Sinais seguidos na mesma frase] ---")

    print(" 1) Enviando sinal 'ALUNO' (t=0.0s..1.2s, conf=0.91)...")
    publicar_sinal_i4(
        producer, session_id,
        sign="ALUNO", confidence=0.91,
        start_at="2026-11-02T17:00:00.000Z", end_at="2026-11-02T17:00:01.200Z",
    )
    time.sleep(0.5)

    print(" 2) Enviando sinal 'ESTUDAR' (t=1.6s..2.8s, gap=0.4s -> MESMO CONTEXTO)...")
    publicar_sinal_i4(
        producer, session_id,
        sign="ESTUDAR", confidence=0.89,
        start_at="2026-11-02T17:00:01.600Z", end_at="2026-11-02T17:00:02.800Z",
    )
    time.sleep(0.5)

    print(" 3) Enviando sinal 'ONTEM' (t=3.1s..4.2s, gap=0.3s -> MESMO CONTEXTO, conf baixa=0.58)...")
    publicar_sinal_i4(
        producer, session_id,
        sign="ONTEM", confidence=0.58,
        start_at="2026-11-02T17:00:03.100Z", end_at="2026-11-02T17:00:04.200Z",
        alternatives=[{"sign": "HOJE", "confidence": 0.34}, {"sign": "AMANHA", "confidence": 0.08}],
    )
    time.sleep(0.8)

    # =========================================================================
    # MUDANÇA DE CONTEXTO (Pausa de 5.5s entre 04.2s e 09.7s >= 3.0s)
    # =========================================================================
    print("\n--- [PAUSA DE 5.5s NOS TIMESTAMPS -> FECHA FRASE 1 E ABRE CONTEXTO 2] ---")

    print(" 4) Enviando sinal 'VOCE' (t=9.7s..10.8s, gap=5.5s -> NOVO CONTEXTO!)...")
    publicar_sinal_i4(
        producer, session_id,
        sign="VOCE", confidence=0.94,
        start_at="2026-11-02T17:00:09.700Z", end_at="2026-11-02T17:00:10.800Z",
    )
    time.sleep(0.5)

    print(" 5) Enviando sinal 'SUCO' (t=11.2s..12.3s, gap=0.4s -> MESMO CONTEXTO 2)...")
    publicar_sinal_i4(
        producer, session_id,
        sign="SUCO", confidence=0.88,
        start_at="2026-11-02T17:00:11.200Z", end_at="2026-11-02T17:00:12.300Z",
    )
    time.sleep(0.5)

    print(" 6) Enviando sinal 'QUERER' (t=12.6s..13.8s, gap=0.3s -> MESMO CONTEXTO 2)...")
    publicar_sinal_i4(
        producer, session_id,
        sign="QUERER", confidence=0.90,
        start_at="2026-11-02T17:00:12.600Z", end_at="2026-11-02T17:00:13.800Z",
    )

    print(" 7) Enviando marcador facial 'INTERROGATIVA_SN' para a Frase 2...")
    ev_marker = build_ods_envelope(
        message_type="event",
        schema=TOPIC_I4_FACIAL_MARKER,
        producer="I4",
        session_id=session_id,
        payload={
            "marker": "INTERROGATIVA_SN",
            "confidence": 0.86,
            "start_at": "2026-11-02T17:00:09.700Z",
            "end_at": "2026-11-02T17:00:13.800Z",
        },
    )
    producer.produce(
        TOPIC_I4_FACIAL_MARKER,
        key=session_id.encode("utf-8"),
        value=json.dumps(ev_marker, ensure_ascii=False).encode("utf-8"),
    )
    producer.flush(3.0)

    print("\n[OK] Todos os sinais foram enviados!")
    print("-> No terminal do `kafka_integration.py`, você verá:")
    print("   1. A Frase 1 (`frase-001`: ['ONTEM', 'ALUNO', 'ESTUDAR']) fechando assim que o sinal 'VOCE' chega após a pausa de 5.5s;")
    print("   2. A Frase 2 (`frase-002`: ['VOCE', 'SUCO', 'QUERER'] + INTERROGATIVA_SN) fechando sozinha após 3.5s de silêncio!")


if __name__ == "__main__":
    main()
