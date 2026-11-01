"""
simular_i4_para_a6.py
======================
Simulador que publica um sinal real (exemplo do I4) no Kafka
para testar o Módulo 5 (Sugestão de Correção / A6) ponta a ponta.
"""

import json
import os
import sys
import time
import uuid
from pathlib import Path
from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "34.66.235.91:9093")

# Carrega um exemplo real gerado pelo I4
caminho_exemplo = Path("../i4/examples/exemplo_aluno.json")
if not caminho_exemplo.exists():
    caminho_exemplo = Path("i4/examples/exemplo_aluno.json")

print(f"Lendo amostra real do I4: {caminho_exemplo}...")
dados = json.loads(caminho_exemplo.read_text(encoding="utf-8-sig"))

sign_info = dados.get("sign", {"sinal": "ALUNO", "confianca": 0.92})
keypoints = dados.get("keypoints", [])

producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 10})
session_id = f"teste-sessao-{uuid.uuid4().hex[:6]}"

print(f"\n[1/2] Publicando {len(keypoints[:15])} frames no tópico 'ods.i4.keypoints' (session_id={session_id})...")
for i, f in enumerate(keypoints[:15]):
    # Mapeia para o schema de envelope
    body_pts = f.get("pose", [])
    body_spec = [[p["x"], p["y"], p["z"], p.get("visibility", 1.0)] for p in body_pts]
    lh_spec = [[p["x"], p["y"], p["z"]] for p in f.get("left_hand_global", [])] or None
    rh_spec = [[p["x"], p["y"], p["z"]] for p in f.get("right_hand_global", [])] or None

    kp_payload = {
        "frame_index": i,
        "image_width": 1280,
        "image_height": 720,
        "coordinates": "NORMALIZED",
        "body": body_spec,
        "left_hand": lh_spec,
        "right_hand": rh_spec,
        "face": None,
    }
    envelope_kp = {
        "message_id": str(uuid.uuid4()),
        "message_type": "event",
        "schema": "ods.i4.keypoints",
        "schema_version": "1.0",
        "producer": "I4",
        "published_at": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "session_id": session_id,
        "payload": kp_payload,
    }
    producer.produce(
        "ods.i4.keypoints",
        key=session_id.encode("utf-8"),
        value=json.dumps(envelope_kp).encode("utf-8")
    )
    time.sleep(0.04)  # ~25 FPS

producer.flush(3.0)

print(f"[2/2] Publicando fechamento de sinal no tópico 'ods.i4.sign'...")
envelope_sign = {
    "message_id": str(uuid.uuid4()),
    "message_type": "event",
    "schema": "ods.i4.sign",
    "schema_version": "1.0",
    "producer": "I4",
    "published_at": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
    "session_id": session_id,
    "payload": {
        "sign": sign_info.get("sinal", "ALUNO"),
        "confidence": sign_info.get("confianca", 0.90),
        "start_at": "2026-11-01T17:00:00.000Z",
        "end_at": "2026-11-01T17:00:01.000Z",
    }
}
producer.produce(
    "ods.i4.sign",
    key=session_id.encode("utf-8"),
    value=json.dumps(envelope_sign).encode("utf-8")
)
producer.flush(5.0)

print("\n SINAL PUBLICADO NO KAFKA COM SUCESSO!")
print("-> Veja agora a janela do kafka_integration.py processando as correções.")
