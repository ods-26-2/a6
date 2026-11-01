"""
kafka_integration.py
====================
Integração completa do Módulo 5 ("Sugestão de Correção") com o barramento Kafka (Squad A6).

Requisitos atendidos:
1. Conexão via confluent-kafka com bootstrap server dinâmico via KAFKA_BOOTSTRAP_SERVERS
   (fallback: 34.66.235.91:9093).
2. Group ID configurado para 'sugestao-correcao-libras'.
3. Consumo sem polling apertado (poll(1.0)), protegendo limites e créditos GCP.
4. Validação estrita do envelope ODS (message_type, schema, version, producer, published_at, payload).
5. Suporte aos tópicos de entrada produzidos pelo time i4:
   - 'ods.i4.keypoints' (um evento por quadro com 'body', 'left_hand', 'right_hand', 'face')
   - 'ods.i4.sign' (evento de sinal reconhecido com rótulo e confiança)
6. Aplicação do pipeline de correção (LibrasCorrectionPipeline) e publicação das correções
   no tópico 'ods.gesto.reconhecido' (ou 'ods.fala.transcricao_produzida' configurável).
7. Geração de JSON estruturado pronto para consumo pelo libras_avatar.py / A6.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from confluent_kafka import Consumer, KafkaError, Producer
    HAS_CONFLUENT_KAFKA = True
except ImportError:
    HAS_CONFLUENT_KAFKA = False
    Consumer = None
    KafkaError = None
    Producer = None

from sugestao_correcao_pipeline import LibrasCorrectionPipeline

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("a6.modulo5.sugestao_correcao")

# Variáveis de ambiente e padrões de arquitetura (TRA-1 / B2)
DEFAULT_BOOTSTRAP = "34.66.235.91:9093"
DEFAULT_GROUP_ID = "sugestao-correcao-libras"
PRODUCER_NAME = "modulo5-sugestao-correcao"
SCHEMA_VERSION = "1.0"

# Tópicos
TOPIC_INPUT_SIGN = os.environ.get("KAFKA_TOPIC_SIGN", "ods.i4.sign")
TOPIC_INPUT_KEYPOINTS = os.environ.get("KAFKA_TOPIC_KEYPOINTS", "ods.i4.keypoints")
TOPIC_OUTPUT_GESTURE = os.environ.get("KAFKA_TOPIC_OUTPUT", "ods.gesto.reconhecido")


def get_bootstrap_servers() -> str:
    """Obtém o bootstrap broker via variável de ambiente com fallback para a nuvem."""
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or os.environ.get("ODS_KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP)


def utc_iso(dt: Optional[datetime] = None) -> str:
    """Formata timestamp UTC ISO 8601 com milissegundos."""
    dt = (dt or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def validate_ods_envelope(msg: Any) -> Tuple[bool, Optional[str]]:
    """Valida a integridade do envelope ODS TRA-1."""
    if not isinstance(msg, dict):
        return False, "Mensagem não é um dicionário JSON válido"

    fixed_fields = ("message_type", "message_id", "schema", "schema_version", "producer", "payload")
    for f in fixed_fields:
        if f not in msg:
            return False, f"Campo obrigatório ausente: '{f}'"

    version = str(msg.get("schema_version", ""))
    if not version.startswith("1."):
        return False, f"Versão de schema não suportada: '{version}'"

    if not isinstance(msg["payload"], dict):
        return False, "Campo 'payload' deve ser um objeto JSON"

    return True, None


def build_ods_envelope(
    schema: str,
    payload: Dict[str, Any],
    session_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    producer: str = PRODUCER_NAME,
) -> Dict[str, Any]:
    """Constrói o envelope padronizado ODS TRA-1 para envio de eventos no barramento."""
    envelope = {
        "message_id": str(uuid.uuid4()),
        "message_type": "event",
        "schema": schema,
        "schema_version": SCHEMA_VERSION,
        "producer": producer,
        "published_at": utc_iso(),
        "payload": payload,
    }
    if session_id:
        envelope["session_id"] = session_id
    if correlation_id:
        envelope["correlation_id"] = correlation_id
    return envelope


class LibrasCorrectionKafkaWorker:
    """
    Trabalhador de integração Kafka para o Módulo 5 (A6):
    - Assina os tópicos de sinais e keypoints do I4.
    - Agrupa os frames da sessão por sinal.
    - Executa o pipeline de correção (LibrasCorrectionPipeline).
    - Publica as sugestões no tópico de saída.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: str = DEFAULT_GROUP_ID,
        input_sign_topic: str = TOPIC_INPUT_SIGN,
        input_kp_topic: str = TOPIC_INPUT_KEYPOINTS,
        output_topic: str = TOPIC_OUTPUT_GESTURE,
        pipeline: Optional[LibrasCorrectionPipeline] = None,
    ):
        self.bootstrap = bootstrap_servers or get_bootstrap_servers()
        self.group_id = group_id
        self.input_sign_topic = input_sign_topic
        self.input_kp_topic = input_kp_topic
        self.output_topic = output_topic
        self.pipeline = pipeline or LibrasCorrectionPipeline()

        self._running = False
        self._consumer = None
        self._producer = None

        self._session_frames: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._session_gloss_history: Dict[str, List[str]] = defaultdict(list)

    def _delivery_callback(self, err, msg):
        if err is not None:
            log.error("Falha ao entregar mensagem no tópico %s: %s", msg.topic(), err)
        else:
            log.info("Mensagem entregue com sucesso em [%s] (offset: %s)", msg.topic(), msg.offset())

    def initialize(self):
        if not HAS_CONFLUENT_KAFKA:
            raise ImportError(
                "O pacote 'confluent-kafka' não está instalado neste ambiente Python!\n"
                "Para instalar, execute: pip install confluent-kafka"
            )

        log.info("Conectando ao broker Kafka em %s", self.bootstrap)
        consumer_conf = {
            "bootstrap.servers": self.bootstrap,
            "group.id": self.group_id,
            "auto.offset.reset": os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            "enable.auto.commit": True,
            "session.timeout.ms": 30000,
        }
        producer_conf = {
            "bootstrap.servers": self.bootstrap,
            "linger.ms": 20,
            "acks": "all",
        }

        self._consumer = Consumer(consumer_conf)
        self._producer = Producer(producer_conf)

        topics_to_subscribe = [self.input_sign_topic, self.input_kp_topic]
        self._consumer.subscribe(topics_to_subscribe)
        log.info("Inscrito nos tópicos: %s | Group ID: %s", topics_to_subscribe, self.group_id)

    def stop(self, *args, **kwargs):
        log.info("Encerrando consumidor Kafka...")
        self._running = False

    def publish_correction(
        self,
        correction_result: Dict[str, Any],
        session_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ):
        if not self._producer:
            log.warning("Producer não inicializado.")
            return

        envelope = build_ods_envelope(
            schema=self.output_topic,
            payload=correction_result,
            session_id=session_id,
            correlation_id=correlation_id,
        )

        key = (session_id or correlation_id or str(uuid.uuid4())).encode("utf-8")
        val = json.dumps(envelope, ensure_ascii=False).encode("utf-8")

        self._producer.produce(
            topic=self.output_topic,
            key=key,
            value=val,
            on_delivery=self._delivery_callback,
        )
        self._producer.poll(0)

    def process_message(self, raw_bytes: bytes):
        try:
            msg_dict = json.loads(raw_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            log.warning("Descartando mensagem malformatada (JSON Inválido): %s", e)
            return

        is_valid, reason = validate_ods_envelope(msg_dict)
        if not is_valid:
            log.warning("Envelope inválido descartado: %s", reason)
            return

        schema = msg_dict.get("schema")
        payload = msg_dict.get("payload", {})
        session_id = msg_dict.get("session_id", "default_session")
        message_id = msg_dict.get("message_id")

        if schema == self.input_kp_topic or "keypoints" in schema:
            self._session_frames[session_id].append(payload)
            if len(self._session_frames[session_id]) > 300:
                self._session_frames[session_id].pop(0)

        elif schema == self.input_sign_topic or "sign" in schema:
            sign_label = payload.get("sign") or payload.get("sinal", "DESCONHECIDO")
            confidence = float(payload.get("confidence") or payload.get("confianca", 1.0))

            log.info("Sinal recebido: '%s' (confiança: %.2f) na sessão %s", sign_label, confidence, session_id)

            keypoint_seq = self._session_frames.pop(session_id, [])
            if not keypoint_seq and "keypoints" in payload:
                keypoint_seq = payload["keypoints"]

            history = self._session_gloss_history[session_id]

            correction = self.pipeline.process_sign_window(
                sign_label=sign_label,
                confidence=confidence,
                keypoints=keypoint_seq,
                context_glosses=history,
            )

            self._session_gloss_history[session_id].append(sign_label.upper())

            self.publish_correction(
                correction_result=correction,
                session_id=session_id,
                correlation_id=message_id,
            )

            # Detalhes das sugestões e correções geradas pelo Módulo 5
            log.info("========== [MÓDULO 5: SUGESTÕES & CORREÇÕES GERADAS] ==========")
            log.info(" Sinal: %s (Confiança: %.2f)", sign_label, confidence)
            log.info(" Fonologia Estimada: %s", correction["phonology"])
            if correction["biomechanical_corrections"]:
                log.info(" Correções Biomecânicas: %s", correction["biomechanical_corrections"])
            else:
                log.info(" Biomecânica: Articulações dentro dos limites anatômicos normais.")
            
            sintaxe = correction["syntactic_correction"]
            if sintaxe["was_reordered"]:
                log.info(" Sugestão Sintática (Reordenação): %s ➔ %s (Regra: %s)", sintaxe["original"], sintaxe["reordered"], sintaxe["rule"])
            else:
                log.info(" Sintaxe: Glosas em conformidade gramatical (%s).", sintaxe["reordered"])

            log.info(" Avatar 3D: %d frames gerados e prontos para o avatar.", len(correction["avatar_frames"]))
            log.info(" Publicado no barramento em: '%s'", self.output_topic)
            log.info("==================================================================")

    def run(self):
        self.initialize()
        self._running = True

        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        log.info("Iniciando loop de consumo do Módulo 5 (A6)...")
        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("Erro Kafka recebido: %s", msg.error())
                    continue

                self.process_message(msg.value())

        except Exception as exc:
            log.exception("Exceção no loop do Kafka: %s", exc)
        finally:
            log.info("Finalizando e esvaziando filas...")
            if self._producer:
                self._producer.flush(5.0)
            if self._consumer:
                self._consumer.close()
            log.info("Módulo 5 desconectado com sucesso.")


if __name__ == "__main__":
    worker = LibrasCorrectionKafkaWorker()
    worker.run()
