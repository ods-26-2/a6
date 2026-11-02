"""
kafka_integration.py
====================
Integração Kafka do Squad A6 (Camada 4 · Libras · Arquitetura 6c e Guia TRA-1).

Inclui o AGREGADOR DE FRASES E CONTEXTO EM TEMPO REAL (`SentenceContextAggregator`):
- Recebe sinais individuais de I4 (`ods.i4.sign`) e marcadores (`ods.i4.facial_marker`).
- Decide automaticamente se o novo sinal pertence ao MESMO CONTEXTO (mesma frase)
  ou se inicia um NOVO CONTEXTO (nova frase), com base em:
    1. Gap temporal entre `end_at` do sinal anterior e `start_at` do novo sinal (> pause_threshold_sec);
    2. Timeout de inatividade em tempo real no loop do consumidor (fecha a frase quando o usuário para de sinalizar);
    3. Quebra gramatical de Libras (ex: chegada de novo marcador de [TEMPO] quando a frase já possui [VERBO]);
    4. Troca de `signer_id`.
- Salva todas as mensagens e frases fechadas na pasta `a6/mensagens_recebidas_kafka/`.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from confluent_kafka import Consumer, KafkaError, Producer
    HAS_CONFLUENT_KAFKA = True
except ImportError:
    HAS_CONFLUENT_KAFKA = False
    Consumer = None
    KafkaError = None
    Producer = None

from sugestao_correcao_pipeline import LibrasCorrectionPipeline, normalize_gloss

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("a6.libras.kafka")

DEFAULT_BOOTSTRAP = "34.66.235.91:9093"
DEFAULT_GROUP_ID = "A6-libras-correcao"
PRODUCER_ID = "A6"
SCHEMA_VERSION = "1.0"

TOPIC_I4_SIGN = "ods.i4.sign"
TOPIC_I4_FACIAL_MARKER = "ods.i4.facial_marker"
TOPIC_I4_CAPTURE_CMD = "ods.i4.lexicon_capture.command"
TOPIC_I4_CAPTURE_RPL = "ods.i4.lexicon_capture.reply"
TOPIC_S6_SYNTHESIS_CMD = "ods.s6.synthesis.command"
TOPIC_S6_SYNTHESIS_RPL = "ods.s6.synthesis.reply"


def get_bootstrap_servers() -> str:
    return (
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        or os.environ.get("ODS_KAFKA_BOOTSTRAP")
        or DEFAULT_BOOTSTRAP
    )


def utc_iso(dt: Optional[datetime] = None) -> str:
    dt = (dt or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def parse_iso_timestamp(iso_str: Optional[str]) -> Optional[float]:
    """Converte string ISO 8601 ('2026-11-02T14:05:11.100Z') para epoch seconds."""
    if not iso_str or not isinstance(iso_str, str):
        return None
    try:
        cleaned = iso_str.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned).timestamp()
    except ValueError:
        return None


def validate_ods_envelope(msg: Any) -> Tuple[bool, Optional[str]]:
    if not isinstance(msg, dict):
        return False, "mensagem não é um objeto JSON"

    required_base = ("message_id", "message_type", "schema", "schema_version", "producer", "payload")
    for field in required_base:
        if field not in msg:
            return False, f"campo obrigatório ausente: {field}"

    if "occurred_at" not in msg and "published_at" not in msg:
        return False, "campo de timestamp ausente (esperado 'occurred_at' ou 'published_at')"

    if not str(msg["schema_version"]).startswith("1."):
        return False, f"versão não suportada: {msg['schema_version']}"

    if not isinstance(msg["payload"], dict):
        return False, "payload não é um objeto"

    return True, None


def build_ods_envelope(
    *,
    message_type: str,
    schema: str,
    payload: Dict[str, Any],
    producer: str = PRODUCER_ID,
    target: Optional[str] = None,
    session_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
) -> Dict[str, Any]:
    now = utc_iso()
    env: Dict[str, Any] = {
        "message_id": str(uuid.uuid4()),
        "message_type": message_type,
        "schema": schema,
        "schema_version": SCHEMA_VERSION,
        "producer": producer,
        "published_at": now,
        "occurred_at": now,
        "payload": payload,
    }
    if target:
        env["target"] = target
    if session_id:
        env["session_id"] = session_id
    if correlation_id:
        env["correlation_id"] = correlation_id
    return env


# =============================================================================
# AGREGADOR DE FRASES E CONTEXTO EM TEMPO REAL
# =============================================================================
class SentenceContextAggregator:
    """
    Agrega sinais isolados recebidos em tempo real de I4 em frases coerentes (contextos)
    e detecta quando uma frase terminou ou quando um novo contexto começou.
    """

    def __init__(
        self,
        pipeline: LibrasCorrectionPipeline,
        pause_threshold_sec: float = 3.0,
        realtime_idle_timeout_sec: float = 3.5,
    ):
        """
        :param pause_threshold_sec: Se `start_at` do novo sinal menos `end_at` do anterior
               for maior que esse valor (em segundos), fecha a frase anterior e abre novo contexto.
        :param realtime_idle_timeout_sec: Se não chegar nenhum sinal novo por esse tempo
               no relógio real, fecha a frase pendente automaticamente.
        """
        self.pipeline = pipeline
        self.pause_threshold_sec = pause_threshold_sec
        self.realtime_idle_timeout_sec = realtime_idle_timeout_sec

        # Estado do contexto aberto por session_id
        self.active_contexts: Dict[str, Dict[str, Any]] = {}
        self.closed_sentences: List[Dict[str, Any]] = []
        self._context_counter = 0

    def _new_context(self, session_id: str, signer_id: Optional[str]) -> Dict[str, Any]:
        self._context_counter += 1
        return {
            "context_id": f"frase-{self._context_counter:03d}",
            "session_id": session_id,
            "signer_id": signer_id,
            "glosses": [],
            "categories": [],
            "signs_detail": [],
            "facial_markers": [],
            "started_at_iso": None,
            "last_end_at_iso": None,
            "last_end_epoch": None,
            "last_wallclock_time": time.time(),
        }

    def check_if_new_context_needed(
        self,
        ctx: Dict[str, Any],
        new_gloss: str,
        new_category: str,
        start_epoch: Optional[float],
        signer_id: Optional[str],
    ) -> Tuple[bool, Optional[str]]:
        """
        Verifica se o novo sinal recebido pertence ao contexto/frase atual
        ou se deve fechar a frase anterior e iniciar uma nova.
        Retorna (deve_abrir_novo_contexto, motivo).
        """
        if not ctx["glosses"]:
            return False, None

        # Regra 1: Mudou o sinalizador (signer_id)
        if signer_id and ctx["signer_id"] and signer_id != ctx["signer_id"]:
            return True, f"Mudança de sinalizador ({ctx['signer_id']} -> {signer_id})"

        # Regra 2: Pausa nos timestamps do evento (`start_at` atual - `end_at` anterior)
        if start_epoch is not None and ctx["last_end_epoch"] is not None:
            gap_sec = start_epoch - ctx["last_end_epoch"]
            if gap_sec >= self.pause_threshold_sec:
                return True, f"Pausa longa entre sinais ({gap_sec:.2f}s >= {self.pause_threshold_sec:.1f}s)"

        # Regra 3: Pausa no relógio de parede (caso timestamps dos eventos sejam omitidos)
        wall_gap = time.time() - ctx["last_wallclock_time"]
        if wall_gap >= self.realtime_idle_timeout_sec and start_epoch is None:
            return True, f"Inatividade em tempo real ({wall_gap:.2f}s)"

        # Regra 4: Quebra Gramatical da Libras
        # Se a frase atual JÁ possui um marcador de TEMPO (ex: 'ONTEM') E já possui um VERBO,
        # e chega UM OUTRO marcador de TEMPO (ex: 'AMANHA'), sabemos que é uma nova oração!
        if new_category == "TEMPO" and ("TEMPO" in ctx["categories"] and "VERBO" in ctx["categories"]):
            return True, f"Novo marcador temporal '{new_gloss}' após oração completa já com TEMPO e VERBO ({ctx['glosses']})"

        return False, None

    def add_sign(
        self,
        session_id: str,
        payload: Dict[str, Any],
        eval_result: Dict[str, Any],
    ) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        """
        Adiciona um sinal reconhecido ao agregador.
        Se detectar mudança de contexto, fecha e retorna `(frase_fechada_anterior, contexto_atual)`.
        Caso contrário, retorna `(None, contexto_atual)`.
        """
        gloss = eval_result["recognized_sign"]
        category = self.pipeline.lexicon_signs.get(gloss, "OUTRO")
        signer_id = payload.get("signer_id")
        start_iso = payload.get("start_at")
        end_iso = payload.get("end_at")
        start_epoch = parse_iso_timestamp(start_iso)
        end_epoch = parse_iso_timestamp(end_iso)

        ctx = self.active_contexts.get(session_id)
        closed_prev: Optional[Dict[str, Any]] = None

        if ctx is not None:
            is_new, reason = self.check_if_new_context_needed(
                ctx, gloss, category, start_epoch, signer_id
            )
            if is_new:
                closed_prev = self.close_context(session_id, close_reason=reason)
                ctx = None

        if ctx is None:
            ctx = self._new_context(session_id, signer_id)
            self.active_contexts[session_id] = ctx

        # Se o sinal teve baixa confiança, registra a melhor sugestão alternativa ao lado
        best_alt = (
            eval_result["suggested_alternatives"][0]["gloss"]
            if (eval_result["needs_correction_suggestion"] and eval_result["suggested_alternatives"])
            else None
        )

        ctx["glosses"].append(gloss)
        ctx["categories"].append(category)
        ctx["signs_detail"].append({
            "gloss": gloss,
            "category": category,
            "confidence": eval_result["confidence"],
            "needs_suggestion": eval_result["needs_correction_suggestion"],
            "top_suggestion": best_alt,
            "alternatives": eval_result["suggested_alternatives"],
            "start_at": start_iso,
            "end_at": end_iso,
        })
        if ctx["started_at_iso"] is None:
            ctx["started_at_iso"] = start_iso
        if end_iso is not None:
            ctx["last_end_at_iso"] = end_iso
        if end_epoch is not None:
            ctx["last_end_epoch"] = end_epoch
        ctx["last_wallclock_time"] = time.time()

        return closed_prev, ctx

    def add_facial_marker(self, session_id: str, marker_payload: Dict[str, Any]):
        ctx = self.active_contexts.get(session_id)
        if ctx is None:
            ctx = self._new_context(session_id, signer_id=None)
            self.active_contexts[session_id] = ctx
        ctx["facial_markers"].append(marker_payload.get("marker", "NEUTRO"))
        ctx["last_wallclock_time"] = time.time()

    def close_context(self, session_id: str, close_reason: str) -> Optional[Dict[str, Any]]:
        """Fecha a frase acumulada na sessão, aplica a sintaxe e gera a sugestão da frase completa."""
        ctx = self.active_contexts.pop(session_id, None)
        if not ctx or not ctx["glosses"]:
            return None

        raw_glosses = list(ctx["glosses"])
        reordered_glosses, was_reordered = self.pipeline.reorganize_syntax(raw_glosses)

        # Monta também a versão da frase caso o usuário aceite as sugestões para sinais de baixa confiança
        glosses_with_suggestions = [
            item["top_suggestion"] if (item["needs_suggestion"] and item["top_suggestion"]) else item["gloss"]
            for item in ctx["signs_detail"]
        ]
        reordered_suggested, _ = self.pipeline.reorganize_syntax(glosses_with_suggestions)

        closed_summary = {
            "context_id": ctx["context_id"],
            "session_id": session_id,
            "signer_id": ctx["signer_id"],
            "close_reason": close_reason,
            "started_at": ctx["started_at_iso"],
            "ended_at": ctx["last_end_at_iso"],
            "raw_gloss_sequence": raw_glosses,
            "grammatical_gloss_sequence": reordered_glosses,
            "suggested_corrected_sentence": reordered_suggested,
            "was_syntactically_reordered": was_reordered,
            "had_low_confidence_replacements": raw_glosses != glosses_with_suggestions,
            "facial_markers": ctx["facial_markers"] or ["NEUTRO"],
            "signs_detail": ctx["signs_detail"],
        }
        self.closed_sentences.append(closed_summary)
        return closed_summary

    def flush_idle_contexts(self) -> List[Dict[str, Any]]:
        """Verifica se alguma sessão ficou em silêncio além do timeout e fecha a frase."""
        now = time.time()
        expired_sessions = [
            sid for sid, ctx in self.active_contexts.items()
            if ctx["glosses"] and (now - ctx["last_wallclock_time"]) >= self.realtime_idle_timeout_sec
        ]
        closed = []
        for sid in expired_sessions:
            res = self.close_context(
                sid,
                close_reason=f"Silêncio/inatividade de {self.realtime_idle_timeout_sec:.1f}s (fim da frase detectado)",
            )
            if res:
                closed.append(res)
        return closed


# =============================================================================
# WORKER KAFKA PRINCIPAL (A6)
# =============================================================================
class LibrasCorrectionKafkaWorker:
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: str = DEFAULT_GROUP_ID,
        pipeline: Optional[LibrasCorrectionPipeline] = None,
        save_dir: Optional[str | Path] = None,
        pause_threshold_sec: float = 3.0,
        realtime_idle_timeout_sec: float = 3.5,
    ):
        self.bootstrap = bootstrap_servers or get_bootstrap_servers()
        self.group_id = group_id
        self.pipeline = pipeline or LibrasCorrectionPipeline()
        self.aggregator = SentenceContextAggregator(
            self.pipeline,
            pause_threshold_sec=pause_threshold_sec,
            realtime_idle_timeout_sec=realtime_idle_timeout_sec,
        )

        self.save_dir = Path(save_dir) if save_dir else (Path(__file__).parent / "mensagens_recebidas_kafka")
        self.save_dir.mkdir(parents=True, exist_ok=True)
        self._msg_counter = 0

        self._running = False
        self._consumer: Optional[Consumer] = None
        self._producer: Optional[Producer] = None
        self._seen_message_ids: set[str] = set()

    def _salvar_mensagem_recebida(
        self,
        evento_bruto: Dict[str, Any],
        resultado_processado: Optional[Dict[str, Any]] = None,
    ) -> Path:
        self._msg_counter += 1
        schema_slug = str(evento_bruto.get("schema", "unknown")).replace(".", "_")
        ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        mid_short = str(evento_bruto.get("message_id", "noid"))[:8]

        registro = {
            "received_at_local": datetime.now().isoformat(),
            "raw_kafka_envelope": evento_bruto,
            "a6_processed_result": resultado_processado,
        }

        arquivo_individual = self.save_dir / f"{self._msg_counter:03d}_{ts_str}_{schema_slug}_{mid_short}.json"
        arquivo_individual.write_text(
            json.dumps(registro, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        arquivo_historico = self.save_dir / "historico_kafka.jsonl"
        with arquivo_historico.open("a", encoding="utf-8") as f:
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")

        log.info(" [ARQUIVO SALVO] %s", arquivo_individual.name)
        return arquivo_individual

    def _salvar_frase_fechada(self, closed_sentence: Dict[str, Any]) -> Path:
        """Salva um arquivo dedicado sempre que uma frase completa (contexto) é fechada."""
        cid = closed_sentence["context_id"]
        arquivo_frase = self.save_dir / f"FRASE_FECHADA_{cid}.json"
        arquivo_frase.write_text(
            json.dumps(closed_sentence, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        log.info("==========================================================================")
        log.info(" >>> [FIM DE FRASE / CONTEXTO FECHADO: %s] <<<", cid)
        log.info(" Motivo do fechamento        : %s", closed_sentence["close_reason"])
        log.info(" Sinais recebidos na ordem   : %s", closed_sentence["raw_gloss_sequence"])
        log.info(" Ordem Gramatical [T+S+O+V]  : %s", closed_sentence["grammatical_gloss_sequence"])
        if closed_sentence["had_low_confidence_replacements"]:
            log.info(" Frase c/ Sugestão Aplicada  : %s", closed_sentence["suggested_corrected_sentence"])
        log.info(" Marcadores Faciais da frase : %s", closed_sentence["facial_markers"])
        log.info(" Arquivo da frase salvo em   : %s", arquivo_frase)
        log.info("==========================================================================")
        return arquivo_frase

    @staticmethod
    def _delivery_callback(err, msg):
        if err is not None:
            log.error("Falha ao entregar em %s: %s", msg.topic(), err)
        else:
            log.info("Mensagem publicada em [%s] (offset: %s)", msg.topic(), msg.offset())

    def initialize(self):
        if not HAS_CONFLUENT_KAFKA:
            raise ImportError("Instale confluent-kafka: pip install confluent-kafka")

        log.info("Conectando ao broker Kafka em %s (Group ID: %s)", self.bootstrap, self.group_id)
        self._consumer = Consumer({
            "bootstrap.servers": self.bootstrap,
            "group.id": self.group_id,
            "auto.offset.reset": os.environ.get("KAFKA_AUTO_OFFSET_RESET", "latest"),
            "enable.auto.commit": True,
        })
        self._producer = Producer({
            "bootstrap.servers": self.bootstrap,
            "linger.ms": 20,
        })

        topics = [TOPIC_I4_SIGN, TOPIC_I4_FACIAL_MARKER, TOPIC_S6_SYNTHESIS_RPL, TOPIC_I4_CAPTURE_RPL]
        self._consumer.subscribe(topics)
        log.info("A6 inscrito nos tópicos: %s", topics)

    def stop(self, *args, **kwargs):
        log.info("Encerrando serviço Kafka A6...")
        self._running = False

    def request_s6_synthesis(
        self,
        words_or_sentence: List[str] | str,
        speed_factor: float = 1.0,
        explicit_marker: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        prep = self.pipeline.prepare_synthesis_command(
            input_words_or_sentence=words_or_sentence,
            speed_factor=speed_factor,
            explicit_marker=explicit_marker,
            auto_apply_top_suggestion=True,
        )

        log.info("========== [A6 -> S6: PRÉ-VALIDAÇÃO & SUGESTÃO DE CORREÇÃO] ==========")
        log.info(" Entrada original: %s", prep["input"])
        if prep["pre_validation"]["word_corrections"]:
            for wc in prep["pre_validation"]["word_corrections"]:
                sug_str = ", ".join(f"{s['gloss']} ({s['score']})" for s in wc["suggestions"])
                log.info(
                    " [SUGESTÃO APLICADA ANTES DO ENVIO] Palavra '%s' não está no LEXICON -> Substituída por '%s' (Alternativas: [%s])",
                    wc["original_word"], wc["applied_gloss"], sug_str
                )
        else:
            log.info(" Todas as palavras já pertencem ao LEXICON oficial de 100 sinais.")

        log.info(
            " Gramática Libras (%s): %s -> %s | Marcador Facial: %s",
            prep["grammar"]["rule"],
            prep["grammar"]["before_syntax_order"],
            prep["grammar"]["after_syntax_order"],
            prep["grammar"]["facial_marker_applied"],
        )

        cmd_envelope = build_ods_envelope(
            message_type="command",
            schema=TOPIC_S6_SYNTHESIS_CMD,
            target="S6",
            session_id=session_id,
            payload=prep["s6_command_payload"],
        )

        if self._producer and prep["s6_command_payload"]["glosses"]:
            key = cmd_envelope["message_id"].encode("utf-8")
            val = json.dumps(cmd_envelope, ensure_ascii=False).encode("utf-8")
            self._producer.produce(
                topic=TOPIC_S6_SYNTHESIS_CMD,
                key=key,
                value=val,
                on_delivery=self._delivery_callback,
            )
            self._producer.flush(5.0)
            log.info(" Comando `ods.s6.synthesis.command` enviado ao S6 (message_id=%s)", cmd_envelope["message_id"])
        log.info("======================================================================")
        return cmd_envelope

    def process_message(self, raw_bytes: bytes) -> Optional[Dict[str, Any]]:
        try:
            evento = json.loads(raw_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            log.warning("mensagem descartada: JSON inválido")
            return None

        valido, motivo = validate_ods_envelope(evento)
        if not valido:
            log.warning("mensagem descartada: %s", motivo)
            return None

        mid = evento["message_id"]
        if mid in self._seen_message_ids:
            return None
        self._seen_message_ids.add(mid)

        schema = evento["schema"]
        payload = evento["payload"]
        session_id = evento.get("session_id", "default_session")

        # ---------------------------------------------------------------------
        # CASO A: Evento `ods.i4.sign` vindo de I4 em tempo real
        # ---------------------------------------------------------------------
        if schema == TOPIC_I4_SIGN:
            # Avalia o sinal isolado (confiança + alternativas)
            current_ctx = self.aggregator.active_contexts.get(session_id)
            history_before = list(current_ctx["glosses"]) if current_ctx else []
            result = self.pipeline.evaluate_incoming_i4_sign(payload, session_history=history_before)

            # Passa para o Agregador de Contexto decidir se continua na mesma frase ou abre nova!
            closed_prev, active_ctx = self.aggregator.add_sign(session_id, payload, result)

            if closed_prev is not None:
                self._salvar_frase_fechada(closed_prev)

            # Atualiza o contexto da frase no resultado para salvar junto no JSON
            reordered_now, _ = self.pipeline.reorganize_syntax(active_ctx["glosses"])
            result["context_tracking"] = {
                "context_id": active_ctx["context_id"],
                "switched_to_new_context": closed_prev is not None,
                "previous_closed_context": closed_prev,
                "active_glosses_in_context": list(active_ctx["glosses"]),
                "active_grammatical_order": reordered_now,
            }

            log.info("---------- [I4 -> A6: SINAL EM TEMPO REAL (%s)] ----------", active_ctx["context_id"])
            log.info(" Glosa recebida: '%s' (conf=%.2f) | Contexto atual: %s",
                     result["recognized_sign"], result["confidence"], active_ctx["glosses"])
            if result["needs_correction_suggestion"]:
                alts = [f"{a['gloss']} ({a['confidence']:.2f})" for a in result["suggested_alternatives"]]
                log.info(" [SUGESTÃO DE PALAVRAS] Confiança baixa! Alternativas p/ '%s': %s",
                         result["recognized_sign"], alts)
            self._salvar_mensagem_recebida(evento, result)
            return result

        # ---------------------------------------------------------------------
        # Marcador Facial Gramatical (`ods.i4.facial_marker`)
        # ---------------------------------------------------------------------
        elif schema == TOPIC_I4_FACIAL_MARKER:
            marker = payload.get("marker", "NEUTRO")
            conf = float(payload.get("confidence", 0.0))
            self.aggregator.add_facial_marker(session_id, payload)
            log.info("[I4 -> A6: MARCADOR FACIAL] Marcador='%s' (conf=%.2f) anexado à frase ativa", marker, conf)
            self._salvar_mensagem_recebida(evento, payload)
            return payload

        # ---------------------------------------------------------------------
        # Reply de Síntese (`ods.s6.synthesis.reply`)
        # ---------------------------------------------------------------------
        elif schema == TOPIC_S6_SYNTHESIS_RPL:
            eval_rpl = self.pipeline.evaluate_s6_synthesis_reply(payload)
            log.info("========== [S6 -> A6: REPLY DE SÍNTESE (`ods.s6.synthesis.reply`)] ==========")
            log.info(" Status: %s | Trajetória: %s | Duração: %s ms",
                     eval_rpl["status"], eval_rpl["trajectory_ref"], eval_rpl["duration_ms"])
            if eval_rpl["has_unknown_signs"]:
                for unk, sugs in eval_rpl["suggestions_for_unknown_signs"].items():
                    sug_names = [f"{s['gloss']} ({s['score']})" for s in sugs]
                    log.info(" [SUGESTÃO P/ `unknown_signs`] '%s' -> %s", unk, sug_names)
            self._salvar_mensagem_recebida(evento, eval_rpl)
            log.info("=============================================================================")
            return eval_rpl

        self._salvar_mensagem_recebida(evento, None)
        return None

    def run(self):
        self.initialize()
        self._running = True
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        log.info("Loop de consumo A6 iniciado (poll=1.0s | Fecha frase após %.1fs de pausa)...",
                 self.aggregator.realtime_idle_timeout_sec)
        try:
            while self._running:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    # Verifica se alguma frase aberta já terminou por silêncio do sinalizador
                    for closed in self.aggregator.flush_idle_contexts():
                        self._salvar_frase_fechada(closed)
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("Erro no Kafka: %s", msg.error())
                    continue
                self.process_message(msg.value())
                # Checa timeouts também entre mensagens
                for closed in self.aggregator.flush_idle_contexts():
                    self._salvar_frase_fechada(closed)
        finally:
            # Ao encerrar, fecha qualquer frase que estivesse em aberto
            for sid in list(self.aggregator.active_contexts.keys()):
                closed = self.aggregator.close_context(sid, close_reason="Encerramento do serviço")
                if closed:
                    self._salvar_frase_fechada(closed)
            if self._producer:
                self._producer.flush(5.0)
            if self._consumer:
                self._consumer.close()
            log.info("Serviço Kafka A6 encerrado com segurança.")


if __name__ == "__main__":
    worker = LibrasCorrectionKafkaWorker()
    worker.run()
