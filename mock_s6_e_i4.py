"""
mock_s6_e_i4.py
================
Mock Oficial dos Componentes Vizinhos (S6 e I4) para o Squad A6 (Camada 4 · Arquitetura 6c).

Por que este Mock existe?
1. O S6 real atualmente só possui 20 sinais gravados em `.parquet`, enquanto o `LEXICON`
   de A6 possui 100 palavras. Além disso, o S6 pode não estar online na nuvem durante
   todos os seus testes.
2. O canal `ods.i4.lexicon_capture.command` -> `ods.i4.lexicon_capture.reply` ainda não
   foi implementado pela equipe I4 (conforme documentado no CONTRATO_I4.md deles).

O que este serviço faz no barramento Kafka (`34.66.235.91:9093`):
- Escuta `ods.s6.synthesis.command` (enviado por A6) e responde imediatamente em
  `ods.s6.synthesis.reply`:
    * Reconhece todas as 100 palavras do `lexicon_a6.json`!
    * Qualquer palavra fora das 100 é devolvida em `unknown_signs` (permitindo testar o fallback de A6).
    * Calcula `fps`, `frame_count`, `duration_ms` escalados pelo `speed_factor`.
    * Gera `segments` (sinais + transições) e `facial_markers` com `start_ms` e `end_ms`.
    * Gera e salva um arquivo JSON de trajetória compatível com o Avatar 3D em `a6/trajectories_mock/`
      e devolve `trajectory_ref: "s6://trajectories/<arquivo>.json"`.
- Escuta `ods.i4.lexicon_capture.command` (enviado por A6) e responde imediatamente em
  `ods.i4.lexicon_capture.reply` com `status: "OK"` e `sequence_ref: "registry://lexicon/<SINAL>/v1"`.

Uso:
    python mock_s6_e_i4.py
"""

from __future__ import annotations

import json
import logging
import math
import os
import signal
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from confluent_kafka import Consumer, KafkaError, Producer
    HAS_CONFLUENT_KAFKA = True
except ImportError:
    HAS_CONFLUENT_KAFKA = False
    Consumer = None
    KafkaError = None
    Producer = None
from kafka_integration import (
    TOPIC_I4_CAPTURE_CMD,
    TOPIC_I4_CAPTURE_RPL,
    TOPIC_S6_SYNTHESIS_CMD,
    TOPIC_S6_SYNTHESIS_RPL,
    build_ods_envelope,
    get_bootstrap_servers,
    validate_ods_envelope,
)
from sugestao_correcao_pipeline import LibrasCorrectionPipeline, normalize_gloss

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("mock.vizinhos.s6_i4")


class MockS6AndI4Service:
    """
    Simula os serviços de comando/resposta de S6 (`ods.s6.synthesis.*`)
    e de captura de léxico de I4 (`ods.i4.lexicon_capture.*`) no Kafka.
    """

    BASE_FPS = 30.0
    FRAMES_PER_SIGN = 30        # ~1.0s por sinal em speed_factor=1.0
    FRAMES_PER_TRANSITION = 6   # ~0.2s de transição entre sinais

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: str = "mock-s6-i4-service",
        trajectory_dir: Optional[str | Path] = None,
    ):
        self.bootstrap = bootstrap_servers or get_bootstrap_servers()
        self.group_id = group_id
        self.pipeline = LibrasCorrectionPipeline()

        self.trajectory_dir = (
            Path(trajectory_dir)
            if trajectory_dir
            else (Path(__file__).parent / "avatar" / "entry")
        )
        self.trajectory_dir.mkdir(parents=True, exist_ok=True)

        # Carrega um template real de frames do avatar (se existir em saida_correcoes ou dataset_maos)
        self._sample_frames = self._load_sample_avatar_frames()

        self._running = False
        self._consumer: Optional[Consumer] = None
        self._producer: Optional[Producer] = None
        self._seen_commands: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _ensure_s6_frame_format(raw_frame: Dict[str, Any]) -> Dict[str, Any]:
        """Garante que o frame tenha `body`, `left_hand`, `right_hand` (formato S6/trajectory_format.py)."""
        if any(k in raw_frame for k in ("body", "left_hand", "right_hand")):
            return raw_frame

        aspect = 1280.0 / 720.0
        left_hand = None
        right_hand = None
        for m in raw_frame.get("maos", []):
            kps = sorted(m.get("keypoints", []), key=lambda k: k.get("id", 0))
            if len(kps) >= 21:
                pts = [[ float(k["x"]) * aspect, float(k["y"]), float(k["z"]) * aspect ] for k in kps[:21]]
                # MediaPipe Hands usa rótulo de imagem espelhada ("Right" na imagem = mão esquerda anatômica)
                if m.get("lado") == "Right":
                    left_hand = pts
                else:
                    right_hand = pts

        return {
            "body": [],
            "left_hand": left_hand,
            "right_hand": right_hand,
            "maos": raw_frame.get("maos", []),
            "corpo": raw_frame.get("corpo", []),
        }

    def _load_sample_avatar_frames(self) -> List[Dict[str, Any]]:
        """Carrega a trajetória padrão/oficial do S6 (template default de entrada do avatar).
        
        Prioriza o arquivo canônico do template S6: avatar/entry/6d9c234fcd35.json
        com estrutura:
          body: 33 landmarks [x, y, z, visibility]
          left_hand: 21 landmarks [x, y, z] ou null
          right_hand: 21 landmarks [x, y, z] ou null
          face: 468 landmarks [x, y, z] ou null
        """
        # 1. Template default oficial de S6
        template_canonical = Path(__file__).parent / "avatar" / "entry" / "6d9c234fcd35.json"
        if not template_canonical.exists():
            template_canonical = Path(__file__).parent / "avatar" / "entry" / "_processados" / "6d9c234fcd35.json"
        
        if template_canonical.exists():
            try:
                doc = json.loads(template_canonical.read_text(encoding="utf-8-sig"))
                frames = doc.get("frames", [])
                if isinstance(frames, list) and frames:
                    log.info("Carregado template oficial S6 de %s com %d frames", template_canonical.name, len(frames))
                    return frames
            except Exception as exc:
                log.warning("Falha ao ler template canônico S6 %s: %s", template_canonical, exc)

        # 2. Outros JSONs válidos de avatar/entry ou _processados
        for search_dir in [Path(__file__).parent / "avatar" / "entry", Path(__file__).parent / "avatar" / "entry" / "_processados"]:
            if search_dir.exists():
                for p in search_dir.glob("*.json"):
                    try:
                        doc = json.loads(p.read_text(encoding="utf-8-sig"))
                        frames = doc.get("frames", [])
                        if isinstance(frames, list) and frames and any("body" in f for f in frames[:5]):
                            return frames
                    except Exception:
                        pass

        # 3. Fallbacks legados
        sample_file = Path(__file__).parent / "saida_correcoes" / "exemplo_aluno_avatar.json"
        if sample_file.exists():
            try:
                data = json.loads(sample_file.read_text(encoding="utf-8-sig"))
                if isinstance(data, list) and data:
                    return [self._ensure_s6_frame_format(f) for f in data[:30]]
            except Exception:
                pass

        kp_dir = Path(__file__).parent / "dataset_maos" / "keypoints"
        frames = []
        if kp_dir.exists():
            for p in sorted(kp_dir.glob("*.json"))[:15]:
                try:
                    raw = json.loads(p.read_text(encoding="utf-8-sig"))
                    frames.append(self._ensure_s6_frame_format(raw))
                except Exception:
                    pass
        return frames or [{"body": [], "left_hand": None, "right_hand": None, "face": []}]

    def _generate_trajectory_file(
        self,
        known_glosses: List[str],
        effective_fps: float,
        total_frames: int,
    ) -> str:
        """Salva um arquivo de trajetória em `avatar/entry/` e retorna a URI `s6://trajectories/...`."""
        file_id = f"traj_{uuid.uuid4().hex[:10]}.json"
        out_path = self.trajectory_dir / file_id

        kp_dir = Path(__file__).parent / "dataset_maos" / "keypoints"
        d_files = sorted(kp_dir.glob("d*.json")) if kp_dir.exists() else []
        l_files = sorted(kp_dir.glob("l*.json")) if kp_dir.exists() else []
        aspect = 1280.0 / 720.0

        gloss_list = known_glosses or ["ALUNO"]
        frames_per_sign = max(12, int(round(total_frames / max(1, len(gloss_list)))))
        frames_out: List[Dict[str, Any]] = []

        for g_idx, gloss in enumerate(gloss_list):
            raw_d = None
            raw_l = None
            if d_files:
                d_idx = abs(hash(gloss)) % len(d_files)
                try:
                    raw_d = json.loads(d_files[d_idx].read_text(encoding="utf-8-sig"))
                except Exception:
                    pass
            if l_files and (abs(hash(gloss)) % 3 == 0):
                l_idx = abs(hash(gloss)) % len(l_files)
                try:
                    raw_l = json.loads(l_files[l_idx].read_text(encoding="utf-8-sig"))
                except Exception:
                    pass

            base_r = None
            maos_preview = []
            if raw_d and raw_d.get("maos"):
                m0 = raw_d["maos"][0]
                kps = sorted(m0.get("keypoints", []), key=lambda k: k.get("id", 0))
                if len(kps) >= 21:
                    base_r = [[float(k["x"]) * aspect, float(k["y"]), float(k["z"]) * aspect] for k in kps[:21]]
                    maos_preview.append(m0)

            base_l = None
            if raw_l and raw_l.get("maos"):
                m1 = raw_l["maos"][0]
                kps_l = sorted(m1.get("keypoints", []), key=lambda k: k.get("id", 0))
                if len(kps_l) >= 21:
                    base_l = [[float(k["x"]) * aspect, float(k["y"]), float(k["z"]) * aspect] for k in kps_l[:21]]
                    m1_copy = dict(m1)
                    m1_copy["lado"] = "Left"
                    maos_preview.append(m1_copy)

            if not base_r and self._sample_frames:
                sample_idx = g_idx % len(self._sample_frames)
                base_r = self._sample_frames[sample_idx].get("left_hand") or self._sample_frames[sample_idx].get("right_hand")

            if not base_r:
                base_r = [[0.8, 0.5, 0.0] for _ in range(21)]

            for step in range(frames_per_sign):
                t = step / max(1, frames_per_sign - 1)
                bob_y = -0.05 * math.sin(t * math.pi)
                sway_x = 0.02 * math.cos(t * math.pi)

                cur_r = [[p[0] + sway_x, p[1] + bob_y, p[2]] for p in base_r]
                cur_l = [[p[0] - sway_x, p[1] + bob_y, p[2]] for p in base_l] if base_l else None

                cur_maos = []
                for m in maos_preview:
                    cur_maos.append({
                        "lado": m.get("lado", "Right"),
                        "keypoints": [
                            {"id": k["id"], "x": k["x"] + sway_x * 0.5, "y": k["y"] + bob_y, "z": k.get("z", 0.0)}
                            for k in m.get("keypoints", [])
                        ]
                    })

                frames_out.append({
                    "body": [],
                    "left_hand": cur_r,
                    "right_hand": cur_l,
                    "maos": cur_maos,
                    "corpo": [],
                })

        trajectory_doc = {
            "fps": effective_fps,
            "frame_count": len(frames_out),
            "coordinates": "NORMALIZED",
            "glosses_synthesized": known_glosses,
            "frames": frames_out,
        }
        out_path.write_text(json.dumps(trajectory_doc, ensure_ascii=False), encoding="utf-8")
        return f"s6://trajectories/{file_id}"

    # =========================================================================
    # HANDLER 1: `ods.s6.synthesis.command` -> `ods.s6.synthesis.reply`
    # =========================================================================
    def handle_s6_synthesis_command(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        payload = msg.get("payload", {})
        glosses = payload.get("glosses") or []
        speed_factor = float(payload.get("speed_factor", 1.0))
        req_markers = payload.get("markers") or []
        lex_version = payload.get("lexicon_version", self.pipeline.lexicon_version)

        log.info("========== [MOCK S6: RECEBIDO `ods.s6.synthesis.command`] ==========")
        log.info(" message_id=%s | glosses=%s | speed_factor=%s | markers=%s",
                 msg.get("message_id"), glosses, speed_factor, req_markers)

        if not glosses or speed_factor <= 0:
            return {
                "status": "ERROR",
                "error": {"code": "INVALID_REQUEST", "message": "glosses vazia ou speed_factor inválido"},
            }

        if lex_version != self.pipeline.lexicon_version:
            return {
                "status": "ERROR",
                "error": {
                    "code": "UNKNOWN_LEXICON_VERSION",
                    "message": f"Versão {lex_version} diferente de {self.pipeline.lexicon_version}",
                },
            }

        # Separa sinais conhecidos (presentes nas 100 palavras de lexicon_a6.json) vs unknown_signs
        known_glosses: List[str] = []
        orig_indices: List[int] = []
        unknown_signs: List[str] = []

        for idx, g in enumerate(glosses):
            norm = normalize_gloss(g)
            if norm in self.pipeline.lexicon_signs:
                known_glosses.append(norm)
                orig_indices.append(idx)
            else:
                unknown_signs.append(g)

        if not known_glosses:
            return {
                "status": "ERROR",
                "error": {"code": "EMPTY_SEQUENCE", "message": f"Nenhum sinal conhecido: {unknown_signs}"},
                "unknown_signs": unknown_signs,
            }

        effective_fps = round(self.BASE_FPS * speed_factor, 2)
        sign_duration_ms = int(round((self.FRAMES_PER_SIGN / effective_fps) * 1000))
        trans_duration_ms = int(round((self.FRAMES_PER_TRANSITION / effective_fps) * 1000))

        segments: List[Dict[str, Any]] = []
        sign_only_Bounds: List[tuple[int, int]] = []
        current_ms = 0
        total_frames = 0

        for k, g_norm in enumerate(known_glosses):
            start_ms = current_ms
            end_ms = start_ms + sign_duration_ms
            segments.append({
                "sign": glosses[orig_indices[k]],
                "start_ms": start_ms,
                "end_ms": end_ms,
                "is_transition": False,
            })
            sign_only_Bounds.append((start_ms, end_ms))
            current_ms = end_ms
            total_frames += self.FRAMES_PER_SIGN

            # Adiciona segmento de transição entre um sinal e outro
            if k < len(known_glosses) - 1:
                t_start = current_ms
                t_end = t_start + trans_duration_ms
                segments.append({
                    "sign": None,
                    "start_ms": t_start,
                    "end_ms": t_end,
                    "is_transition": True,
                })
                current_ms = t_end
                total_frames += self.FRAMES_PER_TRANSITION

        # Calcula os intervalos em ms dos marcadores faciais solicitados por A6
        facial_markers_out: List[Dict[str, Any]] = []
        for m in req_markers:
            m_name = m.get("marker", "NEUTRO")
            lo = int(m.get("from_gloss_index", 0))
            hi = int(m.get("to_gloss_index", len(glosses) - 1))
            covered = [k for k, o_idx in enumerate(orig_indices) if lo <= o_idx <= hi]
            if covered:
                facial_markers_out.append({
                    "marker": m_name,
                    "start_ms": sign_only_Bounds[covered[0]][0],
                    "end_ms": sign_only_Bounds[covered[-1]][1],
                })

        trajectory_ref = self._generate_trajectory_file(known_glosses, effective_fps, total_frames)

        reply_payload = {
            "status": "OK",
            "fps": effective_fps,
            "frame_count": total_frames,
            "duration_ms": current_ms,
            "trajectory_ref": trajectory_ref,
            "segments": segments,
            "facial_markers": facial_markers_out,
            "unknown_signs": unknown_signs,
        }
        log.info(" [MOCK S6 -> A6] Respondendo `ods.s6.synthesis.reply`: trajectory_ref=%s | duration=%dms | unknown=%s",
                 trajectory_ref, current_ms, unknown_signs)
        log.info("====================================================================")
        return reply_payload

    # =========================================================================
    # HANDLER 2: `ods.i4.lexicon_capture.command` -> `ods.i4.lexicon_capture.reply`
    # =========================================================================
    def handle_i4_capture_command(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        payload = msg.get("payload", {})
        sign_label = normalize_gloss(payload.get("sign_label", "NOVO_SINAL"))
        start_at = payload.get("start_at", "2026-11-03T14:10:00.000Z")
        end_at = payload.get("end_at", "2026-11-03T14:10:02.500Z")

        log.info("========== [MOCK I4: RECEBIDO `ods.i4.lexicon_capture.command`] ==========")
        log.info(" Capturando sinal '%s' (%s -> %s)", sign_label, start_at, end_at)

        reply_payload = {
            "status": "OK",
            "sign_label": sign_label,
            "captured_from": start_at,
            "captured_until": end_at,
            "fps": 30.0,
            "frame_count": 75,
            "sequence_ref": f"registry://lexicon/{sign_label}/v1",
        }
        log.info(" [MOCK I4 -> A6] Respondendo `ods.i4.lexicon_capture.reply`: sequence_ref=%s",
                 reply_payload["sequence_ref"])
        log.info("==========================================================================")
        return reply_payload

    def initialize(self):
        if not HAS_CONFLUENT_KAFKA:
            log.info("confluent_kafka não instalado. Mock S6 + I4 funcionando em modo direto (in-memory).")
            return
        log.info("Iniciando Mock S6 + I4 conectado ao broker %s", self.bootstrap)
        self._consumer = Consumer({
            "bootstrap.servers": self.bootstrap,
            "group.id": self.group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        })
        self._producer = Producer({
            "bootstrap.servers": self.bootstrap,
            "linger.ms": 10,
        })
        topics = [TOPIC_S6_SYNTHESIS_CMD, TOPIC_I4_CAPTURE_CMD]
        self._consumer.subscribe(topics)
        log.info("Mock ouvindo comandos nos tópicos: %s", topics)

    def stop(self, *args, **kwargs):
        log.info("Encerrando Mock S6 + I4...")
        self._running = False

    def process_raw_command(self, raw_bytes: bytes) -> Optional[Dict[str, Any]]:
        try:
            msg = json.loads(raw_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

        ok, why = validate_ods_envelope(msg)
        if not ok:
            log.warning("Comando descartado no Mock: %s", why)
            return None

        mid = msg["message_id"]
        if mid in self._seen_commands:
            # Reenvia mesma reply por idempotência
            cached = self._seen_commands[mid]
            self._publish_reply(cached)
            return cached

        schema = msg["schema"]
        session_id = msg.get("session_id")

        if schema == TOPIC_S6_SYNTHESIS_CMD:
            reply_payload = self.handle_s6_synthesis_command(msg)
            reply_env = build_ods_envelope(
                message_type="reply",
                schema=TOPIC_S6_SYNTHESIS_RPL,
                producer="S6",
                session_id=session_id,
                correlation_id=mid,
                payload=reply_payload,
            )
        elif schema == TOPIC_I4_CAPTURE_CMD:
            reply_payload = self.handle_i4_capture_command(msg)
            reply_env = build_ods_envelope(
                message_type="reply",
                schema=TOPIC_I4_CAPTURE_RPL,
                producer="I4",
                session_id=session_id,
                correlation_id=mid,
                payload=reply_payload,
            )
        else:
            return None

        self._seen_commands[mid] = reply_env
        self._publish_reply(reply_env)
        return reply_env

    def _publish_reply(self, reply_env: Dict[str, Any]):
        if not self._producer:
            return
        topic = reply_env["schema"]
        key = (reply_env.get("correlation_id") or reply_env["message_id"]).encode("utf-8")
        val = json.dumps(reply_env, ensure_ascii=False).encode("utf-8")
        self._producer.produce(topic=topic, key=key, value=val)
        self._producer.flush(5.0)

    def run(self):
        self.initialize()
        if not HAS_CONFLUENT_KAFKA or not self._consumer:
            log.info("Mock S6 + I4 aguardando em modo direto local.")
            return
        self._running = True
        try:
            signal.signal(signal.SIGINT, self.stop)
            signal.signal(signal.SIGTERM, self.stop)
        except ValueError:
            pass  # Rodando em thread de background (ex: servidor_a6)

        log.info("Mock S6 + I4 pronto! Aguardando comandos de A6 (poll=1.0s)...")
        try:
            while self._running:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("Erro Kafka no Mock: %s", msg.error())
                    continue
                self.process_raw_command(msg.value())
        finally:
            if self._producer:
                self._producer.flush(5.0)
            if self._consumer:
                self._consumer.close()
            log.info("Mock S6 + I4 encerrado.")


if __name__ == "__main__":
    MockS6AndI4Service().run()

