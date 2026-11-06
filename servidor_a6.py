"""
servidor_a6.py
===============
Servidor Web Completo da Aplicação A6 (Camada 4 · Libras · Arquitetura 6c).

Roda 100% em nuvem ou localmente sem depender de bibliotecas extras além do Python padrão + confluent-kafka!
Integra todas as 6 atividades de A6 em uma única aplicação web acessível (`http://localhost:8080`):
1. Gramática e Léxico (`LEXICON` de 100 sinais + reordenação [TEMPO] + [SUJEITO] + [OBJETO] + [VERBO]).
2. Conjunto fechado de Marcadores Gramaticais Faciais (`INTERROGATIVA_QU`, `INTERROGATIVA_SN`, `NEGACAO`, `AFIRMACAO`, `INTENSIDADE`, `NEUTRO`).
3. Controle de Velocidade (`speed_factor`: 0.5x a 2.0x) em `ods.s6.synthesis.command`.
4. Marcadores Gramaticais na Saída (com `start_ms` e `end_ms` retornados na reply de S6).
5. Sugestão de Correção somente com palavras (pré-validação antes do S6 + `alternatives` de I4 + `unknown_signs` de S6).
6. Renderização do Avatar 3D no navegador (Three.js / `<model-viewer>` carregando `.glb` + player de trajetória 2D/3D dos keypoints).

Inclui ainda:
- Thread do Consumidor Kafka de A6 (`kafka_integration.py`) rodando em background.
- Thread do Mock de S6 e I4 (`mock_s6_e_i4.py`) opcional em background (assim nunca dá timeout mesmo se S6/I4 estiverem desligados!).

Uso:
    python servidor_a6.py --port 8080
"""

from __future__ import annotations

import argparse
import json
import logging
import mimetypes
import os
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from kafka_integration import (
    TOPIC_I4_CAPTURE_CMD,
    TOPIC_I4_FACIAL_MARKER,
    TOPIC_I4_SIGN,
    TOPIC_S6_SYNTHESIS_CMD,
    LibrasCorrectionKafkaWorker,
    build_ods_envelope,
    get_bootstrap_servers,
)
from mock_s6_e_i4 import MockS6AndI4Service
from sugestao_correcao_pipeline import LibrasCorrectionPipeline, normalize_gloss

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("a6.servidor")

BASE_DIR = Path(__file__).parent


class A6ApplicationBackend:
    """Orquestra estado em tempo real da Aplicação A6, Barramento Kafka e Mock de S6/I4."""

    def __init__(self, start_kafka: bool = True, start_mock_neighbors: bool = True):
        self.pipeline = LibrasCorrectionPipeline()
        self.worker = LibrasCorrectionKafkaWorker(
            pipeline=self.pipeline,
            pause_threshold_sec=3.0,
            realtime_idle_timeout_sec=3.5,
        )
        self.mock_service = MockS6AndI4Service()

        self.recent_events: List[Dict[str, Any]] = []
        self.synthesis_history: List[Dict[str, Any]] = []
        self.capture_history: List[Dict[str, Any]] = []
        self.kafka_connected = False
        self._lock = threading.Lock()

        if start_kafka:
            self._start_background_threads(start_mock_neighbors)

    def _start_background_threads(self, start_mock: bool):
        def _run_a6_consumer():
            try:
                self.worker.initialize()
                self.kafka_connected = True
                self.worker._running = True
                while self.worker._running:
                    msg = self.worker._consumer.poll(1.0)
                    if msg is None:
                        for closed in self.worker.aggregator.flush_idle_contexts():
                            self.worker._salvar_frase_fechada(closed)
                        continue
                    if msg.error():
                        continue
                    res = self.worker.process_message(msg.value())
                    if res is not None:
                        with self._lock:
                            self.recent_events.insert(0, {
                                "timestamp": time.strftime("%H:%M:%S"),
                                "result": res,
                            })
                            self.recent_events = self.recent_events[:30]
            except Exception as exc:
                log.warning("Kafka Consumer A6 em modo offline/local: %s", exc)
                self.kafka_connected = False

        t_a6 = threading.Thread(target=_run_a6_consumer, daemon=True)
        t_a6.start()

        if start_mock:
            def _run_mock():
                try:
                    self.mock_service.run()
                except Exception as exc:
                    log.warning("Mock S6/I4 offline no Kafka (usando resposta direta em memória): %s", exc)

            t_mock = threading.Thread(target=_run_mock, daemon=True)
            t_mock.start()

    # =========================================================================
    # AÇÃO 1: Traduzir Português -> Libras (A6 -> S6)
    # =========================================================================
    def translate_pt_to_libras(
        self,
        text: str,
        speed_factor: float = 1.0,
        explicit_marker: Optional[str] = None,
        force_raw_glosses: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        prep = self.pipeline.prepare_synthesis_command(
            input_words_or_sentence=text,
            speed_factor=speed_factor,
            explicit_marker=explicit_marker if explicit_marker != "AUTO" else None,
            auto_apply_top_suggestion=(force_raw_glosses is None),
        )

        if force_raw_glosses is not None:
            prep["s6_command_payload"]["glosses"] = force_raw_glosses

        cmd_env = build_ods_envelope(
            message_type="command",
            schema=TOPIC_S6_SYNTHESIS_CMD,
            target="S6",
            session_id="web-session",
            payload=prep["s6_command_payload"],
        )

        # Publica no Kafka real se conectado
        if self.kafka_connected and self.worker._producer:
            try:
                self.worker._producer.produce(
                    topic=TOPIC_S6_SYNTHESIS_CMD,
                    key=cmd_env["message_id"].encode("utf-8"),
                    value=json.dumps(cmd_env, ensure_ascii=False).encode("utf-8"),
                )
                self.worker._producer.flush(2.0)
            except Exception as exc:
                log.warning("Erro ao publicar no Kafka: %s", exc)

        # Garante resposta imediata processando pelo handler S6 (real/mock)
        reply_payload = self.mock_service.handle_s6_synthesis_command(cmd_env)
        eval_reply = self.pipeline.evaluate_s6_synthesis_reply(reply_payload)

        # Carrega os quadros da trajetória gerada para animar o canvas/avatar no navegador
        traj_ref = reply_payload.get("trajectory_ref", "")
        traj_filename = traj_ref.split("/")[-1] if "/" in traj_ref else ""
        traj_path = self.mock_service.trajectory_dir / traj_filename
        traj_frames = []
        if traj_path.exists():
            try:
                traj_doc = json.loads(traj_path.read_text(encoding="utf-8"))
                traj_frames = traj_doc.get("frames", [])[:90]
            except Exception:
                pass

            # Gera o .glb animado em background para visualização 3D imediata no site
            def _gerar_glb():
                try:
                    candidatos_py = [
                        BASE_DIR / ".venv" / "blender" / "bin" / "python",
                        BASE_DIR / ".venv" / "blender" / "Scripts" / "python.exe",
                        BASE_DIR / "envs" / "blender" / "bin" / "python",
                        BASE_DIR / "envs" / "blender" / "Scripts" / "python.exe",
                    ]
                    py_blender = next((str(p) for p in candidatos_py if p.exists() or p.is_symlink()), None)
                    if py_blender:
                        cmd = [py_blender, str(BASE_DIR / "avatar" / "generate_video.py"), "--arquivo", str(traj_path)]
                        subprocess.run(cmd, timeout=60)
                except Exception as exc:
                    log.warning("Falha ao disparar generate_video.py pelo servidor web: %s", exc)

            threading.Thread(target=_gerar_glb, daemon=True).start()

        record = {
            "timestamp": time.strftime("%H:%M:%S"),
            "preparation": prep,
            "s6_command_envelope": cmd_env,
            "s6_reply_payload": reply_payload,
            "s6_reply_evaluation": eval_reply,
            "trajectory_preview_frames": traj_frames,
        }
        with self._lock:
            self.synthesis_history.insert(0, record)
            self.synthesis_history = self.synthesis_history[:15]
        return record

    # =========================================================================
    # AÇÃO 2: Simular recepção de sinal de I4 (`ods.i4.sign`) em Tempo Real
    # =========================================================================
    def inject_i4_sign(
        self,
        sign: str,
        confidence: float,
        alternatives: List[Dict[str, Any]],
        facial_marker: Optional[str] = None,
        gap_seconds: float = 0.5,
    ) -> Dict[str, Any]:
        session_id = "web-realtime-session"
        ctx = self.worker.aggregator.active_contexts.get(session_id)
        base_epoch = (ctx["last_end_epoch"] if (ctx and ctx["last_end_epoch"]) else time.time()) + float(gap_seconds)
        start_iso = datetime_from_epoch(base_epoch)
        end_iso = datetime_from_epoch(base_epoch + 1.1)

        ev_sign = build_ods_envelope(
            message_type="event",
            schema=TOPIC_I4_SIGN,
            producer="I4",
            session_id=session_id,
            payload={
                "sign": sign,
                "confidence": float(confidence),
                "start_at": start_iso,
                "end_at": end_iso,
                "alternatives": alternatives,
                "signer_id": "signer-01",
            },
        )

        # Publica no Kafka real se conectado, ou processa diretamente
        raw_bytes = json.dumps(ev_sign, ensure_ascii=False).encode("utf-8")
        if self.kafka_connected and self.worker._producer:
            try:
                self.worker._producer.produce(TOPIC_I4_SIGN, key=session_id.encode("utf-8"), value=raw_bytes)
                self.worker._producer.flush(1.5)
            except Exception:
                pass

        res = self.worker.process_message(raw_bytes)

        if facial_marker and facial_marker != "NEUTRO":
            ev_mark = build_ods_envelope(
                message_type="event",
                schema=TOPIC_I4_FACIAL_MARKER,
                producer="I4",
                session_id=session_id,
                payload={
                    "marker": facial_marker,
                    "confidence": 0.88,
                    "start_at": start_iso,
                    "end_at": end_iso,
                },
            )
            self.worker.process_message(json.dumps(ev_mark).encode("utf-8"))

        with self._lock:
            self.recent_events.insert(0, {
                "timestamp": time.strftime("%H:%M:%S"),
                "result": res,
            })
            self.recent_events = self.recent_events[:30]
        return res or {}

    def apply_user_suggestion_on_active_context(self, original_gloss: str, chosen_gloss: str) -> Dict[str, Any]:
        """Permite ao usuário clicar em uma sugestão alternativa na interface para corrigir o sinal de baixa confiança."""
        for ctx in self.worker.aggregator.active_contexts.values():
            for i, g in enumerate(ctx["glosses"]):
                if g == normalize_gloss(original_gloss):
                    ctx["glosses"][i] = normalize_gloss(chosen_gloss)
                    ctx["categories"][i] = self.pipeline.lexicon_signs.get(normalize_gloss(chosen_gloss), "OBJETO")
        for sent in self.worker.aggregator.closed_sentences:
            for i, g in enumerate(sent["raw_gloss_sequence"]):
                if g == normalize_gloss(original_gloss):
                    sent["raw_gloss_sequence"][i] = normalize_gloss(chosen_gloss)
            sent["grammatical_gloss_sequence"], _ = self.pipeline.reorganize_syntax(sent["raw_gloss_sequence"])
        return {"status": "OK", "replaced": original_gloss, "with": chosen_gloss}

    def close_current_sentence_now(self) -> Optional[Dict[str, Any]]:
        for sid in list(self.worker.aggregator.active_contexts.keys()):
            closed = self.worker.aggregator.close_context(sid, close_reason="Fechamento manual pelo usuário / pausa")
            if closed:
                self.worker._salvar_frase_fechada(closed)
                return closed
        return None

    # =========================================================================
    # AÇÃO 3: Captura de Novo Sinal p/ Léxico (`ods.i4.lexicon_capture.command`)
    # =========================================================================
    def capture_new_lexicon_sign(self, sign_label: str, signer_id: str = "signer-01") -> Dict[str, Any]:
        now_epoch = time.time()
        cmd = build_ods_envelope(
            message_type="command",
            schema=TOPIC_I4_CAPTURE_CMD,
            target="I4",
            payload={
                "sign_label": normalize_gloss(sign_label),
                "source_id": "cam0",
                "start_at": datetime_from_epoch(now_epoch),
                "end_at": datetime_from_epoch(now_epoch + 2.5),
                "signer_id": signer_id,
            },
        )
        reply = self.mock_service.handle_i4_capture_command(cmd)
        rec = {"timestamp": time.strftime("%H:%M:%S"), "command": cmd, "reply": reply}
        with self._lock:
            self.capture_history.insert(0, rec)
        return rec

    def get_state(self) -> Dict[str, Any]:
        # Verifica frases ociosas antes de devolver estado
        for closed in self.worker.aggregator.flush_idle_contexts():
            self.worker._salvar_frase_fechada(closed)

        active_list = []
        for sid, ctx in self.worker.aggregator.active_contexts.items():
            reordered, _ = self.pipeline.reorganize_syntax(ctx["glosses"])
            active_list.append({
                "context_id": ctx["context_id"],
                "session_id": sid,
                "raw_glosses": ctx["glosses"],
                "grammatical_order": reordered,
                "facial_markers": ctx["facial_markers"] or ["NEUTRO"],
                "signs_detail": ctx["signs_detail"],
            })

        return {
            "kafka_broker": self.worker.bootstrap,
            "kafka_connected": self.kafka_connected,
            "lexicon_version": self.pipeline.lexicon_version,
            "lexicon_count": len(self.pipeline.valid_glosses),
            "closed_markers": self.pipeline.markers,
            "lexicon_signs": self.pipeline.lexicon_data.get("signs", []),
            "active_contexts": active_list,
            "closed_sentences": list(reversed(self.worker.aggregator.closed_sentences[-10:])),
            "recent_i4_events": self.recent_events[:10],
            "synthesis_history": self.synthesis_history[:5],
            "capture_history": self.capture_history[:5],
        }


def datetime_from_epoch(epoch: float) -> str:
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


# =============================================================================
# FRONT-END HTML/JS EMBUTIDO (ACESSÍVEL, COM AVATAR 3D .GLB + SKELETON + KAFKA)
# =============================================================================
INDEX_HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>A6 · Tradutor Bidirecional de LIBRAS & Avatar 3D (Arquitetura 6c)</title>
  <script type="module" src="https://ajax.googleapis.com/ajax/libs/model-viewer/3.5.0/model-viewer.min.js"></script>
  <style>
    :root {
      --bg: #0f172a;
      --card: #1e293b;
      --border: #334155;
      --text: #f8fafc;
      --muted: #94a3b8;
      --primary: #38bdf8;
      --accent: #818cf8;
      --ok: #34d399;
      --warn: #fbbf24;
      --danger: #f87171;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Segoe UI', system-ui, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 18px; line-height: 1.45; }
    header {
      display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap;
      background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px 20px; margin-bottom: 18px;
    }
    header h1 { font-size: 1.25rem; color: var(--primary); }
    .badge { padding: 4px 10px; border-radius: 999px; font-size: 0.78rem; font-weight: 600; background: #0284c733; color: var(--primary); border: 1px solid var(--primary); }
    .badge.ok { background: #05966933; color: var(--ok); border-color: var(--ok); }
    .badge.warn { background: #d9770633; color: var(--warn); border-color: var(--warn); }

    .grid { display: grid; grid-template-columns: 1.15fr 1.15fr 1fr; gap: 16px; }
    @media (max-width: 1200px) { .grid { grid-template-columns: 1fr; } }

    .card {
      background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px;
      display: flex; flex-direction: column; gap: 12px;
    }
    .card h2 { font-size: 1.05rem; color: var(--primary); border-bottom: 1px solid var(--border); padding-bottom: 8px; }
    label { font-size: 0.84rem; color: var(--muted); display: block; margin-bottom: 4px; }
    input[type="text"], select {
      width: 100%; padding: 9px 12px; border-radius: 8px; border: 1px solid var(--border);
      background: #0b1120; color: var(--text); font-size: 0.92rem;
    }
    button {
      background: var(--primary); color: #0f172a; border: none; border-radius: 8px;
      padding: 9px 14px; font-weight: 700; cursor: pointer; transition: 0.15s; font-size: 0.88rem;
    }
    button:hover { filter: brightness(1.1); }
    button.secondary { background: #334155; color: var(--text); }
    button.warn-btn { background: var(--warn); color: #0f172a; }

    .pill-list { display: flex; flex-wrap: wrap; gap: 6px; }
    .pill {
      background: #0b1120; border: 1px solid var(--border); padding: 4px 10px;
      border-radius: 6px; font-size: 0.82rem; font-family: monospace;
    }
    .pill.gloss { border-color: var(--primary); color: var(--primary); font-weight: bold; }
    .pill.sug { border-color: var(--warn); color: var(--warn); cursor: pointer; }
    .pill.marker { border-color: var(--accent); color: var(--accent); }

    .box { background: #0b1120; border: 1px solid var(--border); border-radius: 8px; padding: 10px; font-size: 0.85rem; }
    .row { display: flex; gap: 10px; align-items: center; }
    model-viewer { width: 100%; height: 290px; background: #0b1120; border-radius: 10px; border: 1px solid var(--border); }
    canvas#trajCanvas { width: 100%; height: 190px; background: #070b14; border-radius: 8px; border: 1px solid var(--border); }
    pre { font-family: monospace; font-size: 0.76rem; white-space: pre-wrap; word-break: break-all; color: #cbd5e1; max-height: 170px; overflow-y: auto; }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>🤟 A6 · Tradutor Bidirecional de LIBRAS & Avatar 3D (Camada 4 · Arquitetura 6c)</h1>
      <div style="font-size: 0.82rem; color: var(--muted);">
        Integração em Tempo Real via Kafka com <b>I4</b> (Captura/Visão) e <b>S6</b> (Síntese de Trajetórias) + Sugestão de Correção
      </div>
    </div>
    <div class="row">
      <span id="kafkaBadge" class="badge">Kafka: verificando...</span>
      <span id="lexiconBadge" class="badge ok">LEXICON: 100 sinais</span>
    </div>
  </header>

  <div class="grid">
    <!-- COLUNA 1: PORTUGUÊS -> LIBRAS (A6 -> S6) -->
    <div class="card">
      <h2>1. Português ➔ LIBRAS (Comando p/ S6 + Pré-Validação)</h2>
      <div>
        <label>Digite uma frase em Português (ou glosas fora do léxico para testar a Sugestão):</label>
        <input type="text" id="ptInput" value="Amanhã o estudante desejar estudar no colégio?" />
      </div>
      <div class="row">
        <div style="flex:1;">
          <label>Velocidade (`speed_factor`): <b id="speedVal">1.0x</b></label>
          <input type="range" id="speedInput" min="0.5" max="2.0" step="0.1" value="1.0" style="width:100%;"
                 oninput="document.getElementById('speedVal').innerText = this.value + 'x'; applySpeedToViewer(this.value);" />
        </div>
        <div style="flex:1;">
          <label>Marcador Facial Gramatical (`markers`):</label>
          <select id="markerSelect">
            <option value="AUTO">Automático pela Frase</option>
            <option value="INTERROGATIVA_SN">INTERROGATIVA_SN</option>
            <option value="INTERROGATIVA_QU">INTERROGATIVA_QU</option>
            <option value="NEGACAO">NEGACAO</option>
            <option value="AFIRMACAO">AFIRMACAO</option>
            <option value="INTENSIDADE">INTENSIDADE</option>
            <option value="NEUTRO">NEUTRO</option>
          </select>
        </div>
      </div>
      <div class="row">
        <button onclick="translatePtToLibras()" style="flex:2;">🚀 Pré-Validar e Enviar `ods.s6.synthesis.command`</button>
        <button class="secondary" onclick="testUnknownSignFallback()" title="Envia palavra fora do léxico sem substituir antes para testar unknown_signs do S6">Testar `unknown_signs`</button>
      </div>

      <div id="s6ResultBox" class="box">
        <div style="color:var(--muted);">Clique no botão acima para validar as palavras contra o LEXICON de 100 sinais e sintetizar com o S6.</div>
      </div>

      <h2>Autoria de Léxico (`ods.i4.lexicon_capture.command`)</h2>
      <div class="row">
        <input type="text" id="captureInput" placeholder="Nova glosa (ex: PROFESSOR)" value="PROFESSOR" />
        <button class="secondary" onclick="captureSignI4()">🎥 Capturar em I4</button>
      </div>
      <div id="captureBox" class="box" style="display:none;"></div>
    </div>

    <!-- COLUNA 2: LIBRAS -> PORTUGUÊS (I4 -> A6 EM TEMPO REAL) -->
    <div class="card">
      <h2>2. LIBRAS ➔ Português (`ods.i4.sign` + Agregador de Contexto)</h2>
      <div style="font-size:0.82rem; color:var(--muted);">
        Recebe sinais em tempo real do barramento Kafka (I4). Sinais com pausa &lt; 3.0s ficam na <b>mesma frase</b>; pausas &ge; 3.0s fecham a frase e abrem <b>novo contexto</b>.
      </div>

      <div class="box">
        <label>Simular Chegada de Sinal em Tempo Real de I4 (`ods.i4.sign`):</label>
        <div class="row" style="margin-top:6px; flex-wrap:wrap;">
          <button class="secondary" onclick="sendI4Sign('ALUNO', 0.92, [], 'NEUTRO', 0.4)">+ 'ALUNO' (conf 0.92)</button>
          <button class="secondary" onclick="sendI4Sign('ESTUDAR', 0.89, [], 'NEUTRO', 0.4)">+ 'ESTUDAR' (conf 0.89)</button>
          <button class="warn-btn" onclick="sendI4Sign('ONTEM', 0.56, [{sign:'HOJE',confidence:0.35},{sign:'AMANHA',confidence:0.09}], 'NEUTRO', 0.4)">
            ⚠️ + 'ONTEM' (Baixa Conf 0.56 ➔ Sugere 'HOJE')
          </button>
          <button onclick="sendI4Sign('VOCE', 0.94, [], 'INTERROGATIVA_SN', 5.0)">
            ⏸️ Pausa 5s + 'VOCE' (Novo Contexto!)
          </button>
          <button class="secondary" onclick="closeSentenceNow()">✅ Fechar Frase Agora</button>
        </div>
      </div>

      <div class="box">
        <div style="font-weight:700; color:var(--primary); margin-bottom:6px;">🟢 Frase Ativa (Contexto Aberto em Tempo Real):</div>
        <div id="activeContextBox">Nenhum sinal em aberto no momento.</div>
      </div>

      <div class="box">
        <div style="font-weight:700; color:var(--ok); margin-bottom:6px;">📚 Frases Fechadas por Contexto (`FRASE_FECHADA_*.json`):</div>
        <div id="closedSentencesBox">Nenhuma frase fechada ainda.</div>
      </div>
    </div>

    <!-- COLUNA 3: AVATAR 3D & TRAJETÓRIA S6 -->
    <div class="card">
      <h2>3. Renderização do Avatar 3D & Trajetória S6</h2>
      <div class="row" style="justify-content:space-between;">
        <span style="font-size:0.82rem; color:var(--muted);">Modelo 3D Mixamo (`.glb`):</span>
        <select id="glbSelect" onchange="changeGlbModel(this.value)" style="width:auto;">
          <option value="/avatar/saida/aluno_corrigido.glb">aluno_corrigido.glb (Animado)</option>
          <option value="/avatar/ybot.glb">ybot.glb (Base)</option>
        </select>
      </div>

      <model-viewer id="avatarViewer" src="/avatar/saida/aluno_corrigido.glb" camera-controls autoplay shadow-intensity="1" ar></model-viewer>
      <div class="row" style="margin-top:6px; margin-bottom:10px; gap:8px;">
        <button type="button" class="btn" onclick="replayAvatar()" style="flex:1; padding:7px 12px; font-size:0.85rem;">🔄 Replay da Animação (Tocar Novamente)</button>
      </div>

      <div>
        <div class="row" style="justify-content:space-between; margin-bottom:4px;">
          <label>Player da Trajetória Recebida de S6 (`trajectory_ref`):</label>
          <span id="trajStatus" class="badge">Pronto</span>
        </div>
        <canvas id="trajCanvas" width="420" height="190"></canvas>
      </div>

      <div class="box">
        <div style="font-weight:700; margin-bottom:4px;">📖 Artefato `LEXICON` de A6 (100 Sinais Oficiais + Marcadores):</div>
        <div id="lexiconPills" class="pill-list" style="max-height:110px; overflow-y:auto;"></div>
      </div>
    </div>
  </div>

  <script>
    let currentTrajFrames = [];
    let currentTrajIndex = 0;
    let currentSpeedFactor = 1.0;

    function applySpeedToViewer(speed) {
      currentSpeedFactor = parseFloat(speed) || 1.0;
      const viewer = document.getElementById('avatarViewer');
      if (viewer) viewer.timeScale = currentSpeedFactor;
    }

    function replayAvatar() {
      const viewer = document.getElementById('avatarViewer');
      if (viewer) {
        viewer.currentTime = 0;
        try {
          viewer.play({ repetitions: 1 });
        } catch (e) {
          viewer.play();
        }
      }
    }

    function changeGlbModel(src) {
      const viewer = document.getElementById('avatarViewer');
      if (viewer) {
        viewer.src = src;
        viewer.currentTime = 0;
        try {
          viewer.play({ repetitions: 1 });
        } catch (e) {
          viewer.play();
        }
      }
    }

    // Configura o model-viewer para tocar 1 vez e parar na pose de repouso
    window.addEventListener('DOMContentLoaded', () => {
      const v = document.getElementById('avatarViewer');
      if (v) {
        v.addEventListener('load', () => {
          v.currentTime = 0;
          try { v.play({ repetitions: 1 }); } catch (e) { v.play(); }
        });
      }
    });

    async function fetchState() {
      try {
        const res = await fetch('/api/state');
        const data = await res.json();

        const kb = document.getElementById('kafkaBadge');
        kb.innerText = `Kafka (${data.kafka_broker}): ` + (data.kafka_connected ? 'Conectado' : 'Local/Mock');
        kb.className = 'badge ' + (data.kafka_connected ? 'ok' : 'warn');

        document.getElementById('lexiconBadge').innerText = `${data.lexicon_version} (${data.lexicon_count} sinais)`;

        // Renderiza LEXICON na primeira vez
        const lexContainer = document.getElementById('lexiconPills');
        if (lexContainer.children.length === 0 && data.lexicon_signs) {
          lexContainer.innerHTML = data.lexicon_signs.map(s =>
            `<span class="pill" title="${s.category}: ${s.sequence_ref}">${s.gloss}</span>`
          ).join('');
        }

        // Contexto ativo
        const actBox = document.getElementById('activeContextBox');
        if (!data.active_contexts || data.active_contexts.length === 0) {
          actBox.innerHTML = '<span style="color:var(--muted);">Aguardando sinais de I4... (ou frase anterior já foi fechada por pausa)</span>';
        } else {
          actBox.innerHTML = data.active_contexts.map(ctx => {
            const detailsHtml = ctx.signs_detail.map(sd => {
              if (sd.needs_suggestion && sd.alternatives && sd.alternatives.length > 0) {
                const btns = sd.alternatives.map(a =>
                  `<span class="pill sug" onclick="acceptSuggestion('${sd.gloss}', '${a.gloss}')" title="Clique para substituir por esta sugestão">👉 Sugestão: ${a.gloss} (${Math.round(a.confidence*100)}%)</span>`
                ).join(' ');
                return `<div style="margin-top:6px;"><span class="pill" style="border-color:var(--danger);color:var(--danger);">${sd.gloss} (conf ${sd.confidence})</span> ${btns}</div>`;
              }
              return `<span class="pill gloss">${sd.gloss} (${sd.confidence})</span>`;
            }).join(' ');

            return `
              <div><b>ID do Contexto:</b> <span class="badge">${ctx.context_id}</span> | <b>Marcadores:</b> <span class="pill marker">${ctx.facial_markers.join(', ')}</span></div>
              <div style="margin-top:6px;"><b>Sinais recebidos:</b> ${detailsHtml}</div>
              <div style="margin-top:6px;"><b>Ordem Gramatical [T+S+O+V]:</b> <span class="pill gloss">${ctx.grammatical_order.join(' ➔ ')}</span></div>
            `;
          }).join('<hr style="margin:8px 0;border-color:var(--border);"/>');
        }

        // Frases fechadas
        const closedBox = document.getElementById('closedSentencesBox');
        if (!data.closed_sentences || data.closed_sentences.length === 0) {
          closedBox.innerHTML = '<span style="color:var(--muted);">Nenhuma frase fechada ainda.</span>';
        } else {
          closedBox.innerHTML = data.closed_sentences.map(c => `
            <div style="border-bottom:1px solid var(--border); padding:6px 0;">
              <div><span class="badge ok">${c.context_id}</span> <span style="font-size:0.78rem;color:var(--muted);">${c.close_reason}</span></div>
              <div style="margin-top:4px;"><b>Recebido:</b> <code>${c.raw_gloss_sequence.join(' ')}</code> ➔ <b>Gramática Libras:</b> <span class="pill gloss">${c.grammatical_gloss_sequence.join(' ')}</span></div>
              ${c.had_low_confidence_replacements ? `<div style="margin-top:3px;color:var(--warn);">💡 <b>Frase c/ Sugestão de Baixa Confiança:</b> <code>${c.suggested_corrected_sentence.join(' ')}</code></div>` : ''}
              <div style="font-size:0.78rem; color:var(--accent);">Marcadores Faciais: ${c.facial_markers.join(', ')}</div>
            </div>
          `).join('');
        }
      } catch (e) {}
    }

    async function translatePtToLibras() {
      const text = document.getElementById('ptInput').value;
      const speed = parseFloat(document.getElementById('speedInput').value) || 1.0;
      const marker = document.getElementById('markerSelect').value;

      const res = await fetch('/api/translate', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({text, speed_factor: speed, explicit_marker: marker})
      });
      const data = await res.json();
      renderS6Result(data);
    }

    async function testUnknownSignFallback() {
      const speed = parseFloat(document.getElementById('speedInput').value) || 1.0;
      const res = await fetch('/api/translate', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          text: 'ALUNO ESTUDANTE_FORA_DO_LEXICO ESTUDAR',
          speed_factor: speed,
          force_raw_glosses: ['ALUNO', 'ESTUDANTE', 'ESTUDAR']
        })
      });
      const data = await res.json();
      renderS6Result(data);
    }

    function renderS6Result(data) {
      const prep = data.preparation;
      const rpl = data.s6_reply_payload;
      const ev = data.s6_reply_evaluation;

      let corrHtml = '<span style="color:var(--ok);">✅ Todas as palavras já pertenciam ao LEXICON de 100 sinais.</span>';
      if (prep.pre_validation.word_corrections && prep.pre_validation.word_corrections.length > 0) {
        corrHtml = prep.pre_validation.word_corrections.map(w => {
          const alts = w.suggestions.map(s => `${s.gloss} (${Math.round(s.score*100)}%)`).join(', ');
          return `<div>💡 <b>Pré-Validação em A6:</b> <code>"${w.original_word}"</code> fora do léxico ➔ substituída antes do envio por <span class="pill gloss">${w.applied_gloss}</span> <small>(opções: ${alts})</small></div>`;
        }).join('');
      }

      let unkHtml = '';
      if (ev.has_unknown_signs) {
        const items = Object.entries(ev.suggestions_for_unknown_signs).map(([u, sugs]) =>
          `<div>⚠️ <b>Fallback <code>unknown_signs</code> na Reply de S6:</b> <code>"${u}"</code> ➔ Sugestões do LEXICON: ${sugs.map(s => `<span class="pill sug" onclick="document.getElementById('ptInput').value='${s.gloss}';translatePtToLibras();">${s.gloss}</span>`).join(' ')}</div>`
        ).join('');
        unkHtml = `<div style="margin-top:6px;">${items}</div>`;
      }

      document.getElementById('s6ResultBox').innerHTML = `
        ${corrHtml}
        <div style="margin-top:6px;"><b>Ordem Gramatical Enviada (<code>ods.s6.synthesis.command</code>):</b> <span class="pill gloss">${prep.s6_command_payload.glosses.join(' ➔ ')}</span></div>
        <div style="margin-top:4px;"><b>Marcador Facial:</b> <span class="pill marker">${prep.grammar.facial_marker_applied}</span> | <b>Velocidade:</b> <code>${prep.s6_command_payload.speed_factor}x</code></div>
        <hr style="margin:8px 0; border-color:var(--border);" />
        <div><b>Reply de S6 (<code>ods.s6.synthesis.reply</code>):</b> Status <span class="badge ok">${rpl.status}</span> | Duração: <b>${rpl.duration_ms} ms</b> (${rpl.fps} FPS)</div>
        <div><b>Trajetória (<code>trajectory_ref</code>):</b> <code>${rpl.trajectory_ref}</code></div>
        ${unkHtml}
      `;

      if (data.trajectory_preview_frames && data.trajectory_preview_frames.length > 0) {
        currentTrajFrames = data.trajectory_preview_frames;
        currentTrajIndex = 0;
        document.getElementById('trajStatus').innerText = `Reproduzindo ${currentTrajFrames.length} frames (${rpl.fps} FPS)`;
      }

      // Atualiza o model-viewer com o novo .glb assim que terminar de exportar
      setTimeout(() => {
        const viewer = document.getElementById('avatarViewer');
        if (viewer) {
          viewer.src = `/avatar/saida/aluno_corrigido.glb?t=${Date.now()}`;
          viewer.currentTime = 0;
          try { viewer.play({ repetitions: 1 }); } catch (e) { viewer.play(); }
        }
      }, 1600);
    }

    async function sendI4Sign(sign, confidence, alternatives, facial_marker, gap_seconds) {
      await fetch('/api/i4_sign', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({sign, confidence, alternatives, facial_marker, gap_seconds})
      });
      fetchState();
    }

    async function acceptSuggestion(original_gloss, chosen_gloss) {
      await fetch('/api/accept_suggestion', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({original_gloss, chosen_gloss})
      });
      fetchState();
    }

    async function closeSentenceNow() {
      await fetch('/api/close_sentence', {method: 'POST'});
      fetchState();
    }

    async function captureSignI4() {
      const sign_label = document.getElementById('captureInput').value || 'PROFESSOR';
      const res = await fetch('/api/capture_sign', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({sign_label})
      });
      const data = await res.json();
      const box = document.getElementById('captureBox');
      box.style.display = 'block';
      box.innerHTML = `✅ <b>Reply de I4 (<code>ods.i4.lexicon_capture.reply</code>):</b> Sinal <b>${data.reply.sign_label}</b> gravado em <code>${data.reply.sequence_ref}</code> (${data.reply.frame_count} frames).`;
    }

    // Desenha os keypoints da trajetória recebida de S6 no Canvas 2D
    function drawTrajectoryLoop() {
      const canvas = document.getElementById('trajCanvas');
      if (canvas && currentTrajFrames.length > 0) {
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        const frame = currentTrajFrames[Math.floor(currentTrajIndex) % currentTrajFrames.length];

        // Desenha corpo
        ctx.fillStyle = '#38bdf8';
        (frame.corpo || []).forEach(pt => {
          ctx.beginPath();
          ctx.arc(pt.x * canvas.width, pt.y * canvas.height, 3, 0, Math.PI * 2);
          ctx.fill();
        });

        // Desenha mãos
        (frame.maos || []).forEach(mao => {
          ctx.fillStyle = mao.lado === 'Right' ? '#34d399' : '#fbbf24';
          (mao.keypoints || []).forEach(kp => {
            ctx.beginPath();
            ctx.arc(kp.x * canvas.width, kp.y * canvas.height, 3.5, 0, Math.PI * 2);
            ctx.fill();
          });
        });

        currentTrajIndex = (currentTrajIndex + 0.5 * currentSpeedFactor) % currentTrajFrames.length;
      }
      requestAnimationFrame(drawTrajectoryLoop);
    }

    setInterval(fetchState, 1200);
    fetchState();
    requestAnimationFrame(drawTrajectoryLoop);
  </script>
</body>
</html>
"""


class A6HTTPRequestHandler(BaseHTTPRequestHandler):
    backend: A6ApplicationBackend = None  # Injetado no main()

    def log_message(self, format: str, *args: Any):
        # Silencia logs repetitivos de polling de /api/state
        if "/api/state" not in (args[0] if args else ""):
            log.info("%s - %s", self.address_string(), format % args)

    def _send_json(self, data: Any, status: int = 200):
        raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path in ("/", "/index.html"):
            raw = INDEX_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return

        if path == "/api/state":
            self._send_json(self.backend.get_state())
            return

        # Serve arquivos estáticos de avatar (.glb, .png, etc.)
        if path.startswith("/avatar/"):
            rel_path = path.lstrip("/")
            file_path = (BASE_DIR / rel_path).resolve()
            # Se aluno_corrigido.glb ainda não foi gerado, faz fallback automático para avatar/ybot.glb
            if not file_path.exists() and file_path.name == "aluno_corrigido.glb":
                file_path = (BASE_DIR / "avatar" / "ybot.glb").resolve()
            if file_path.exists() and file_path.is_file():
                mime, _ = mimetypes.guess_type(str(file_path))
                if file_path.suffix.lower() == ".glb":
                    mime = "model/gltf-binary"
                raw = file_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", mime or "application/octet-stream")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return

        self.send_error(404, "Arquivo não encontrado")

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length).decode("utf-8")) if length > 0 else {}

        if parsed.path == "/api/translate":
            res = self.backend.translate_pt_to_libras(
                text=body.get("text", ""),
                speed_factor=float(body.get("speed_factor", 1.0)),
                explicit_marker=body.get("explicit_marker"),
                force_raw_glosses=body.get("force_raw_glosses"),
            )
            self._send_json(res)
            return

        if parsed.path == "/api/i4_sign":
            res = self.backend.inject_i4_sign(
                sign=body.get("sign", "ALUNO"),
                confidence=float(body.get("confidence", 0.90)),
                alternatives=body.get("alternatives", []),
                facial_marker=body.get("facial_marker"),
                gap_seconds=float(body.get("gap_seconds", 0.5)),
            )
            self._send_json(res)
            return

        if parsed.path == "/api/accept_suggestion":
            res = self.backend.apply_user_suggestion_on_active_context(
                original_gloss=body.get("original_gloss", ""),
                chosen_gloss=body.get("chosen_gloss", ""),
            )
            self._send_json(res)
            return

        if parsed.path == "/api/close_sentence":
            res = self.backend.close_current_sentence_now()
            self._send_json(res or {"status": "EMPTY"})
            return

        if parsed.path == "/api/capture_sign":
            res = self.backend.capture_new_lexicon_sign(
                sign_label=body.get("sign_label", "PROFESSOR"),
            )
            self._send_json(res)
            return

        self.send_error(404, "Endpoint não encontrado")


def main():
    ap = argparse.ArgumentParser(description="Servidor Web da Aplicação A6 (Camada 4 · Libras)")
    ap.add_argument("--host", default="0.0.0.0", help="Host para bind (0.0.0.0 para nuvem/rede)")
    ap.add_argument("--port", type=int, default=8080, help="Porta HTTP (padrão: 8080)")
    ap.add_argument("--no-mock", action="store_true", help="Desativa o Mock integrado de S6/I4")
    args = ap.parse_args()

    backend = A6ApplicationBackend(
        start_kafka=True,
        start_mock_neighbors=not args.no_mock,
    )
    A6HTTPRequestHandler.backend = backend

    # Dispara uma primeira síntese de exemplo para já deixar o player populado ao abrir
    backend.translate_pt_to_libras("Amanhã o estudante desejar estudar no colégio?", speed_factor=1.0)

    server = ThreadingHTTPServer((args.host, args.port), A6HTTPRequestHandler)
    log.info("=" * 72)
    log.info(" SERVIDOR WEB A6 INICIADO COM SUCESSO!")
    log.info(" -> Acesse no navegador: http://localhost:%d", args.port)
    log.info(" -> Broker Kafka configurado: %s", get_bootstrap_servers())
    log.info("=" * 72)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Encerrando servidor A6...")
        server.server_close()


if __name__ == "__main__":
    main()

