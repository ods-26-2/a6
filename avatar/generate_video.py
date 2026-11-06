#!/usr/bin/env python3
"""
generate_video.py - Le a(s) trajetoria(s) da pasta `entry/` e gera um arquivo .glb animado do avatar.

Fluxo da animacao:
    1. Avatar inicia na posicao NORMAL de repouso (bracos abaixados ao lado do corpo, maos relaxadas);
    2. Faz a transicao suave para a primeira pose do sinal de Libras;
    3. Executa a trajetoria completa dos keypoints (com suavizacao temporal);
    4. Retorna suavemente para a posicao NORMAL com as maos para baixo;
    5. Permanece parado na posicao de repouso ao finalizar (nao fica em loop infinito);
    6. Suporta dar REPLAY na ultima trajetoria via flag `--replay`.

Estrutura:
    avatar/
    ├── entry/            <- coloque aqui o JSON da trajetoria (qualquer nome)
    │   └── _processados/ <- arquivos ja convertidos sao guardados aqui
    ├── saida/            <- os .glb animados saem aqui (mesmo nome do JSON + aluno_corrigido.glb)
    ├── ybot.fbx
    └── generate_video.py

Uso no ambiente Blender (precisa do bpy):
    python avatar/generate_video.py
    python avatar/generate_video.py --replay              # re-executa a ultima trajetoria lida
    python avatar/generate_video.py --replay --view       # re-executa e abre no viewer 3D
    python avatar/generate_video.py --arquivo traj.json   # processa apenas este arquivo
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import bpy  # noqa: E402
from mathutils import Quaternion, Vector  # noqa: E402

try:
    import avatar_keypoints as la  # noqa: E402
except ModuleNotFoundError:
    import libras_avatar as la  # noqa: E402

import trajectory_format as tf  # noqa: E402


# --------------------------------------------------------------------------
# Utilitarios de Poses e Interpolacao Quaternion
# --------------------------------------------------------------------------
def snapshot_pose(rig: la.Rig) -> Dict[str, Quaternion]:
    """Captura o quaternion de rotacao atual de cada osso do rig."""
    return {pb.name: pb.rotation_quaternion.copy() for pb in rig.arm.pose.bones}


def apply_pose_quaternions(rig: la.Rig, pose_dict: Dict[str, Quaternion]):
    """Aplica quaternions a cada osso do rig."""
    for pb in rig.arm.pose.bones:
        if pb.name in pose_dict:
            pb.rotation_quaternion = pose_dict[pb.name].copy()
    bpy.context.view_layer.update()


def slerp_pose(pose_a: Dict[str, Quaternion], pose_b: Dict[str, Quaternion], t: float) -> Dict[str, Quaternion]:
    """Interpola duas poses com smoothstep e slerp esferico."""
    s = t * t * (3.0 - 2.0 * t)  # smoothstep
    out: Dict[str, Quaternion] = {}
    for name, qa in pose_a.items():
        qb = pose_b.get(name, qa)
        out[name] = qa.slerp(qb, s)
    return out


# --------------------------------------------------------------------------
# Gerenciamento de Arquivos de Entrada e Replay
# --------------------------------------------------------------------------
def obter_ultima_processada(entry_dir: Path) -> Optional[Path]:
    """Retorna o JSON processado mais recente para permitir replay."""
    pastas = [entry_dir / "_processados", entry_dir]
    candidatos: List[Path] = []
    for pasta in pastas:
        if pasta.exists():
            candidatos.extend(p for p in pasta.glob("*.json") if p.is_file())
    if not candidatos:
        return None
    candidatos.sort(key=lambda p: (p.stat().st_mtime, p.name), reverse=True)
    return candidatos[0]


def listar_entradas(entry_dir: Path, so_arquivo: Optional[str] = None, replay: bool = False) -> List[Path]:
    """Retorna a lista de JSONs para processar."""
    if so_arquivo:
        p = Path(so_arquivo)
        if not p.is_absolute():
            cand1 = entry_dir / p.name
            cand2 = entry_dir / "_processados" / p.name
            p = cand1 if cand1.is_file() else (cand2 if cand2.is_file() else p)
        return [p] if p.is_file() else []

    if replay:
        ult = obter_ultima_processada(entry_dir)
        if ult:
            print(f"Modo REPLAY: re-processando ultima trajetoria '{ult.name}'", flush=True)
            return [ult]
        print(f"Modo REPLAY: nenhum JSON anterior encontrado em {entry_dir}", flush=True)
        return []

    # Processa da pasta entry/ (do mais antigo para o mais novo)
    pendentes = sorted((p for p in entry_dir.glob("*.json") if p.is_file()),
                       key=lambda p: (p.stat().st_mtime, p.name))
    return pendentes


def mover_processado(json_path: Path, entry_dir: Path):
    """Move o JSON para _processados para nao re-renderizar na proxima execucao."""
    destino = entry_dir / "_processados"
    destino.mkdir(exist_ok=True)
    if json_path.parent == destino:
        return  # Ja esta na pasta de processados (ex: modo replay)
    alvo = destino / json_path.name
    if alvo.exists():
        alvo = destino / f"{json_path.stem}_{int(time.time())}{json_path.suffix}"
    shutil.move(str(json_path), str(alvo))


# --------------------------------------------------------------------------
# Animacao e Exportacao GLB
# --------------------------------------------------------------------------
def resetar_animacao(arm: Any):
    """Limpa dados de animacao anteriores para evitar sobreposicao de trilhas."""
    if arm.animation_data:
        arm.animation_data_clear()


def exportar_animacao_glb(
    json_path: Path,
    a: argparse.Namespace,
    rig: la.Rig,
    scene: bpy.types.Scene,
    saida_dir: Path,
) -> Optional[Path]:
    """Gera o arquivo .glb animado para o arquivo de trajetoria."""
    doc = json.loads(json_path.read_text(encoding="utf-8-sig"))
    raw_frames = tf.build_frames(
        doc,
        mirror=a.mirror,
        z_body=a.z_body,
        z_hand=a.z_hand,
        vis_min=a.vis_min,
        min_run=a.min_run,
        max_gap=a.max_gap,
    )

    if not raw_frames:
        print(f"ERRO: nenhum frame valido em {json_path.name}", flush=True)
        return None

    aviso_escala = tf.verificar_escala(doc)
    if aviso_escala:
        print(f"  AVISO: {aviso_escala}", flush=True)

    fps = a.fps if a.fps > 0 else float(doc.get("fps") or 25.0)
    fps_int = max(1, int(round(fps)))
    scene.render.fps = fps_int

    print(f"{json_path.name}: {tf.resumo(raw_frames)} | {fps_int} fps (~{len(raw_frames) / fps_int:.1f} s de sinal)", flush=True)

    # Limpa animacoes anteriores no rig
    resetar_animacao(rig.arm)

    # 1. Definir Pose de Repouso (bracos abaixados, maos relaxadas para baixo)
    la.apply_frame(rig, {"hands": {}, "pose": {}})
    rest_pose = snapshot_pose(rig)

    # 2. Capturar poses de inicio e fim do sinal
    la.apply_frame(rig, raw_frames[0])
    sign_start_pose = snapshot_pose(rig)

    la.apply_frame(rig, raw_frames[-1])
    sign_end_pose = snapshot_pose(rig)

    current_frame = 1

    # ---- Fase 1: Inicio na posicao normal com as maos para baixo ----
    hold_start = max(1, a.hold_start)
    for _ in range(hold_start):
        scene.frame_set(current_frame)
        apply_pose_quaternions(rig, rest_pose)
        rig.keyframe(current_frame)
        current_frame += 1

    # ---- Fase 2: Transicao suave de repouso -> inicio do sinal ----
    trans_in = max(1, a.trans_in)
    for step in range(1, trans_in + 1):
        t = step / (trans_in + 1)
        pose_t = slerp_pose(rest_pose, sign_start_pose, t)
        scene.frame_set(current_frame)
        apply_pose_quaternions(rig, pose_t)
        rig.keyframe(current_frame)
        current_frame += 1

    # ---- Fase 3: Execucao do sinal de Libras (quadros da trajetoria) ----
    prev = None
    passo = max(1, len(raw_frames) // 10)
    for idx, f in enumerate(raw_frames, start=1):
        f = la.smooth(prev, f, a.smooth)
        prev = f
        scene.frame_set(current_frame)
        la.apply_frame(rig, f)
        rig.keyframe(current_frame)
        current_frame += 1
        if idx % passo == 0 or idx == len(raw_frames):
            print(f"  processando sinal {idx}/{len(raw_frames)}", flush=True)

    # ---- Fase 4: Transicao suave de volta para a posicao normal (maos para baixo) ----
    trans_out = max(1, a.trans_out)
    for step in range(1, trans_out + 1):
        t = step / (trans_out + 1)
        pose_t = slerp_pose(sign_end_pose, rest_pose, t)
        scene.frame_set(current_frame)
        apply_pose_quaternions(rig, pose_t)
        rig.keyframe(current_frame)
        current_frame += 1

    # ---- Fase 5: Segura parado na posicao normal (para nao ficar em loop infinito) ----
    hold_end = max(1, a.hold_end)
    for _ in range(hold_end):
        scene.frame_set(current_frame)
        apply_pose_quaternions(rig, rest_pose)
        rig.keyframe(current_frame)
        current_frame += 1

    total_anim_frames = current_frame - 1
    scene.frame_start = 1
    scene.frame_end = total_anim_frames

    # Exporta para .glb
    saida_glb = saida_dir / f"{json_path.stem}.glb"
    t0 = time.time()
    bpy.ops.export_scene.gltf(
        filepath=str(saida_glb),
        export_format="GLB",
        export_animations=True,
    )
    dt = time.time() - t0

    # Atualiza copias padrao usadas pelo servidor web e visualizadores
    copias_padrao = [
        saida_dir / "aluno_corrigido.glb",
        saida_dir / "ultima_trajetoria.glb",
    ]
    for cp in copias_padrao:
        try:
            if saida_glb.resolve() != cp.resolve():
                shutil.copy2(saida_glb, cp)
        except Exception:
            pass

    print(f"GLB exportado com sucesso em {dt:.1f}s ({total_anim_frames} frames): {saida_glb.name}", flush=True)
    return saida_glb


def abrir_viewer(glb_path: Path):
    """Abre o viewer_avatar.py (Panda3D) em processo filho para inspecionar o GLB."""
    viewer_py = HERE / "viewer.py"
    if not viewer_py.is_file():
        return
    py_generic = HERE.parent / ".venv" / "standard" / "bin" / "python"
    cmd = [str(py_generic if py_generic.is_file() else sys.executable), str(viewer_py), str(glb_path)]
    print(f"Abrindo visualizador 3D: {' '.join(cmd)}", flush=True)
    try:
        subprocess.Popen(cmd)
    except Exception as exc:
        print(f"Nao foi possivel abrir viewer: {exc}", flush=True)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--entry", default=str(HERE / "entry"), help="pasta com o(s) JSON(s) de trajetoria")
    ap.add_argument("--saida", default=str(HERE / "saida"), help="pasta de saida dos arquivos .glb")
    ap.add_argument("--model", default=str(HERE / "ybot.fbx"), help="avatar .fbx / .glb (rig Mixamo)")
    ap.add_argument("--arquivo", help="processa apenas este arquivo .json")
    ap.add_argument("--replay", action="store_true", help="re-processa a ultima trajetoria lida em _processados/")
    ap.add_argument("--keep-json", action="store_true", help="nao move o JSON para _processados/")
    ap.add_argument("--view", action="store_true", help="abre o avatar interativo apos exportar")
    ap.add_argument("--fps", type=float, default=0, help="taxa de quadros (0 = usa do proprio JSON)")
    ap.add_argument("--mirror", action=argparse.BooleanOptionalAction, default=la.MIRROR_PADRAO,
                    help="espelha a pose (padrao: MIRROR_PADRAO)")
    ap.add_argument("--smooth", type=float, default=0.6, help="suavizacao temporal dos keypoints (0..1)")
    ap.add_argument("--hold-start", type=int, default=5, help="frames segurando em repouso no inicio")
    ap.add_argument("--trans-in", type=int, default=10, help="frames de transicao repouso -> sinal")
    ap.add_argument("--trans-out", type=int, default=12, help="frames de transicao sinal -> repouso")
    ap.add_argument("--hold-end", type=int, default=15, help="frames segurando em repouso no final")
    ap.add_argument("--z-body", type=float, default=tf.Z_BODY, help="atenuacao do z do corpo")
    ap.add_argument("--z-hand", type=float, default=tf.Z_HAND, help="escala do z das maos")
    ap.add_argument("--vis-min", type=float, default=tf.VIS_MIN, help="visibilidade minima para o braco")
    ap.add_argument("--min-run", type=int, default=tf.MIN_RUN, help="mao presente por menos frames = ruido")
    ap.add_argument("--max-gap", type=int, default=tf.MAX_GAP, help="preenche buracos de ate N frames")
    a = ap.parse_args()

    entry_dir, saida_dir = Path(a.entry), Path(a.saida)
    entry_dir.mkdir(parents=True, exist_ok=True)
    saida_dir.mkdir(parents=True, exist_ok=True)

    arquivos = listar_entradas(entry_dir, a.arquivo, replay=a.replay)
    if not arquivos:
        print(f"Nenhum .json encontrado em {entry_dir} (use --replay para repetir a ultima)", flush=True)
        sys.exit(0)

    # Carrega modelo e prepara cena no Blender
    bpy.ops.wm.read_factory_settings(use_empty=True)
    arm = la.load_model(a.model)
    scene = bpy.context.scene
    la.setup_scene(scene, 720)
    rig = la.Rig(arm)
    la.face_front(rig)

    falhas = 0
    ultimo_gerado: Optional[Path] = None

    for json_path in arquivos:
        try:
            glb = exportar_animacao_glb(json_path, a, rig, scene, saida_dir)
        except (json.JSONDecodeError, ValueError, KeyError, IndexError) as exc:
            print(f"ERRO: {json_path.name} invalido ({exc})", flush=True)
            falhas += 1
            continue

        if glb:
            ultimo_gerado = glb
            # Imprime tanto GLB quanto VIDEO para total compatibilidade com servico_avatar.py
            print(f"GLB: {glb.resolve()}", flush=True)
            print(f"VIDEO: {glb.resolve()}", flush=True)
            if not a.keep_json and not a.replay:
                mover_processado(json_path, entry_dir)
        else:
            falhas += 1

    if ultimo_gerado and a.view:
        abrir_viewer(ultimo_gerado)

    sys.exit(1 if falhas else 0)


if __name__ == "__main__":
    main()
