#!/usr/bin/env python3
"""
generate_video.py - Le a(s) trajetoria(s) da pasta `entry/` e gera um .mp4 do avatar por arquivo.

O NOME do arquivo nao importa: tudo que for .json dentro de `entry/` e processado
(do mais antigo para o mais novo). Depois de gerar o video, o JSON e movido para
`entry/_processados/` para nao ser renderizado de novo.

Estrutura:
    avatar/
    ├── entry/            <- coloque aqui o JSON da trajetoria (qualquer nome)
    ├── saida/            <- os .mp4 saem aqui (mesmo nome do JSON)
    ├── ybot.fbx
    └── generate_video.py

Usar no ambiente do Blender (precisa do bpy):
    python avatar/generate_video.py
    python avatar/generate_video.py --entry outra_pasta --saida videos --model avatar/ybot.fbx

Requer ffmpeg (`sudo pacman -S ffmpeg` ou `pip install imageio-ffmpeg`).
"""
import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import bpy  # noqa: E402

try:
    import avatar_keypoints as la  # noqa: E402
except ModuleNotFoundError:        # nome antigo do mesmo modulo
    import libras_avatar as la     # noqa: E402

import trajectory_format as tf  # noqa: E402


# --------------------------------------------------------------------------
# ffmpeg
# --------------------------------------------------------------------------
def achar_ffmpeg():
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except ImportError:
        return None


def montar_video(frames_dir, saida, fps):
    ffmpeg = achar_ffmpeg()
    if not ffmpeg:
        print("ERRO: ffmpeg nao encontrado. Instale com `sudo pacman -S ffmpeg` "
              "ou `pip install imageio-ffmpeg`.", flush=True)
        return False
    cmd = [ffmpeg, "-y", "-loglevel", "error", "-framerate", str(fps),
           "-i", str(frames_dir / "%06d.png"),
           "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "18", str(saida)]
    return subprocess.run(cmd).returncode == 0


# --------------------------------------------------------------------------
# Pasta entry/
# --------------------------------------------------------------------------
def listar_entradas(entry_dir, so_arquivo=None):
    """JSONs da pasta (nao entra em subpastas), do mais antigo para o mais novo."""
    if so_arquivo:
        p = Path(so_arquivo)
        if not p.is_absolute():
            p = entry_dir / p.name
        return [p] if p.is_file() else []
    return sorted((p for p in entry_dir.glob("*.json") if p.is_file()),
                  key=lambda p: (p.stat().st_mtime, p.name))


def mover_processado(json_path, entry_dir):
    destino = entry_dir / "_processados"
    destino.mkdir(exist_ok=True)
    alvo = destino / json_path.name
    if alvo.exists():
        alvo = destino / f"{json_path.stem}_{int(time.time())}{json_path.suffix}"
    shutil.move(str(json_path), str(alvo))


# --------------------------------------------------------------------------
def renderizar_arquivo(json_path, a, rig, scene, saida_dir):
    """Gera o mp4 de UM arquivo de trajetoria. Retorna o caminho do video ou None."""
    doc = json.loads(json_path.read_text(encoding="utf-8-sig"))
    frames = tf.build_frames(
        doc, mirror=a.mirror, z_body=a.z_body, z_hand=a.z_hand,
        vis_min=a.vis_min, min_run=a.min_run, max_gap=a.max_gap,
    )
    aviso_escala = tf.verificar_escala(doc)
    if aviso_escala:
        print(f"  AVISO: {aviso_escala}", flush=True)
    if not any(f["hands"] for f in frames):
        print("  AVISO: nenhuma mao detectada nesta trajetoria - o avatar ficara parado.", flush=True)
    fps = a.fps if a.fps > 0 else float(doc.get("fps") or 25.0)
    scene.render.fps = max(1, int(round(fps)))
    total = len(frames)
    print(f"{json_path.name}: {tf.resumo(frames)} | {fps:g} fps (~{total / fps:.1f} s)", flush=True)

    # Enquadramento: usa o primeiro frame em que ha mao (onde o sinal realmente acontece)
    cam_i = next((i for i, f in enumerate(frames) if f["hands"]), 0)
    la.apply_frame(rig, frames[cam_i])
    la.frame_camera(scene, rig, a.focus, a.margin)

    if a.keep_frames:
        frames_dir = Path(a.keep_frames) / json_path.stem
        frames_dir.mkdir(parents=True, exist_ok=True)
    else:
        frames_dir = Path(tempfile.mkdtemp(prefix="libras_frames_"))

    prev = None
    passo = max(1, total // 10)
    for n, frame in enumerate(frames, start=1):
        frame = la.smooth(prev, frame, a.smooth)
        prev = frame
        la.apply_frame(rig, frame)
        scene.render.filepath = str(frames_dir / f"{n:06d}.png")
        bpy.ops.render.render(write_still=True)
        if n % passo == 0 or n == total:
            print(f"  renderizando {n}/{total}", flush=True)

    saida = saida_dir / f"{json_path.stem}.mp4"
    ok = montar_video(frames_dir, saida, fps)
    if ok and not a.keep_frames:
        shutil.rmtree(frames_dir, ignore_errors=True)
    elif not ok:
        print(f"  frames mantidos em {frames_dir}", flush=True)
    return saida if ok else None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--entry", default=str(HERE / "entry"), help="pasta com o(s) JSON(s) de trajetoria")
    ap.add_argument("--saida", default=str(HERE / "saida"), help="pasta de saida dos .mp4")
    ap.add_argument("--model", default=str(HERE / "ybot.fbx"), help="avatar .fbx / .glb (rig Mixamo)")
    ap.add_argument("--arquivo", help="processa so este arquivo (nome dentro de entry/)")
    ap.add_argument("--keep-json", action="store_true", help="nao move o JSON para _processados/")
    ap.add_argument("--fps", type=float, default=0, help="0 = usa o fps do proprio JSON")
    ap.add_argument("--res", type=int, default=720, help="resolucao do video (quadrado)")
    ap.add_argument("--mirror", action=argparse.BooleanOptionalAction, default=la.MIRROR_PADRAO,
                    help="espelha a pose (padrao: MIRROR_PADRAO em avatar_keypoints.py)")
    ap.add_argument("--smooth", type=float, default=0.6, help="0-1 (1 = sem suavizacao)")
    ap.add_argument("--focus", choices=["auto", "full", "hands"], default="auto")
    ap.add_argument("--margin", type=float, default=1.0, help=">1 afasta, <1 aproxima")
    ap.add_argument("--z-body", type=float, default=tf.Z_BODY, help="atenuacao do z do corpo")
    ap.add_argument("--z-hand", type=float, default=tf.Z_HAND, help="escala do z das maos")
    ap.add_argument("--vis-min", type=float, default=tf.VIS_MIN, help="visibilidade minima p/ guiar o braco")
    ap.add_argument("--min-run", type=int, default=tf.MIN_RUN, help="mao por menos frames = ruido")
    ap.add_argument("--max-gap", type=int, default=tf.MAX_GAP, help="preenche buracos de ate N frames")
    ap.add_argument("--keep-frames", metavar="DIR", help="guarda os PNGs nesta pasta")
    a = ap.parse_args()

    entry_dir, saida_dir = Path(a.entry), Path(a.saida)
    entry_dir.mkdir(parents=True, exist_ok=True)
    saida_dir.mkdir(parents=True, exist_ok=True)

    arquivos = listar_entradas(entry_dir, a.arquivo)
    if not arquivos:
        print(f"Nenhum .json encontrado em {entry_dir}", flush=True)
        sys.exit(0)

    res = a.res + (a.res % 2)   # yuv420p exige largura/altura pares
    arm = la.load_model(a.model)
    scene = bpy.context.scene
    la.setup_scene(scene, res)
    rig = la.Rig(arm)
    faltando = [n for n in ("LeftArm", "LeftHand", "LeftHandIndex1", "RightArm", "RightHand", "RightHandIndex1")
                if rig.bone(n) is None]
    if faltando:
        print("AVISO: ossos nao encontrados:", faltando, flush=True)
    la.face_front(rig)

    falhas = 0
    for json_path in arquivos:
        try:
            video = renderizar_arquivo(json_path, a, rig, scene, saida_dir)
        except (json.JSONDecodeError, ValueError, KeyError, IndexError) as e:
            print(f"ERRO: {json_path.name} invalido ({e})", flush=True)
            falhas += 1
            continue
        if video:
            print(f"VIDEO: {video.resolve()}", flush=True)
            if not a.keep_json:
                mover_processado(json_path, entry_dir)
        else:
            falhas += 1
    sys.exit(1 if falhas else 0)


if __name__ == "__main__":
    main()