#!/usr/bin/env python3
"""
gerar_video.py - Le uma pasta com varios sinais e gera UM video .mp4 do avatar.

Estrutura esperada:
    Geral/
    ├── Sinal 1/
    │   ├── Keypoint_1.json
    │   └── Keypoint_2.json
    ├── Sinal 2/
    │   └── ...
    └── ...

As subpastas e os JSONs sao lidos em ORDEM NATURAL (Sinal 2 vem antes de Sinal 10,
Keypoint_2 antes de Keypoint_10). Entre um sinal e outro, o script cria uma transicao
suave (--transicao) para a mao nao "pular".

Usar no ambiente do Blender (precisa do bpy), com libras_avatar.py na mesma pasta:

    python gerar_video.py Geral/ --model ybot.fbx -o frase.mp4

Requer ffmpeg: `sudo pacman -S ffmpeg`  ou  `pip install imageio-ffmpeg`.
"""
import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bpy  # noqa: E402
import avatar_keypoints as la  # noqa: E402


# --------------------------------------------------------------------------
# Leitura da estrutura de pastas
# --------------------------------------------------------------------------
def natural_key(path):
    """Ordem natural: 'Sinal 2' < 'Sinal 10'."""
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", path.name)]


def descobrir_sinais(raiz):
    """Retorna [(nome_do_sinal, [json, json, ...]), ...] em ordem."""
    sinais = []
    subpastas = sorted((d for d in raiz.iterdir() if d.is_dir() and not d.name.startswith(".")),
                       key=natural_key)
    for d in subpastas:
        jsons = sorted(d.glob("*.json"), key=natural_key)
        if jsons:
            sinais.append((d.name, jsons))
        else:
            print(f"AVISO: pasta '{d.name}' nao tem .json, ignorada.")
    if not sinais:  # sem subpastas: trata a propria pasta como um unico sinal
        jsons = sorted(raiz.glob("*.json"), key=natural_key)
        if jsons:
            sinais.append((raiz.name, jsons))
    return sinais


# --------------------------------------------------------------------------
# Transicao entre sinais
# --------------------------------------------------------------------------
def lerp_frame(a, b, t):
    """Interpola dois frames (dicts com 'hands' e 'pose') com suavizacao (smoothstep)."""
    t = t * t * (3 - 2 * t)
    out = {"hands": {}, "pose": {}}
    for side in set(a["hands"]) | set(b["hands"]):
        pa, pb = a["hands"].get(side), b["hands"].get(side)
        if pa and pb:
            out["hands"][side] = {i: pa[i].lerp(pb[i], t) for i in pa if i in pb}
        else:  # mao so existe de um lado: mantem ate a metade e troca
            src = pa if t < 0.5 else pb
            if src:
                out["hands"][side] = src
    pa, pb = a["pose"], b["pose"]
    if pa and pb:
        out["pose"] = {i: pa[i].lerp(pb[i], t) for i in pa if i in pb}
    else:
        out["pose"] = (pa if t < 0.5 else pb) or {}
    return out


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
        print("\nERRO: ffmpeg nao encontrado. Instale com `sudo pacman -S ffmpeg` "
              "ou `pip install imageio-ffmpeg`.")
        return False
    cmd = [ffmpeg, "-y", "-loglevel", "error", "-framerate", str(fps),
           "-i", str(frames_dir / "%06d.png"),
           "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "18", str(saida)]
    return subprocess.run(cmd).returncode == 0


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pasta", help="pasta geral com uma subpasta por sinal")
    ap.add_argument("--model", required=True, help="avatar .fbx / .glb (rig Mixamo)")
    ap.add_argument("-o", "--saida", help="video de saida (padrao: <nome_da_pasta>.mp4)")
    ap.add_argument("--fps", type=int, default=30)
    ap.add_argument("--transicao", type=int, default=8,
                    help="frames de transicao entre sinais (0 = corte seco)")
    ap.add_argument("--res", type=int, default=720, help="resolucao do video (quadrado)")
    ap.add_argument("--width", type=int, default=1280, help="largura da imagem original dos keypoints")
    ap.add_argument("--height", type=int, default=720, help="altura da imagem original dos keypoints")
    ap.add_argument("--keep-labels", action="store_true", help="NAO inverter Left/Right das maos")
    ap.add_argument("--mirror", action=argparse.BooleanOptionalAction, default=la.MIRROR_PADRAO,
                    help="espelhar a pose (camera selfie); o padrao vem de MIRROR_PADRAO em "
                         "libras_avatar.py. Use --no-mirror para desligar")
    ap.add_argument("--smooth", type=float, default=0.6, help="0-1 (1 = sem suavizacao)")
    ap.add_argument("--focus", choices=["auto", "full", "hands"], default="auto")
    ap.add_argument("--margin", type=float, default=1.0, help=">1 afasta, <1 aproxima")
    ap.add_argument("--keep-frames", metavar="DIR", help="salva os PNGs nesta pasta (nao apaga)")
    a = ap.parse_args()

    raiz = Path(a.pasta)
    if not raiz.is_dir():
        sys.exit(f"Pasta nao encontrada: {raiz}")
    sinais = descobrir_sinais(raiz)
    if not sinais:
        sys.exit("Nenhum sinal (.json) encontrado.")

    # ---- 1. carregar todos os keypoints e montar a sequencia -----------------
    sequencia = []   # lista de frames (dicts)
    for nome, arquivos in sinais:
        frames = []
        for f in arquivos:
            try:
                frames.append(la.load_frame(f, a.width, a.height,
                                            swap_hand_labels=not a.keep_labels, mirror=a.mirror))
            except (json.JSONDecodeError, KeyError) as e:
                print(f"AVISO: {f.name} invalido ({e}), ignorado.")
        if not frames:
            continue
        if sequencia and a.transicao > 0:
            ultimo = sequencia[-1]
            for j in range(1, a.transicao + 1):
                sequencia.append(lerp_frame(ultimo, frames[0], j / (a.transicao + 1)))
        sequencia.extend(frames)
        print(f"  {nome}: {len(frames)} frames")

    total = len(sequencia)
    print(f"{len(sinais)} sinais, {total} frames, ~{total / a.fps:.1f} s a {a.fps} fps")

    # ---- 2. preparar avatar e cena -------------------------------------------
    res = a.res + (a.res % 2)    # largura/altura pares (exigencia do yuv420p)
    arm = la.load_model(a.model)
    scene = bpy.context.scene
    scene.render.fps = a.fps
    la.setup_scene(scene, res)
    rig = la.Rig(arm)
    esperados = ["LeftArm", "LeftHand", "LeftHandIndex1", "RightArm", "RightHand", "RightHandIndex1"]
    faltando = [n for n in esperados if rig.bone(n) is None]
    if faltando:
        print("AVISO: ossos nao encontrados:", faltando)
    la.face_front(rig)

    # ---- 3. renderizar frame a frame -----------------------------------------
    if a.keep_frames:
        frames_dir = Path(a.keep_frames)
        frames_dir.mkdir(parents=True, exist_ok=True)
    else:
        frames_dir = Path(tempfile.mkdtemp(prefix="libras_frames_"))

    prev = None
    for n, frame in enumerate(sequencia, start=1):
        frame = la.smooth(prev, frame, a.smooth)
        prev = frame
        la.apply_frame(rig, frame)
        if n == 1:
            la.frame_camera(scene, rig, a.focus, a.margin)
        scene.render.filepath = str(frames_dir / f"{n:06d}.png")
        bpy.ops.render.render(write_still=True)
        print(f"\rRenderizando {n}/{total}", end="", flush=True)
    print()

    # ---- 4. montar o mp4 -----------------------------------------------------
    saida = Path(a.saida) if a.saida else Path(f"{raiz.name}.mp4")
    ok = montar_video(frames_dir, saida, a.fps)
    if ok:
        print("Video salvo em", saida)
        if not a.keep_frames:
            shutil.rmtree(frames_dir, ignore_errors=True)
    else:
        print(f"Os frames ficaram em {frames_dir} (monte o video manualmente com o ffmpeg).")


if __name__ == "__main__":
    main()