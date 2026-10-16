#!/usr/bin/env python3
"""
libras_avatar.py - Anima um avatar 3D (rig Mixamo) a partir de keypoints MediaPipe.

Instalacao:
    pip install bpy        # exige a versao de Python que o pacote pede (geralmente 3.11)

Exemplos:
    # um frame -> PNG
    python libras_avatar.py d01_20261007_111447.json --model ybot.fbx --render-dir saida

    # pasta com varios JSONs (sequencia) -> PNGs + animacao .glb
    python libras_avatar.py pasta_jsons/ --model ybot.fbx --render-dir saida --export-glb sinal.glb

Formato de entrada (um JSON por frame):
    "maos":  [{"lado": "Right"|"Left", "keypoints": [{"id","x","y","z"} x21]}]   (MediaPipe Hands)
    "corpo": [{"id","x","y","z"} x33]    (OPCIONAL - MediaPipe Pose, formato assumido)
    "rosto": [...]                       (OPCIONAL - ainda nao usado, ver apply_face)
"""
import argparse
import json
import sys
from pathlib import Path

import bpy
from mathutils import Vector

# --------------------------------------------------------------------------
# Mapeamento landmark MediaPipe -> osso (nomes Mixamo, sem o prefixo "mixamorig:")
# Cada osso "aponta" do landmark A para o landmark B.
# --------------------------------------------------------------------------
FINGER_BONES = {
    "Thumb1": (1, 2), "Thumb2": (2, 3), "Thumb3": (3, 4),
    "Index1": (5, 6), "Index2": (6, 7), "Index3": (7, 8),
    "Middle1": (9, 10), "Middle2": (10, 11), "Middle3": (11, 12),
    "Ring1": (13, 14), "Ring2": (14, 15), "Ring3": (15, 16),
    "Pinky1": (17, 18), "Pinky2": (18, 19), "Pinky3": (19, 20),
}

# MediaPipe Pose: (ombro, cotovelo, punho) por lado anatomico
POSE_ARM = {"Left": (11, 13, 15), "Right": (12, 14, 16)}


# --------------------------------------------------------------------------
# Entrada de dados
# --------------------------------------------------------------------------
def to_world(kp, w, h):
    """MediaPipe (x dir, y baixo, z profundidade; normalizado) -> mundo Blender
    (X dir, Y para longe da camera, Z cima). Avatar olha para -Y (para o espectador)."""
    return Vector((kp["x"] * w, kp["z"] * w, -kp["y"] * h))


def load_frame(path, w, h, swap_hand_labels):
    d = json.loads(Path(path).read_text(encoding="utf-8"))
    hands = {}
    for m in d.get("maos", []):
        label = m["lado"]
        if swap_hand_labels:  # MediaPipe assume imagem espelhada
            label = "Left" if label == "Right" else "Right"
        hands[label] = {k["id"]: to_world(k, w, h) for k in m["keypoints"]}
    pose = {k["id"]: to_world(k, w, h) for k in d.get("corpo", [])}
    return {"hands": hands, "pose": pose}


def smooth(prev, cur, alpha):
    """Media movel exponencial simples (alpha=1 -> sem suavizacao)."""
    if prev is None or alpha >= 1:
        return cur
    out = {"hands": {}, "pose": {}}
    for side, pts in cur["hands"].items():
        old = prev["hands"].get(side)
        out["hands"][side] = {i: (p * alpha + old[i] * (1 - alpha)) if old else p
                              for i, p in pts.items()}
    for i, p in cur["pose"].items():
        old = prev["pose"].get(i)
        out["pose"][i] = p * alpha + old * (1 - alpha) if old else p
    return out


# --------------------------------------------------------------------------
# Cena / modelo
# --------------------------------------------------------------------------
def load_model(path):
    bpy.ops.wm.read_factory_settings(use_empty=True)
    ext = Path(path).suffix.lower()
    if ext == ".fbx":
        bpy.ops.import_scene.fbx(filepath=str(path))
    elif ext in (".glb", ".gltf"):
        bpy.ops.import_scene.gltf(filepath=str(path))
    else:
        sys.exit(f"Formato nao suportado: {ext} (use .fbx, .glb ou .gltf)")
    arms = [o for o in bpy.context.scene.objects if o.type == "ARMATURE"]
    if not arms:
        sys.exit("O modelo nao tem armature (esqueleto).")
    return arms[0]


def setup_camera_and_render(scene, res, cam_dist, cam_height, look_height):
    cam_data = bpy.data.cameras.new("cam")
    cam = bpy.data.objects.new("cam", cam_data)
    scene.collection.objects.link(cam)
    cam.location = Vector((0, -cam_dist, cam_height))
    direction = Vector((0, 0, look_height)) - cam.location
    cam.rotation_euler = direction.to_track_quat("-Z", "Y").to_euler()
    scene.camera = cam

    scene.render.engine = "BLENDER_WORKBENCH"        # roda sem GPU / headless
    scene.display.shading.light = "STUDIO"
    scene.display.shading.color_type = "MATERIAL"
    scene.render.resolution_x = scene.render.resolution_y = res
    world = bpy.data.worlds.new("w")
    world.color = (0.85, 0.88, 0.95)
    scene.world = world


class Rig:
    def __init__(self, arm):
        self.arm = arm
        self.names = [b.name for b in arm.pose.bones]
        for pb in arm.pose.bones:
            pb.rotation_mode = "QUATERNION"

    def bone(self, suffix):
        """Acha o osso ignorando prefixo (mixamorig:, mixamorig9:, mixamorig...)."""
        c = [n for n in self.names if n.endswith(suffix)]
        return self.arm.pose.bones[min(c, key=len)] if c else None

    def reset(self):
        for pb in self.arm.pose.bones:
            pb.rotation_quaternion = (1, 0, 0, 0)
            pb.location = (0, 0, 0)
            pb.scale = (1, 1, 1)
        bpy.context.view_layer.update()

    def _to_arm_space(self, v_world):
        return self.arm.matrix_world.inverted().to_3x3() @ v_world

    def aim(self, bone_suffix, direction_world):
        """Gira o osso (head->tail) para apontar na direcao pedida."""
        pb = self.bone(bone_suffix)
        if pb is None or direction_world.length < 1e-9:
            return None
        target = self._to_arm_space(direction_world).normalized()
        current = (pb.tail - pb.head).normalized()
        rot = current.rotation_difference(target).to_matrix().to_4x4()
        head = pb.head.copy()
        from mathutils import Matrix
        pb.matrix = (Matrix.Translation(head) @ rot @ Matrix.Translation(-head)) @ pb.matrix
        bpy.context.view_layer.update()
        return pb

    def twist_hand(self, side, across_world):
        """Gira a mao em torno do proprio eixo para o 'indicador->mindinho' bater com o alvo."""
        hand = self.bone(f"{side}Hand")
        idx, pnk = self.bone(f"{side}HandIndex1"), self.bone(f"{side}HandPinky1")
        if not (hand and idx and pnk):
            return
        axis = (hand.tail - hand.head).normalized()
        cur = pnk.head - idx.head
        tgt = self._to_arm_space(across_world)
        cur_p = cur - axis * cur.dot(axis)
        tgt_p = tgt - axis * tgt.dot(axis)
        if cur_p.length < 1e-9 or tgt_p.length < 1e-9:
            return
        from mathutils import Matrix
        rot = cur_p.rotation_difference(tgt_p).to_matrix().to_4x4()
        head = hand.head.copy()
        hand.matrix = (Matrix.Translation(head) @ rot @ Matrix.Translation(-head)) @ hand.matrix
        bpy.context.view_layer.update()

    def keyframe(self, frame):
        for pb in self.arm.pose.bones:
            pb.keyframe_insert("rotation_quaternion", frame=frame)


# --------------------------------------------------------------------------
# Aplicacao da pose
# --------------------------------------------------------------------------
def default_stance(rig):
    """Bracos em posicao de 'sinalizar' quando nao ha keypoints de corpo."""
    for side, sx in (("Left", 1), ("Right", -1)):   # avatar: Left em +X
        rig.aim(f"{side}Arm", Vector((0.25 * sx, 0.0, -1.0)))
        rig.aim(f"{side}ForeArm", Vector((0.15 * sx, -1.0, 0.6)))


def apply_body(rig, pose):
    for side, (s, e, w) in POSE_ARM.items():
        if all(i in pose for i in (s, e, w)):
            rig.aim(f"{side}Arm", pose[e] - pose[s])
            rig.aim(f"{side}ForeArm", pose[w] - pose[e])


def apply_hand(rig, side, p):
    rig.aim(f"{side}Hand", p[9] - p[0])                 # palma aponta p/ base do dedo medio
    rig.twist_hand(side, p[17] - p[5])                  # orientacao da palma
    for name, (a, b) in FINGER_BONES.items():
        rig.aim(f"{side}Hand{name}", p[b] - p[a])


def apply_face(rig, face):
    """TODO: marcadores de rosto.
    - Rotacao da cabeca: aim em 'Neck'/'Head' usando nariz/orelhas/queixo.
    - Expressoes (gramaticais em Libras: sobrancelhas, boca, olhos): precisam de
      shape keys/blendshapes no modelo (ex.: ARKit 52). Ex.:
        obj.data.shape_keys.key_blocks['browInnerUp'].value = valor
    """
    return


def apply_frame(rig, frame):
    rig.reset()
    if frame["pose"]:
        apply_body(rig, frame["pose"])
    else:
        default_stance(rig)
    for side, pts in frame["hands"].items():
        if len(pts) >= 21:
            apply_hand(rig, side, pts)


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input", help="arquivo .json ou pasta com .json (sequencia)")
    ap.add_argument("--model", required=True, help="avatar .fbx / .glb / .gltf (rig Mixamo)")
    ap.add_argument("--width", type=int, default=1280, help="largura da imagem original (p/ proporcao)")
    ap.add_argument("--height", type=int, default=720, help="altura da imagem original")
    ap.add_argument("--keep-labels", action="store_true",
                    help="NAO inverter Left/Right das maos (use se a imagem original estava espelhada)")
    ap.add_argument("--smooth", type=float, default=0.6, help="0-1 (1 = sem suavizacao)")
    ap.add_argument("--render-dir", help="pasta para salvar PNGs")
    ap.add_argument("--export-glb", help="salva animacao em .glb")
    ap.add_argument("--fps", type=int, default=30)
    ap.add_argument("--res", type=int, default=720, help="resolucao do render (quadrado)")
    ap.add_argument("--cam-dist", type=float, default=2.0)
    ap.add_argument("--cam-height", type=float, default=1.45)
    ap.add_argument("--look-height", type=float, default=1.35)
    a = ap.parse_args()

    src = Path(a.input)
    files = sorted(src.glob("*.json")) if src.is_dir() else [src]
    if not files:
        sys.exit("Nenhum JSON encontrado.")

    arm = load_model(a.model)
    scene = bpy.context.scene
    scene.render.fps = a.fps
    setup_camera_and_render(scene, a.res, a.cam_dist, a.cam_height, a.look_height)
    rig = Rig(arm)

    out_dir = Path(a.render_dir) if a.render_dir else None
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)

    prev = None
    for i, f in enumerate(files, start=1):
        frame = load_frame(f, a.width, a.height, swap_hand_labels=not a.keep_labels)
        frame = smooth(prev, frame, a.smooth)
        prev = frame
        apply_frame(rig, frame)
        scene.frame_set(i)
        if a.export_glb:
            rig.keyframe(i)
        if out_dir:
            scene.render.filepath = str(out_dir / f"{f.stem}.png")
            bpy.ops.render.render(write_still=True)
        print(f"[{i}/{len(files)}] {f.name}")

    scene.frame_start, scene.frame_end = 1, len(files)
    if a.export_glb:
        bpy.ops.export_scene.gltf(filepath=a.export_glb, export_format="GLB", export_animations=True)
        print("Animacao salva em", a.export_glb)


if __name__ == "__main__":
    main()