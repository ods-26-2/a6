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
import math
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
def to_world(kp, w, h, mirror=False):
    """MediaPipe (x dir, y baixo, z profundidade; normalizado) -> mundo Blender
    (X dir, Y para longe da camera, Z cima). Avatar olha para -Y (para o espectador).
    mirror=True inverte o eixo X (espelha a pose)."""
    sx = -1.0 if mirror else 1.0
    return Vector((sx * kp["x"] * w, kp["z"] * w, -kp["y"] * h))


# Indices do MediaPipe Pose que trocam de lado ao espelhar
_POSE_SWAP = {11: 12, 12: 11, 13: 14, 14: 13, 15: 16, 16: 15}


def load_frame(path, w, h, swap_hand_labels, mirror=False):
    d = json.loads(Path(path).read_text(encoding="utf-8"))
    # espelhar inverte a anatomia: a mao esquerda vira direita e vice-versa
    do_swap = swap_hand_labels != mirror
    hands = {}
    for m in d.get("maos", []):
        label = m["lado"]
        if do_swap:  # MediaPipe assume imagem espelhada
            label = "Left" if label == "Right" else "Right"
        hands[label] = {k["id"]: to_world(k, w, h, mirror) for k in m["keypoints"]}
    pose = {}
    for k in d.get("corpo", []):
        idx = _POSE_SWAP.get(k["id"], k["id"]) if mirror else k["id"]
        pose[idx] = to_world(k, w, h, mirror)
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
    arm = arms[0]
    # O FBX do Mixamo costuma trazer uma "animacao" de 1 frame (a T-pose) que
    # sobrescreve a pose manual a cada frame_set/render. Removemos.
    arm.animation_data_clear()
    arm.data.pose_position = "POSE"
    return arm


def setup_scene(scene, res):
    cam_data = bpy.data.cameras.new("cam")
    cam = bpy.data.objects.new("cam", cam_data)
    scene.collection.objects.link(cam)
    scene.camera = cam   # posicionada depois por frame_camera()

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
def face_front(rig):
    """Gira o avatar em Z para ele olhar para -Y (de frente para a camera),
    independente de como o FBX/GLB foi importado."""
    L, R = rig.bone("LeftArm"), rig.bone("RightArm")
    if not (L and R):
        return
    mw = rig.arm.matrix_world
    fwd = (mw @ L.head - mw @ R.head).cross(Vector((0, 0, 1)))
    fwd.z = 0
    if fwd.length < 1e-9:
        return
    fwd.normalize()
    front = Vector((0, -1, 0))
    angle = fwd.angle(front)
    if fwd.cross(front).z < 0:
        angle = -angle
    from mathutils import Matrix
    rig.arm.matrix_world = Matrix.Rotation(angle, 4, "Z") @ rig.arm.matrix_world
    bpy.context.view_layer.update()


def frame_camera(scene, rig, focus, margin):
    """Posiciona a camera pelo tamanho real do avatar (nao depende de escala/unidade)."""
    pts = [o.matrix_world @ Vector(c)
           for o in scene.objects if o.type == "MESH" for c in o.bound_box]
    if not pts:
        return
    mn = Vector((min(p.x for p in pts), min(p.y for p in pts), min(p.z for p in pts)))
    mx = Vector((max(p.x for p in pts), max(p.y for p in pts), max(p.z for p in pts)))
    height = mx.z - mn.z
    center = (mn + mx) / 2

    mw = rig.arm.matrix_world
    wrists = [mw @ pb.head for pb in (rig.bone("LeftHand"), rig.bone("RightHand")) if pb]
    print(f"DEBUG malha: min={tuple(round(v, 3) for v in mn)} max={tuple(round(v, 3) for v in mx)}")
    for w in wrists:
        print(f"DEBUG punho: {tuple(round(v, 3) for v in w)}")

    if focus == "full":
        target = Vector((center.x, center.y, mn.z + height * 0.50))
        visible = height * 1.10
    elif focus == "hands" and wrists:
        pts_h = wrists + [wrists[0] + Vector((0, 0, height * 0.12))]
        lo = Vector((min(p.x for p in pts_h), 0, min(p.z for p in pts_h)))
        hi = Vector((max(p.x for p in pts_h), 0, max(p.z for p in pts_h)))
        target = Vector(((lo.x + hi.x) / 2, center.y, (lo.z + hi.z) / 2))
        visible = max(hi.x - lo.x, hi.z - lo.z, height * 0.25) * 1.6
    else:  # "auto": cabeca ate cintura, sempre incluindo os punhos reais
        zs = [mx.z, mn.z + height * 0.40] + [w.z for w in wrists]
        xs = [center.x - height * 0.20, center.x + height * 0.20] + [w.x for w in wrists]
        target = Vector(((min(xs) + max(xs)) / 2, center.y, (min(zs) + max(zs)) / 2))
        visible = max(max(zs) - min(zs), max(xs) - min(xs)) * 1.25

    visible *= margin
    cam = scene.camera
    dist = (visible / 2) / math.tan(cam.data.angle / 2) + (mx.y - mn.y) / 2
    cam.data.clip_start = dist * 0.01
    cam.data.clip_end = dist * 20
    cam.location = target + Vector((0, -dist, 0))
    cam.rotation_euler = (target - cam.location).to_track_quat("-Z", "Y").to_euler()
    print(f"Avatar: altura={height:.3f}  camera a {dist:.3f} (foco={focus})")


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
    ap.add_argument("--mirror", action="store_true",
                    help="espelha a pose (use se a imagem original era de camera 'selfie'/espelhada)")
    ap.add_argument("--smooth", type=float, default=0.6, help="0-1 (1 = sem suavizacao)")
    ap.add_argument("--render-dir", help="pasta para salvar PNGs")
    ap.add_argument("--export-glb", help="salva animacao em .glb")
    ap.add_argument("--fps", type=int, default=30)
    ap.add_argument("--res", type=int, default=720, help="resolucao do render (quadrado)")
    ap.add_argument("--focus", choices=["auto", "full", "hands"], default="auto",
                    help="enquadramento: auto (cabeca ate cintura, com as maos), full (corpo inteiro) ou hands (so as maos)")
    ap.add_argument("--margin", type=float, default=1.0,
                    help="multiplicador de zoom (>1 afasta, <1 aproxima)")
    a = ap.parse_args()

    src = Path(a.input)
    files = sorted(src.glob("*.json")) if src.is_dir() else [src]
    if not files:
        sys.exit("Nenhum JSON encontrado.")

    arm = load_model(a.model)
    scene = bpy.context.scene
    scene.render.fps = a.fps
    setup_scene(scene, a.res)
    rig = Rig(arm)
    esperados = ["LeftArm", "LeftForeArm", "LeftHand", "LeftHandIndex1", "LeftHandThumb1",
                 "RightArm", "RightForeArm", "RightHand", "RightHandIndex1", "RightHandThumb1"]
    faltando = [n for n in esperados if rig.bone(n) is None]
    print(f"Ossos no modelo: {len(rig.names)}")
    if faltando:
        print("AVISO: ossos nao encontrados:", faltando)
        print("Exemplo de nomes no modelo:", rig.names[:12])
    face_front(rig)

    out_dir = Path(a.render_dir) if a.render_dir else None
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)

    prev = None
    for i, f in enumerate(files, start=1):
        frame = load_frame(f, a.width, a.height, swap_hand_labels=not a.keep_labels,
                           mirror=a.mirror)
        frame = smooth(prev, frame, a.smooth)
        prev = frame
        scene.frame_set(i)
        apply_frame(rig, frame)
        if i == 1:
            frame_camera(scene, rig, a.focus, a.margin)
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