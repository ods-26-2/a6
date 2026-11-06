"""
trajectory_format.py - Converte o JSON de trajetoria (formato S6/I4) em frames para o avatar.

Formato de entrada (um unico arquivo com a trajetoria inteira, tudo em ordem):
    {
      "fps": 25.0, "frame_count": 132, "coordinates": "NORMALIZED",
      "frames": [
        {"body": [[x,y,z,visibilidade] x33],      # MediaPipe Pose
         "left_hand":  [[x,y,z] x21] | null,      # mao ANATOMICA esquerda
         "right_hand": [[x,y,z] x21] | null,
         "face": [[x,y,z] x92]},                  # ainda nao usado pelo avatar
        ...
      ]
    }
Coordenadas: origem no centro dos ombros, unidade = largura dos ombros, y para baixo.

Saida: lista de frames {"hands": {"Left"|"Right": {0..20: Vector}},
                        "pose":  {11..16: Vector}}
no mesmo formato que `avatar_keypoints.apply_frame` ja entende.

Tratamentos (o MediaPipe falha/pisca com frequencia):
  - buracos curtos de mao (<= max_gap frames) sao preenchidos por interpolacao;
  - aparicoes curtas de mao (< min_run frames) sao descartadas (falsas deteccoes);
  - o braco so e guiado pelo corpo quando ombro/cotovelo (e punho, se nao ha mao) estao visiveis;
  - o z do corpo e muito ruidoso, entao e atenuado (z_body).
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from mathutils import Vector

# Indices do MediaPipe Pose por lado anatomico: (ombro, cotovelo, punho)
POSE_ARM = {"Left": (11, 13, 15), "Right": (12, 14, 16)}
HAND_KEYS = {"Left": "left_hand", "Right": "right_hand"}

# Valores padrao (ajustaveis na linha de comando do generate_video.py)
Z_BODY = 0.5        # atenuacao do z dos pontos do corpo (o z do MediaPipe Pose e instavel)
Z_HAND = 1.0
VIS_MIN = 0.5       # visibilidade minima do ponto do corpo para guiar o braco
MIN_RUN = 6         # mao presente por menos frames que isso = ruido
MAX_GAP = 4         # buracos de ate N frames sao preenchidos


def _swap(side: str) -> str:
    return "Right" if side == "Left" else "Left"


def _valid_hand(raw) -> Optional[list]:
    if not raw or len(raw) < 21:
        return None
    return raw


def limpar_maos(seq: List[Optional[list]], max_gap: int, min_run: int) -> List[Optional[list]]:
    """seq: por frame, mao (21 pontos [x,y,z]) ou None.
    1) preenche buracos de ate `max_gap` frames por interpolacao linear;
    2) remove sequencias de mao com menos de `min_run` frames."""
    n = len(seq)
    out = list(seq)

    i = 0
    while i < n:
        if out[i] is None:
            j = i
            while j < n and out[j] is None:
                j += 1
            gap = j - i
            if i > 0 and j < n and gap <= max_gap:
                a, b = out[i - 1], out[j]
                for k in range(i, j):
                    t = (k - i + 1) / (gap + 1)
                    out[k] = [[pa[c] + (pb[c] - pa[c]) * t for c in range(3)]
                              for pa, pb in zip(a, b)]
            i = j
        else:
            i += 1

    i = 0
    while i < n:
        if out[i] is not None:
            j = i
            while j < n and out[j] is not None:
                j += 1
            if j - i < min_run:
                for k in range(i, j):
                    out[k] = None
            i = j
        else:
            i += 1
    return out


_DUMMY = [[0.0, 0.0, 0.0]] * 21   # "mao" falsa so para reaproveitar limpar_maos em flags


def limpar_flags(flags: List[bool], max_gap: int, min_run: int) -> List[bool]:
    """Mesma limpeza das maos (preenche buracos curtos, remove blocos curtos) para uma lista de bool."""
    seq = limpar_maos([_DUMMY if f else None for f in flags], max_gap, min_run)
    return [x is not None for x in seq]


def build_frames(
    doc: Dict[str, Any],
    mirror: bool = False,
    z_body: float = Z_BODY,
    z_hand: float = Z_HAND,
    vis_min: float = VIS_MIN,
    min_run: int = MIN_RUN,
    max_gap: int = MAX_GAP,
) -> List[Dict[str, Any]]:
    """Converte o documento de trajetoria em frames {"hands":..., "pose":...} para o avatar."""
    raw_frames = doc.get("frames")
    if not isinstance(raw_frames, list) or not raw_frames:
        raise ValueError("JSON sem a lista 'frames' (ou lista vazia).")

    if not any(k in raw_frames[0] for k in ("body", "left_hand", "right_hand")):
        raise ValueError(
            "frames sem 'body'/'left_hand'/'right_hand' - formato nao reconhecido "
            "(o formato antigo 'maos'/'corpo' nao e suportado por este conversor)."
        )

    sx = -1.0 if mirror else 1.0   # espelhar = inverter o eixo X e trocar esquerda/direita

    def world(p, zs):
        # (x dir, y baixo, z profundidade) -> mundo Blender (X dir, Y p/ longe da camera, Z cima)
        return Vector((sx * p[0], p[2] * zs, -p[1]))

    hands_by_side = {
        side: limpar_maos([_valid_hand(f.get(key)) for f in raw_frames], max_gap, min_run)
        for side, key in HAND_KEYS.items()
    }

    def braco_confiavel(side: str, idx: int) -> bool:
        body = raw_frames[idx].get("body") or []
        if len(body) < 17:
            return False
        s_id, e_id, w_id = POSE_ARM[side]
        tem_mao = hands_by_side[side][idx] is not None
        return (body[s_id][3] >= vis_min and body[e_id][3] >= vis_min
                and (tem_mao or body[w_id][3] >= vis_min))

    # evita "tranco" do braco: so usa o corpo em trechos estaveis (sem frames isolados)
    arm_ok = {side: limpar_flags([braco_confiavel(side, i) for i in range(len(raw_frames))],
                                 max_gap, min_run)
              for side in ("Left", "Right")}

    frames: List[Dict[str, Any]] = []
    for idx, f in enumerate(raw_frames):
        body = f.get("body") or []
        hands: Dict[str, Dict[int, Vector]] = {}
        pose: Dict[int, Vector] = {}

        for side in ("Left", "Right"):
            out_side = _swap(side) if mirror else side      # lado do AVATAR
            hand = hands_by_side[side][idx]
            if hand is not None:
                hands[out_side] = {i: world(hand[i], z_hand) for i in range(21)}

            # braco guiado pelo corpo (ombro -> cotovelo -> punho)
            if len(body) < 17 or not arm_ok[side][idx]:
                continue   # sem confianca: o avatar usa o braco relaxado / levantado padrao
            s_id, e_id, w_id = POSE_ARM[side]

            wrist = list(body[w_id][:3])
            if hand is not None:        # o detector de mao e mais preciso no plano da imagem
                wrist[0], wrist[1] = hand[0][0], hand[0][1]
            ids = POSE_ARM[out_side]
            pose[ids[0]] = world(body[s_id], z_body)
            pose[ids[1]] = world(body[e_id], z_body)
            pose[ids[2]] = world(wrist, z_body)

        frames.append({"hands": hands, "pose": pose})
    return frames


def verificar_escala(doc: Dict[str, Any]) -> Optional[str]:
    """Retorna um aviso se o arquivo parecer estar em coordenadas de IMAGEM (0..1), como as mensagens
    ods.i4.keypoints, e nao centrado nos ombros com largura de ombros ~1.0 (formato da trajetoria do S6).
    ("NORMALIZED" significa coisas diferentes nos dois casos.)"""
    for f in doc.get("frames", []):
        body = f.get("body") or []
        if len(body) >= 13:
            d = math.dist(body[11][:2], body[12][:2])
            if not 0.7 <= d <= 1.3:
                return (f"largura dos ombros = {d:.2f} (esperado ~1.0). O arquivo parece estar em "
                        f"coordenadas de imagem [0,1] e nao centrado nos ombros; a pose pode sair distorcida.")
            return None
    return None


def resumo(frames: List[Dict[str, Any]]) -> str:
    n = len(frames)
    cnt = lambda side: sum(1 for f in frames if side in f["hands"])
    arms = lambda ids: sum(1 for f in frames if ids[0] in f["pose"])
    return (f"{n} frames | mao esquerda: {cnt('Left')} | mao direita: {cnt('Right')} | "
            f"braco esq. pelo corpo: {arms(POSE_ARM['Left'])} | braco dir. pelo corpo: {arms(POSE_ARM['Right'])}")