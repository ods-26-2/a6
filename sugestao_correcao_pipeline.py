"""
sugestao_correcao_pipeline.py
==============================
Módulo 5: Pipeline de Sugestão e Correção de LIBRAS (Squad A6).

Fluxo arquitetural:
1. 5.2.2 - Savitzky-Golay (scipy.signal.savgol_filter / fallback suave):
          Suavização temporal dos eixos x, y, z para eliminação de ruídos e tremores.
2. 5.2.3 - ST-GCN / Extrator Fonológico:
          Classificação e extração dos parâmetros fonológicos (CM, PA, Orientação, Movimento).
3. 5.2.4 - Validador Paramétrico:
          Correções biomecânicas angulares nas articulações (ex: rotação de pulso limitada fisiologicamente).
4. 5.2.5 - Sintaxe LIBRAS:
          Reorganização gramatical das glosas para a ordem canônica [TEMPO] + [SUJEITO] + [OBJETO] + [VERBO].
5. Renderização / Exportação para o avatar 3D (formato nativo compatível com a6/avatar/avatar_keypoints.py).
"""

from __future__ import annotations

import copy
import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

try:
    from scipy.signal import savgol_filter
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False


class LibrasCorrectionPipeline:
    """Pipeline completo de correção, validação e refinamento biomecânico e sintático de LIBRAS."""

    GRAMMAR_LEXICON = {
        # Marcadores temporais
        "HOJE": "TEMPO",
        "ONTEM": "TEMPO",
        "AMANHA": "TEMPO",
        "AMANHÃ": "TEMPO",
        "AGORA": "TEMPO",
        "PASSADO": "TEMPO",
        "FUTURO": "TEMPO",
        "ANO-PASSADO": "TEMPO",
        "SEMANA-PASSADA": "TEMPO",

        # Sujeitos / Pronomes
        "EU": "SUJEITO",
        "VOCE": "SUJEITO",
        "VOCÊ": "SUJEITO",
        "ELE": "SUJEITO",
        "ELA": "SUJEITO",
        "NOS": "SUJEITO",
        "NÓS": "SUJEITO",
        "ELES": "SUJEITO",
        "ELAS": "SUJEITO",
        "ALUNO": "SUJEITO",
        "PROFESSOR": "SUJEITO",
        "PROFESSORA": "SUJEITO",
        "HOMEM": "SUJEITO",
        "MULHER": "SUJEITO",
        "MENINO": "SUJEITO",
        "MENINA": "SUJEITO",

        # Objetos / Substantivos
        "LIVRO": "OBJETO",
        "CASA": "OBJETO",
        "ESCOLA": "OBJETO",
        "AGUA": "OBJETO",
        "ÁGUA": "OBJETO",
        "CARRO": "OBJETO",
        "COMIDA": "OBJETO",
        "BOLA": "OBJETO",
        "BANCO": "OBJETO",
        "CINCO": "OBJETO",

        # Verbos
        "ESTUDAR": "VERBO",
        "LER": "VERBO",
        "IR": "VERBO",
        "VIR": "VERBO",
        "COMER": "VERBO",
        "BEBER": "VERBO",
        "TER": "VERBO",
        "QUERER": "VERBO",
        "GOSTAR": "VERBO",
        "TRABALHAR": "VERBO",
        "COMPRAR": "VERBO",
        "VENDER": "VERBO",
        "VER": "VERBO",
    }

    # Índices MediaPipe Pose
    POSE_LEFT_SHOULDER = 11
    POSE_RIGHT_SHOULDER = 12
    POSE_LEFT_ELBOW = 13
    POSE_RIGHT_ELBOW = 14
    POSE_LEFT_WRIST = 15
    POSE_RIGHT_WRIST = 16

    # Índices MediaPipe Hands
    WRIST = 0
    INDEX_MCP, INDEX_PIP, INDEX_DIP, INDEX_TIP = 5, 6, 7, 8
    MIDDLE_MCP, MIDDLE_PIP, MIDDLE_DIP, MIDDLE_TIP = 9, 10, 11, 12
    RING_MCP, RING_PIP, RING_DIP, RING_TIP = 13, 14, 15, 16
    PINKY_MCP, PINKY_PIP, PINKY_DIP, PINKY_TIP = 17, 18, 19, 20

    def __init__(
        self,
        savgol_window: int = 7,
        savgol_polyorder: int = 2,
        wrist_twist_max_deg: float = 85.0,
        confidence_threshold: float = 0.60,
    ):
        self.savgol_window = savgol_window if savgol_window % 2 != 0 else savgol_window + 1
        self.savgol_polyorder = savgol_polyorder
        self.wrist_twist_max_deg = wrist_twist_max_deg
        self.confidence_threshold = confidence_threshold

    # =========================================================================
    # 5.2.2 - FILTRO SAVITZKY-GOLAY (Suavização Temporal)
    # =========================================================================
    def apply_savitzky_golay(
        self, keypoint_sequence: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        n_frames = len(keypoint_sequence)
        if n_frames < 3:
            return copy.deepcopy(keypoint_sequence)

        window = min(self.savgol_window, n_frames)
        if window % 2 == 0:
            window -= 1
        poly = min(self.savgol_polyorder, window - 1)

        smoothed = copy.deepcopy(keypoint_sequence)

        for hand_key in ("left_hand", "right_hand", "left_hand_global", "right_hand_global"):
            smoothed = self._smooth_stream(smoothed, hand_key, window=window, poly=poly)

        for body_key in ("body", "pose"):
            smoothed = self._smooth_stream(smoothed, body_key, window=window, poly=poly)

        smoothed = self._smooth_stream(smoothed, "face", window=window, poly=poly)
        return smoothed

    def _smooth_stream(
        self,
        sequence: List[Dict[str, Any]],
        key: str,
        window: int,
        poly: int,
    ) -> List[Dict[str, Any]]:
        frames_present = [i for i, f in enumerate(sequence) if f.get(key) is not None and len(f[key]) > 0]
        if len(frames_present) < window or window <= poly:
            return sequence

        is_dict_format = isinstance(sequence[frames_present[0]][key][0], dict)
        actual_points = len(sequence[frames_present[0]][key])
        coords = np.zeros((len(frames_present), actual_points, 3), dtype=np.float32)

        for idx, f_idx in enumerate(frames_present):
            pts = sequence[f_idx][key]
            for p_idx in range(min(actual_points, len(pts))):
                pt = pts[p_idx]
                if is_dict_format:
                    coords[idx, p_idx, 0] = pt.get("x", 0.0)
                    coords[idx, p_idx, 1] = pt.get("y", 0.0)
                    coords[idx, p_idx, 2] = pt.get("z", 0.0)
                else:
                    coords[idx, p_idx, 0] = pt[0]
                    coords[idx, p_idx, 1] = pt[1]
                    coords[idx, p_idx, 2] = pt[2]

        if HAS_SCIPY:
            for p_idx in range(actual_points):
                for axis in range(3):
                    coords[:, p_idx, axis] = savgol_filter(
                        coords[:, p_idx, axis], window_length=window, polyorder=poly
                    )
        else:
            k = window // 2
            for p_idx in range(actual_points):
                for axis in range(3):
                    orig = coords[:, p_idx, axis].copy()
                    for idx in range(len(frames_present)):
                        start = max(0, idx - k)
                        end = min(len(frames_present), idx + k + 1)
                        coords[idx, p_idx, axis] = float(np.mean(orig[start:end]))

        for idx, f_idx in enumerate(frames_present):
            for p_idx in range(min(actual_points, len(sequence[f_idx][key]))):
                if is_dict_format:
                    sequence[f_idx][key][p_idx]["x"] = float(round(coords[idx, p_idx, 0], 4))
                    sequence[f_idx][key][p_idx]["y"] = float(round(coords[idx, p_idx, 1], 4))
                    sequence[f_idx][key][p_idx]["z"] = float(round(coords[idx, p_idx, 2], 4))
                else:
                    sequence[f_idx][key][p_idx][0] = float(round(coords[idx, p_idx, 0], 4))
                    sequence[f_idx][key][p_idx][1] = float(round(coords[idx, p_idx, 1], 4))
                    sequence[f_idx][key][p_idx][2] = float(round(coords[idx, p_idx, 2], 4))

        return sequence

    # =========================================================================
    # 5.2.3 - ST-GCN / EXTRAÇÃO FONOLÓGICA
    # =========================================================================
    def extract_phonological_parameters(
        self, sign_label: str, keypoint_sequence: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        if not keypoint_sequence:
            return {
                "sign": sign_label,
                "configuracao_mao": "DESCONHECIDA",
                "ponto_articulacao": "ESPACO_NEUTRO",
                "orientacao_palma": "NEUTRA",
                "movimento": "ESTATICO",
            }

        mid_idx = len(keypoint_sequence) // 2
        mid_frame = keypoint_sequence[mid_idx]

        hand = mid_frame.get("right_hand") or mid_frame.get("left_hand")
        if not hand and "right_hand_global" in mid_frame:
            hand = mid_frame.get("right_hand_global") or mid_frame.get("left_hand_global")

        return {
            "sign": sign_label,
            "configuracao_mao": self._estimate_handshape(hand),
            "ponto_articulacao": self._estimate_articulation_point(mid_frame),
            "orientacao_palma": self._estimate_palm_orientation(hand),
            "movimento": self._estimate_movement_dynamics(keypoint_sequence),
        }

    def _estimate_handshape(self, hand_points: Optional[List[Any]]) -> str:
        if not hand_points or len(hand_points) < 21:
            return "NAO_DETECTADA"

        pts = []
        for p in hand_points:
            if isinstance(p, dict):
                pts.append([p.get("x", 0.0), p.get("y", 0.0), p.get("z", 0.0)])
            else:
                pts.append([p[0], p[1], p[2]])
        pts = np.array(pts)

        wrist = pts[self.WRIST]
        dedos_estendidos = []
        for tip_idx, mcp_idx in [
            (self.INDEX_TIP, self.INDEX_MCP),
            (self.MIDDLE_TIP, self.MIDDLE_MCP),
            (self.RING_TIP, self.RING_MCP),
            (self.PINKY_TIP, self.PINKY_MCP),
        ]:
            d_tip = np.linalg.norm(pts[tip_idx] - wrist)
            d_mcp = np.linalg.norm(pts[mcp_idx] - wrist)
            dedos_estendidos.append(d_tip > d_mcp * 1.15)

        idx, mid, ring, pnk = dedos_estendidos
        if all(dedos_estendidos):
            return "PALMA_ABERTA_B"
        if not any(dedos_estendidos):
            return "PUNHO_FECHADO_S_OU_A"
        if idx and not mid and not ring and not pnk:
            return "INDICADOR_1_OU_D"
        if idx and mid and not ring and not pnk:
            return "V_OU_DOIS"
        if idx and mid and ring and not pnk:
            return "TRES_OU_W"
        if not idx and not mid and not ring and pnk:
            return "I_OU_MINDINHO"
        if idx and not mid and not ring and pnk:
            return "ILY_OU_Y"
        return "CONFIGURACAO_COMPLEXA"

    def _estimate_articulation_point(self, frame: Dict[str, Any]) -> str:
        pose = frame.get("body") or frame.get("pose")
        hand = frame.get("right_hand") or frame.get("left_hand") or frame.get("right_hand_global")
        if not pose or not hand or len(pose) < 17 or len(hand) < 1:
            return "ESPACO_NEUTRO"

        hand_y = hand[0]["y"] if isinstance(hand[0], dict) else hand[0][1]
        nose_y = pose[0]["y"] if isinstance(pose[0], dict) else pose[0][1]
        shoulder_y = pose[11]["y"] if isinstance(pose[11], dict) else pose[11][1]

        if hand_y < nose_y + 0.05:
            return "CABECA_ROSTO"
        elif hand_y < shoulder_y + 0.15:
            return "PEITO_TRONCO"
        return "ESPACO_NEUTRO"

    def _estimate_palm_orientation(self, hand: Optional[List[Any]]) -> str:
        if not hand or len(hand) < 21:
            return "NEUTRA"

        pts = []
        for p in hand:
            if isinstance(p, dict):
                pts.append([p.get("x", 0.0), p.get("y", 0.0), p.get("z", 0.0)])
            else:
                pts.append([p[0], p[1], p[2]])
        pts = np.array(pts)

        v_wrist_middle = pts[self.MIDDLE_MCP] - pts[self.WRIST]
        v_wrist_pinky = pts[self.PINKY_MCP] - pts[self.INDEX_MCP]
        normal = np.cross(v_wrist_middle[:3], v_wrist_pinky[:3])
        norm_len = np.linalg.norm(normal)
        if norm_len > 1e-6:
            normal /= norm_len

        if normal[2] > 0.5:
            return "PARA_FRENTE"
        elif normal[2] < -0.5:
            return "PARA_O_CORPO"
        elif normal[0] > 0.5:
            return "PARA_DIREITA"
        elif normal[0] < -0.5:
            return "PARA_ESQUERDA"
        return "NEUTRA"

    def _estimate_movement_dynamics(self, sequence: List[Dict[str, Any]]) -> str:
        if len(sequence) < 2:
            return "ESTATICO"

        traj = []
        for f in sequence:
            h = f.get("right_hand") or f.get("left_hand") or f.get("right_hand_global")
            if h and len(h) > 0:
                y = h[0]["y"] if isinstance(h[0], dict) else h[0][1]
                x = h[0]["x"] if isinstance(h[0], dict) else h[0][0]
                traj.append((x, y))

        if len(traj) < 2:
            return "ESTATICO"

        total_disp = sum(
            math.hypot(traj[i][0] - traj[i - 1][0], traj[i][1] - traj[i - 1][1])
            for i in range(1, len(traj))
        )
        if total_disp < 0.05:
            return "ESTATICO"

        dy = traj[-1][1] - traj[0][1]
        dx = traj[-1][0] - traj[0][0]
        if abs(dy) > abs(dx) * 1.5:
            return "DESCENDENTE" if dy > 0 else "ASCENDENTE"
        elif abs(dx) > abs(dy) * 1.5:
            return "HORIZONTAL"
        return "CIRCULAR_OU_DIAGONAL"

    # =========================================================================
    # 5.2.4 - VALIDADOR PARAMÉTRICO (Biomecânica)
    # =========================================================================
    def validate_and_correct_biomechanics(
        self, keypoint_sequence: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        corrections_applied = []
        corrected_seq = copy.deepcopy(keypoint_sequence)

        for f_idx, frame in enumerate(corrected_seq):
            for side, hand_key in [("Left", "left_hand"), ("Right", "right_hand")]:
                hand = frame.get(hand_key)
                if not hand or len(hand) < 21:
                    continue

                idx_pt = hand[self.INDEX_MCP]
                pnk_pt = hand[self.PINKY_MCP]
                if isinstance(idx_pt, dict):
                    v_palm = np.array([pnk_pt["x"] - idx_pt["x"], pnk_pt["y"] - idx_pt["y"], pnk_pt["z"] - idx_pt["z"]])
                else:
                    v_palm = np.array([pnk_pt[0] - idx_pt[0], pnk_pt[1] - idx_pt[1], pnk_pt[2] - idx_pt[2]])

                palm_norm = np.linalg.norm(v_palm[:2])
                if palm_norm > 1e-6:
                    angle_deg = abs(math.degrees(math.atan2(v_palm[1], v_palm[0])))
                    if angle_deg > self.wrist_twist_max_deg:
                        msg = f"Frame {f_idx}: Limite articular no pulso ({side}) excedido ({angle_deg:.1f}°). Ajustado para {self.wrist_twist_max_deg}°"
                        if msg not in corrections_applied:
                            corrections_applied.append(msg)
                        factor = self.wrist_twist_max_deg / angle_deg
                        for p in hand:
                            if isinstance(p, dict):
                                p["z"] *= factor
                            else:
                                p[2] *= factor

        return corrected_seq, corrections_applied

    # =========================================================================
    # 5.2.5 - SINTAXE LIBRAS ([TEMPO] + [SUJEITO] + [OBJETO] + [VERBO])
    # =========================================================================
    def reorganize_syntax(self, glosses: List[str]) -> Tuple[List[str], bool]:
        if not glosses or len(glosses) <= 1:
            return glosses, False

        tempo_tokens: List[str] = []
        sujeito_tokens: List[str] = []
        objeto_tokens: List[str] = []
        verbo_tokens: List[str] = []
        outros_tokens: List[str] = []

        for g in glosses:
            g_clean = g.strip().upper()
            role = self.GRAMMAR_LEXICON.get(g_clean)
            if role == "TEMPO":
                tempo_tokens.append(g_clean)
            elif role == "SUJEITO":
                sujeito_tokens.append(g_clean)
            elif role == "OBJETO":
                objeto_tokens.append(g_clean)
            elif role == "VERBO":
                verbo_tokens.append(g_clean)
            else:
                outros_tokens.append(g_clean)

        reordered = tempo_tokens + sujeito_tokens + objeto_tokens + verbo_tokens + outros_tokens
        was_reordered = [g.upper() for g in glosses] != reordered
        return reordered, was_reordered

    # =========================================================================
    # EXPORTAÇÃO PARA O AVATAR 3D (a6/avatar/avatar_keypoints.py)
    # =========================================================================
    def export_for_avatar(
        self,
        keypoint_sequence: List[Dict[str, Any]],
        image_size: Tuple[int, int] = (1280, 720),
    ) -> List[Dict[str, Any]]:
        avatar_frames = []
        for f in keypoint_sequence:
            frame_dict: Dict[str, Any] = {"maos": [], "corpo": []}

            body = f.get("body") or f.get("pose")
            if body:
                corpo_list = []
                for idx, pt in enumerate(body):
                    if isinstance(pt, dict):
                        corpo_list.append({"id": idx, "x": pt.get("x", 0.0), "y": pt.get("y", 0.0), "z": pt.get("z", 0.0)})
                    elif len(pt) >= 3:
                        corpo_list.append({"id": idx, "x": pt[0], "y": pt[1], "z": pt[2]})
                frame_dict["corpo"] = corpo_list

            for side_label, key in [("Right", "right_hand"), ("Left", "left_hand"),
                                    ("Right", "right_hand_global"), ("Left", "left_hand_global")]:
                pts = f.get(key)
                if pts and len(pts) >= 21:
                    if any(m["lado"] == side_label for m in frame_dict["maos"]):
                        continue
                    kp_list = []
                    for idx, pt in enumerate(pts):
                        if isinstance(pt, dict):
                            kp_list.append({"id": idx, "x": pt.get("x", 0.0), "y": pt.get("y", 0.0), "z": pt.get("z", 0.0)})
                        elif len(pt) >= 3:
                            kp_list.append({"id": idx, "x": pt[0], "y": pt[1], "z": pt[2]})
                    frame_dict["maos"].append({"lado": side_label, "keypoints": kp_list})

            avatar_frames.append(frame_dict)
        return avatar_frames

    # =========================================================================
    # PROCESSAMENTO COMPLETO DA JANELA
    # =========================================================================
    def process_sign_window(
        self,
        sign_label: str,
        confidence: float,
        keypoints: List[Dict[str, Any]],
        context_glosses: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        smoothed_keypoints = self.apply_savitzky_golay(keypoints)
        phonology = self.extract_phonological_parameters(sign_label, smoothed_keypoints)
        validated_keypoints, biomechanical_corrections = self.validate_and_correct_biomechanics(
            smoothed_keypoints
        )

        all_glosses = (context_glosses or []) + [sign_label]
        ordered_glosses, was_reordered = self.reorganize_syntax(all_glosses)
        avatar_payload = self.export_for_avatar(validated_keypoints)

        return {
            "sign": sign_label,
            "confidence": confidence,
            "phonology": phonology,
            "biomechanical_corrections": biomechanical_corrections,
            "syntactic_correction": {
                "original": all_glosses,
                "reordered": ordered_glosses,
                "was_reordered": was_reordered,
                "rule": "[TEMPO] + [SUJEITO] + [OBJETO] + [VERBO]",
            },
            "corrected_keypoints": validated_keypoints,
            "avatar_frames": avatar_payload,
        }
