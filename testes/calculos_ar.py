import math
import numpy as np

# Fator de temperatura para a Softmax (valores maiores amortecem saltos bruscos)
temperatura = 1.5


# --- DISTÂNCIA EUCLIDIANA ROBUSTA (2D e 3D Real) ---

def ponto_3d_real(p, w, h):
    """
    Converte as coordenadas do MediaPipe para uma escala 3D metricamente coerente.
    MediaPipe x, y estão em [0,1]. z é escalonado similar a x.
    """
    return np.array([p.x * w, p.y * h, p.z * w])

def dist_3d(p1, p2, w, h):
    """Calcula a distância euclidiana 3D real no espaço de pixels/profundidade."""
    v1 = ponto_3d_real(p1, w, h)
    v2 = ponto_3d_real(p2, w, h)
    return float(np.linalg.norm(v1 - v2))

def dist_2d(p1, p2, w, h):
    """Calcula a distância euclidiana 2D simples."""
    x1, y1 = p1.x * w, p1.y * h
    x2, y2 = p2.x * w, p2.y * h
    return math.hypot(x2 - x1, y2 - y1)


# --- ESTIMATIVA ROBUSTA DE YAW (ROTAÇÃO DA CABEÇA) ---

def estimar_yaw(landmarks):
    """
    Estima a rotação horizontal usando múltiplos pontos de referência
    (Eixo central do nariz vs contorno das bochechas/orelhas).
    """
    # Nariz central e pontas laterais das bochechas
    p_nariz = landmarks[1]
    p_esq = landmarks[234]
    p_dir = landmarks[454]
    
    dist_esq = abs(p_nariz.x - p_esq.x)
    dist_dir = abs(p_dir.x - p_nariz.x)
    total = dist_esq + dist_dir
    
    if total == 0:
        return 0.0
    return (dist_dir - dist_esq) / total


# --- BASE DE REFERÊNCIA INTEROCULAR 3D ---

def obter_escala_interocular_3d(landmarks, w, h):
    """
    Calcula o fator de escala de referência usando o vetor 3D
    entre os cantos externos e internos dos dois olhos.
    """
    d1 = dist_3d(landmarks[33], landmarks[263], w, h)   # Cantos externos
    d2 = dist_3d(landmarks[133], landmarks[362], w, h) # Cantos internos
    return (d1 + d2) / 2.0


# --- 1. EAR (Eye Aspect Ratio - Abertura dos Olhos) ---

def calcular_ear(olho_landmarks, landmarks, w, h):
    """
    EAR Padrão de Soukupová & Čech estendido com verificação multi-ponto.
    olho_landmarks: [v1_top, v1_bottom, v2_top, v2_bottom, h_left, h_right]
    """
    v1 = dist_2d(landmarks[olho_landmarks[0]], landmarks[olho_landmarks[1]], w, h)
    v2 = dist_2d(landmarks[olho_landmarks[2]], landmarks[olho_landmarks[3]], w, h)
    h_dist = dist_2d(landmarks[olho_landmarks[4]], landmarks[olho_landmarks[5]], w, h)
    
    if h_dist < 1e-6:
        return 0.0
    return (v1 + v2) / (2.0 * h_dist)


# --- 2. MAR (Mouth Aspect Ratio - Abertura da Boca) ---

def calcular_mar(landmarks, w, h):
    """
    MAR com 3 vetores verticais internos + externos para estabilidade no riso/fala.
    """
    # Sup/Inf Centro (13, 14), Sup/Inf Esquerda (82, 87), Sup/Inf Direita (312, 317)
    v1 = dist_2d(landmarks[13], landmarks[14], w, h)
    v2 = dist_2d(landmarks[82], landmarks[87], w, h)
    v3 = dist_2d(landmarks[312], landmarks[317], w, h)
    
    # Cantos externos da boca (61, 291)
    h_dist = dist_2d(landmarks[61], landmarks[291], w, h)
    
    if h_dist < 1e-6:
        return 0.0
    return (v1 + v2 + v3) / (3.0 * h_dist)


# --- 3. BAR (Brow Aspect Ratio - Altura e Franzimento 3D) ---

def calcular_bar(landmarks, w, h):
    """
    Mede a altura e aproximação das sobrancelhas usando geometria 3D imune à pose.
    """
    escala = obter_escala_interocular_3d(landmarks, w, h)
    if escala < 1e-6:
        return 0.0, 0.0

    # 1. Altura das Sobrancelhas (Pontos superiores 70, 300 para pálpebras 159, 386)
    alt_esq = dist_3d(landmarks[70], landmarks[159], w, h)
    alt_dir = dist_3d(landmarks[300], landmarks[386], w, h)
    altura_relativa = ((alt_esq + alt_dir) / 2.0) / escala

    # 2. Franzimento (Distância em V: Pontas internas 55, 285 + Arco 107, 336 + Glabela 9)
    d_pontas = dist_3d(landmarks[55], landmarks[285], w, h)
    d_arcos = dist_3d(landmarks[107], landmarks[336], w, h)
    d_v_esq = dist_3d(landmarks[55], landmarks[9], w, h)
    d_v_dir = dist_3d(landmarks[285], landmarks[9], w, h)

    franzimento_composto = (d_pontas + d_arcos + d_v_esq + d_v_dir) / 4.0
    dist_relativa_juntas = franzimento_composto / escala

    return altura_relativa, dist_relativa_juntas


# --- 4. NMAR (Normalized Mouth Corner Ratio - Sorriso e Esticamento) ---

def calcular_nmar(landmarks, w, h):
    """
    Mede a largura da boca normalizada pela distância 3D dos olhos.
    """
    escala = obter_escala_interocular_3d(landmarks, w, h)
    if escala < 1e-6:
        return 0.0
        
    dist_boca = dist_3d(landmarks[61], landmarks[291], w, h)
    return dist_boca / escala


# --- 5. PUP (Pucker Ratio - Bico / Lábios Projetados) ---

def calcular_pup(landmarks, w, h):
    """
    Mede a projeção vertical dos lábios externos e internos vs largura labial.
    """
    largura_boca = dist_2d(landmarks[61], landmarks[291], w, h)
    if largura_boca < 1e-6:
        return 0.0
        
    # Combinação de Lábio Externo (0, 17) e Lábio Interno (13, 14)
    alt_ext = dist_2d(landmarks[0], landmarks[17], w, h)
    alt_int = dist_2d(landmarks[13], landmarks[14], w, h)
    
    return ((alt_ext + alt_int) / 2.0) / largura_boca


# --- 6. CAR (Cheek Aspect Ratio - Bochechas Infladas) ---

def calcular_car(landmarks, w, h):
    """
    Mede a expansão das bochechas normalizada pelo eixo vertical central do rosto.
    Ajustado com múltiplos pontos para capturar inflamento.
    """
    # Eixo vertical do rosto: Nariz (1) ao Queixo (152) e Testa (10) a Queixo (152)
    alt_rosto1 = dist_3d(landmarks[1], landmarks[152], w, h)
    alt_rosto2 = dist_3d(landmarks[10], landmarks[152], w, h)
    alt_media = (alt_rosto1 + alt_rosto2 / 2.0) / 2.0
    
    if alt_media < 1e-6:
        return 0.0
        
    # Eixo horizontal das bochechas (234 a 454) + Maçãs do rosto (192 a 416)
    larg_bochechas1 = dist_3d(landmarks[234], landmarks[454], w, h)
    larg_bochechas2 = dist_3d(landmarks[192], landmarks[416], w, h)
    larg_media = (larg_bochechas1 + larg_bochechas2) / 2.0

    return larg_media / (2.0 * alt_media)


# --- 7. NAR (Nasal Aspect Ratio - Nariz Franzido) ---

def calcular_nar(landmarks, w, h):
    """
    Mede o encurtamento do dorso nasal (AU9 - Nose Wrinkle).
    """
    dist_interna_olhos = dist_3d(landmarks[133], landmarks[362], w, h)
    if dist_interna_olhos < 1e-6:
        return 0.0
        
    # Multi-ponto do dorso: (198 a 2) e (6 a 2)
    dorso1 = dist_3d(landmarks[198], landmarks[2], w, h)
    dorso2 = dist_3d(landmarks[6], landmarks[2], w, h)
    dorso_medio = (dorso1 + dorso2) / 2.0

    return dorso_medio / dist_interna_olhos


# --- SOFTMAX COM TEMPERATURA E PROTEÇÃO NUMÉRICA ---

def calcular_probabilidades(scores):
    """
    Aplica Softmax com escala de temperatura ajustada.
    """
    scores_array = np.array(scores, dtype=np.float64)
    scaled = scores_array / temperatura
    # Subtrai o máximo para estabilidade numérica e evitar overflow exponencial
    exp_scores = np.exp(scaled - np.max(scaled))
    
    soma = exp_scores.sum()
    if soma == 0:
        return np.ones_like(scores_array) / len(scores_array)
        
    return exp_scores / soma