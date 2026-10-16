import cv2
import mediapipe as mp
import numpy as np
import testes.calculos_ar as ar

mp_holistic = mp.solutions.holistic
mp_drawing = mp.solutions.drawing_utils

PONTOS_OLHO_ESQ = [160, 144, 158, 153, 33, 133]
PONTOS_OLHO_DIR = [385, 380, 387, 373, 362, 263]
PONTOS_BAR = [70, 159, 300, 386, 33, 263, 55, 285, 9]
PONTOS_VERMELHOS = [165, 391, 164, 18]

# --- 1. CONFIGURAÇÃO DA CÂMERA ---
cap = cv2.VideoCapture(0)

# Tenta definir a resolução de captura para HD (1280x720)
cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)

NOME_JANELA = 'Reconhecedor de LIBRAS - Mapeamento e Calibração'

# --- 2. CONFIGURAÇÃO DA JANELA QUE SE ADAPTA AUTOMATICAMENTE ---
cv2.namedWindow(NOME_JANELA, cv2.WINDOW_NORMAL | cv2.WINDOW_KEEPRATIO)
cv2.resizeWindow(NOME_JANELA, 1280, 720)

# --- ESTADOS DO MAPEAMENTO/CALIBRAÇÃO ---
estagio_calibracao = 'FRENTE'
frames_estagio = 0
FRAMES_NECESSARIOS = 30  

calib_frente = []
calib_direita = []
calib_esquerda = []

perfis_neutros = {
    'frente': None,
    'direita': None,
    'esquerda': None
}

# --- VARIÁVEIS DE ESTABILIZAÇÃO TEMPORAL ---
probabilidades_suavizadas = None
ALPHA_SUAVIZACAO = 0.20
posicao_cabeca_atual = "Frente"

with mp_holistic.Holistic(
    min_detection_confidence=0.5,
    min_tracking_confidence=0.5
) as holistic:

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        # Espelhamento da câmera
        frame = cv2.flip(frame, 1)
        altura, largura, _ = frame.shape

        image = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        image.flags.writeable = False
        results = holistic.process(image)
        image.flags.writeable = True
        image = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)

        # RESET SE O ROSTO SUMIR DA CÂMERA
        if not results.face_landmarks:
            estagio_calibracao = 'FRENTE'
            frames_estagio = 0
            calib_frente.clear()
            calib_direita.clear()
            calib_esquerda.clear()
            perfis_neutros = {'frente': None, 'direita': None, 'esquerda': None}
            probabilidades_suavizadas = None

            cv2.putText(image, "Aguardando rosto para iniciar calibracao...", (30, altura // 2),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
        else:
            landmarks = results.face_landmarks.landmark
            yaw = ar.estimar_yaw(landmarks)

            # Desenhar malha do rosto
            mp_drawing.draw_landmarks(
                image, results.face_landmarks, mp_holistic.FACEMESH_TESSELATION,
                mp_drawing.DrawingSpec(color=(80, 110, 10), thickness=1, circle_radius=1)
            )

            # Destaque dos pontos vermelhos
            for idx in PONTOS_VERMELHOS:
                pt = landmarks[idx]
                cx, cy = int(pt.x * largura), int(pt.y * altura)
                cv2.circle(image, (cx, cy), 5, (0, 0, 255), -1)
                cv2.circle(image, (cx, cy), 6, (0, 0, 0), 1)

            # CÁLCULO DOS RATIOS
            ear_esq = ar.calcular_ear(PONTOS_OLHO_ESQ, landmarks, largura, altura)
            ear_dir = ar.calcular_ear(PONTOS_OLHO_DIR, landmarks, largura, altura)
            ear_medio = (ear_esq + ear_dir) / 2.0

            mar = ar.calcular_mar(landmarks, largura, altura)
            bar_altura, bar_juntas = ar.calcular_bar(landmarks, largura, altura)
            nmar = ar.calcular_nmar(landmarks, largura, altura)
            pup = ar.calcular_pup(landmarks, largura, altura)
            car = ar.calcular_car(landmarks, largura, altura)
            nar = ar.calcular_nar(landmarks, largura, altura)

            vetor_atual = np.array([bar_altura, bar_juntas, ear_medio, mar, nmar, pup, car, nar])

            # --- ETAPA DE MAPEAMENTO / CALIBRAÇÃO ---
            if estagio_calibracao != 'CONCLUIDO':
                cv2.rectangle(image, (20, 20), (largura - 20, 120), (0, 0, 0), -1)

                if estagio_calibracao == 'FRENTE':
                    instrucao = f"Fique de FRENTE com expressao NEUTRA ({frames_estagio}/{FRAMES_NECESSARIOS})"
                    if abs(yaw) < 0.12:
                        calib_frente.append(vetor_atual)
                        frames_estagio += 1
                        if frames_estagio >= FRAMES_NECESSARIOS:
                            perfis_neutros['frente'] = np.mean(calib_frente, axis=0)
                            estagio_calibracao = 'DIREITA'
                            frames_estagio = 0
                    else:
                        instrucao += " [Olhe diretamente para a camera!]"

                elif estagio_calibracao == 'DIREITA':
                    instrucao = f"Vire um pouco para a DIREITA (neutro) ({frames_estagio}/{FRAMES_NECESSARIOS})"
                    if yaw > 0.12:
                        calib_direita.append(vetor_atual)
                        frames_estagio += 1
                        if frames_estagio >= FRAMES_NECESSARIOS:
                            perfis_neutros['direita'] = np.mean(calib_direita, axis=0)
                            estagio_calibracao = 'ESQUERDA'
                            frames_estagio = 0
                    else:
                        instrucao += " [Gire a cabeca mais para a direita!]"

                elif estagio_calibracao == 'ESQUERDA':
                    instrucao = f"Vire um pouco para a ESQUERDA (neutro) ({frames_estagio}/{FRAMES_NECESSARIOS})"
                    if yaw < -0.12:
                        calib_esquerda.append(vetor_atual)
                        frames_estagio += 1
                        if frames_estagio >= FRAMES_NECESSARIOS:
                            perfis_neutros['esquerda'] = np.mean(calib_esquerda, axis=0)
                            estagio_calibracao = 'CONCLUIDO'

                cv2.putText(image, "CALIBRANDO ROSTO", (30, 55),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2)
                cv2.putText(image, instrucao, (30, 95),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)

            # --- ETAPA DE RECONHECIMENTO (PÓS-CALIBRAÇÃO) ---
            else:
                if posicao_cabeca_atual == "Frente":
                    if yaw > 0.14:
                        posicao_cabeca_atual = "Direita"
                    elif yaw < -0.14:
                        posicao_cabeca_atual = "Esquerda"
                elif posicao_cabeca_atual == "Direita":
                    if yaw < 0.08:
                        posicao_cabeca_atual = "Frente"
                elif posicao_cabeca_atual == "Esquerda":
                    if yaw > -0.08:
                        posicao_cabeca_atual = "Frente"

                posicao_cabeca = posicao_cabeca_atual
                if posicao_cabeca == "Direita":
                    neutro_ref = perfis_neutros['direita']
                elif posicao_cabeca == "Esquerda":
                    neutro_ref = perfis_neutros['esquerda']
                else:
                    neutro_ref = perfis_neutros['frente']

                b_alt_n, b_juntas_n, ear_n, mar_n, nmar_n, pup_n, car_n, nar_n = neutro_ref

                                # Variação relativa ao neutro calibrado da angulação
                queda_juntas = (b_juntas_n - bar_juntas) / b_juntas_n if b_juntas_n > 0 else 0

                # SCORES CONTÍNUOS COM DEADZONE (ignora micro-oscilações abaixo do limiar com max(0.0, ...))
                score_pergunta_geral = max(0.0, ((bar_altura - b_alt_n) - 0.02) / 0.07) + \
                                       max(0.0, ((ear_medio - ear_n) - 0.015) / 0.05)

                score_pergunta_especifica = max(0.0, ((queda_juntas - 0.08) / 0.10)) + \
                                            max(0.0, ((nar_n - nar) - 0.02) / 0.05)

                score_bico = max(0.0, ((pup - pup_n) - 0.03) / 0.09) - max(0.0, (mar - mar_n) / 0.10)
                score_bico = max(0.0, score_bico)

                score_bochechas = max(0.0, ((car - car_n) - 0.04) / 0.12)
                score_boca_aberta = max(0.0, ((mar - mar_n) - 0.04) / 0.10)

                score_sorriso = max(0.0, ((nmar - nmar_n) - 0.03) / 0.08) - max(0.0, (mar - mar_n) / 0.12)
                score_sorriso = max(0.0, score_sorriso)

                # Neutro: forte quando as outras expressões forem fracas
                max_expressao = max(
                    score_pergunta_geral, score_pergunta_especifica, score_bico,
                    score_bochechas, score_boca_aberta, score_sorriso
                )
                score_neutro = max(0.0, 1.2 - (max_expressao * 1.5))

                scores = np.array([
                    score_pergunta_geral, score_pergunta_especifica, score_bico,
                    score_bochechas, score_boca_aberta, score_sorriso, score_neutro
                ])

                nomes_expressoes = [
                    "Pergunta Geral (Sim/Nao)", "Pergunta Especifica (Quem/Onde)",
                    "Bico ('PSI/PE')", "Bochechas Infladas", "Boca Aberta / Enfase",
                    "Sorriso / Confirmacao", "Neutro"
                ]

                # Probabilidades brutas com temperatura mais suave
                probs_instataneas = ar.calcular_probabilidades(scores)

                # FILTRO PASSA-BAIXA / EMA (Garante estabilidade absoluta entre frames)
                if probabilidades_suavizadas is None:
                    probabilidades_suavizadas = probs_instataneas
                else:
                    probabilidades_suavizadas = (
                        ALPHA_SUAVIZACAO * probs_instataneas +
                        (1.0 - ALPHA_SUAVIZACAO) * probabilidades_suavizadas
                    )

                chances_ordenadas = sorted(
                    zip(nomes_expressoes, probabilidades_suavizadas),
                    key=lambda item: item[1],
                    reverse=True
                )


                y_offset = 30
                cv2.putText(image, f"Mapeado OK | Orientacao: {posicao_cabeca_atual}", (10, y_offset),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 255, 0), 2)

                for i in range(3):
                    nome, prob = chances_ordenadas[i]
                    y_offset += 25
                    cor = (0, 255, 0) if i == 0 else (200, 200, 200)
                    cv2.putText(image, f"{i+1}. {nome}: {prob*100:.1f}%", (10, y_offset),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.55, cor, 1, cv2.LINE_AA)

                info_texto = f"BAR_alt:{bar_altura:.2f} | MAR:{mar:.2f} | Yaw:{yaw:.2f} | CAR:{car:.2f}"
                cv2.putText(image, info_texto, (10, altura - 15),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.45, (255, 255, 255), 1)

        if results.pose_landmarks:
            mp_drawing.draw_landmarks(image, results.pose_landmarks, mp_holistic.POSE_CONNECTIONS)
        if results.left_hand_landmarks:
            mp_drawing.draw_landmarks(image, results.left_hand_landmarks, mp_holistic.HAND_CONNECTIONS)
        if results.right_hand_landmarks:
            mp_drawing.draw_landmarks(image, results.right_hand_landmarks, mp_holistic.HAND_CONNECTIONS)

        cv2.imshow(NOME_JANELA, image)

        if cv2.waitKey(10) & 0xFF == ord('q'):
            break

        if cv2.getWindowProperty(NOME_JANELA, cv2.WND_PROP_VISIBLE) < 1:
            break

cap.release()
cv2.destroyAllWindows()